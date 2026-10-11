import {spawn} from 'node:child_process';
import {mkdir,stat,open,writeFile,rename} from 'node:fs/promises';
import path from 'node:path';
import {BridgeError,SEGMENT_SECONDS,BATCH_SEGMENTS} from './protocol.mjs';

export async function probe(input,ffmpeg,timeout=90000) {
  return new Promise((resolve,reject)=>{
    const child=spawn(ffmpeg,['-hide_banner','-threads','1','-analyzeduration','2000000','-probesize','2000000','-protocol_whitelist','file,http,tcp,crypto','-i',input],{windowsHide:true,stdio:['ignore','ignore','pipe']});
    let output='',settled=false;
    const done=(error,data)=>{if(settled)return;settled=true;clearTimeout(timer);error?reject(error):resolve(data);};
    const timer=setTimeout(()=>{child.kill();done(new BridgeError('Video metadata timed out. Try a source with more peers.',504));},timeout);
    child.stderr.on('data',b=>{output=(output+b.toString()).slice(-60000);});
    child.once('error',()=>done(new BridgeError('FFmpeg is unavailable on the bridge.',503)));
    child.once('close',()=>{
      const match=/Duration:\s*(\d+):(\d+):([\d.]+)/.exec(output),video=/Video:\s*([^\s,]+)[^\n]*?\b(\d{2,5})x(\d{2,5})\b/.exec(output);
      const duration=match?Number(match[1])*3600+Number(match[2])*60+Number(match[3]):0;
      if(!video || duration<=0 || duration>86400) return done(new BridgeError('This video has no supported finite timeline. Try another source.',422));
      const audio=[...output.matchAll(/Stream #0:(\d+)(?:\[.*?\])?(?:\(([^)]+)\))?[^\n]*Audio:\s*([^,\n]+)/g)].map(m=>({id:Number(m[1]),lang:m[2]||'und',default:output.slice(m.index,output.indexOf('\n',m.index)).includes('(default)'),codec:m[3].trim().split(/[ (]/)[0],name:(m[2]||'Original')+' · '+m[3].trim()}));
      const line=output.split('\n').find(s=>s.includes('Video:')) || '';
      done(null,{duration,width:Number(video[2]),height:Number(video[3]),codec:video[1],dolbyVision:/DOVI configuration/i.test(output),codecTag:/\((avc1|hvc1|hev1)\s*\//.exec(line)?.[1] || '',pixelFormat:/\b(yuvj?\d+p(?:\d+le)?)(?:\(|,| )/.exec(line)?.[1] || '',audio});
    });
  });
}
export async function remuxSegment({ffmpeg,input,directory,index,start,duration,codec,audio,audioCopy,onChild,timeout=90000}){
  await mkdir(directory,{recursive:true});const temporary=path.join(directory,`copy-${index}.tmp.mp4`);
  const args=['-hide_banner','-loglevel','error','-nostdin','-y','-protocol_whitelist','file,http,tcp,crypto','-threads','1','-ss',String(start),'-i',input,'-t',String(duration),'-map','0:v:0','-c:v','copy','-tag:v',codec==='hevc'?'hvc1':'avc1','-sn','-dn'];
  if(audio<0)args.push('-an');else{args.push('-map',`0:${audio}`,'-c:a',audioCopy?'copy':'aac');if(!audioCopy)args.push('-profile:a','aac_low','-ac','2','-ar','48000','-b:a','160k','-threads:a','1');}
  args.push('-avoid_negative_ts','make_zero','-movflags','frag_keyframe+empty_moov+default_base_moof+negative_cts_offsets','-f','mp4',temporary);
  await new Promise((resolve,reject)=>{const child=spawn(ffmpeg,args,{windowsHide:true,stdio:['ignore','ignore','pipe']});onChild?.(child);const timer=setTimeout(()=>child.kill(),timeout);child.once('error',()=>{clearTimeout(timer);reject(new BridgeError('The video packager could not start.',503));});child.once('close',code=>{clearTimeout(timer);onChild?.(null);code===0?resolve():reject(new BridgeError('Video packaging stalled. Try another source.',502));});});
  const file=await open(temporary,'r');let position=0,split;
  try{const bytes=Buffer.alloc(16);for(let count=0;count<32;count++){const r=await file.read(bytes,0,16,position);if(r.bytesRead<8)break;const type=bytes.toString('ascii',4,8),size=bytes.readUInt32BE(0)===1?Number(bytes.readBigUInt64BE(8)):bytes.readUInt32BE(0);if(type==='moof'){split=position;break;}if(size<8 || !Number.isSafeInteger(size) || position+size>1024**2)break;position+=size;}if(!split)throw new BridgeError('The source has no valid MP4 fragment.',422);const init=Buffer.alloc(split);await file.read(init,0,split,0);await writeFile(path.join(directory,`copy-init-${index}.tmp`),init);}finally{await file.close();}
  // Serve the media resource through a virtual byte view after its init section.
  // This avoids copying the potentially large mdat again.
  await rename(temporary,path.join(directory,`copy-segment-${index}.m4s`));await rename(path.join(directory,`copy-init-${index}.tmp`),path.join(directory,`copy-init-${index}.mp4`));
  const size=(await stat(path.join(directory,`copy-segment-${index}.m4s`))).size;
  return {offset:split,length:size-split,size};
}
export class JobQueue {
  constructor(concurrency=2,maxPending=16){this.concurrency=concurrency;this.maxPending=maxPending;this.running=0;this.pending=[];}
  run(work) {
    if(this.pending.length>=this.maxPending)return Promise.reject(new BridgeError('The bridge is busy. Retry shortly.',503));
    return new Promise((resolve,reject)=>{this.pending.push({work,resolve,reject});this.drain();});
  }
  drain(){while(this.running<this.concurrency && this.pending.length){const item=this.pending.shift();this.running++;Promise.resolve().then(item.work).then(item.resolve,item.reject).finally(()=>{this.running--;this.drain();});}}
}
export async function encodeBatch({ffmpeg,input,directory,batch,quality,audio,duration,threads=2,preset='veryfast',timeout=180000,onChild}) {
  await mkdir(directory,{recursive:true});
  const first=batch*BATCH_SEGMENTS,start=first*SEGMENT_SECONDS,length=Math.min(BATCH_SEGMENTS*SEGMENT_SECONDS,duration-start);
  if(length<=0)throw new BridgeError('Segment is outside the video.',404);
  const args=['-hide_banner','-loglevel','error','-nostdin','-y','-protocol_whitelist','file,http,tcp,crypto','-threads','1','-filter_threads','1','-ss',String(start),'-i',input,'-t',String(length),'-map','0:v:0'];
  if(audio>=0)args.push('-map',`0:${audio}`);else args.push('-an');
  args.push('-sn','-dn','-vf',`scale=w=-2:h=${quality},format=yuv420p`,'-c:v','libx264','-preset',['ultrafast','superfast','veryfast'].includes(preset)?preset:'veryfast','-crf','22','-profile:v','high','-level:v',quality>1080?'5.2':'4.1','-threads',String(threads),'-force_key_frames',`expr:gte(t,n_forced*${SEGMENT_SECONDS})`,'-sc_threshold','0');
  if(audio>=0)args.push('-c:a','aac','-ac','2','-ar','48000','-b:a','160k');
  args.push('-f','hls','-hls_time',String(SEGMENT_SECONDS),'-hls_list_size','0','-hls_flags','independent_segments+temp_file','-start_number',String(first),'-hls_segment_filename',path.join(directory,'segment-%d.ts'),path.join(directory,`batch-${batch}.m3u8`));
  await new Promise((resolve,reject)=>{
    const child=spawn(ffmpeg,args,{windowsHide:true,stdio:['ignore','ignore','pipe']});onChild?.(child);
    let diagnostic='';child.stderr.on('data',b=>{diagnostic=(diagnostic+b.toString()).slice(-3000);});
    const timer=setTimeout(()=>child.kill(),timeout);
    child.once('error',()=>{clearTimeout(timer);reject(new BridgeError('The video converter could not start.',503));});
    child.once('close',code=>{clearTimeout(timer);onChild?.(null);code===0?resolve():reject(new BridgeError(diagnostic.includes('Invalid data')?'The source contains damaged or unsupported video.':'Conversion stalled or failed. Try a source with more peers.',502));});
  });
  await stat(path.join(directory,`segment-${first}.ts`));
}
