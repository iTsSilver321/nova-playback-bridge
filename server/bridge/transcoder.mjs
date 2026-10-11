import {spawn} from 'node:child_process';
import {mkdir,stat,open,writeFile,rename,rm} from 'node:fs/promises';
import {createReadStream,createWriteStream} from 'node:fs';
import {pipeline} from 'node:stream/promises';
import path from 'node:path';
import {BridgeError,SEGMENT_SECONDS,BATCH_SEGMENTS} from './protocol.mjs';
import {videoTrack,fragmentVideoTime} from './mp4-fragments.mjs';

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
      done(null,{duration,width:Number(video[2]),height:Number(video[3]),codec:video[1],fps:Number(/([\d.]+) fps/.exec(line)?.[1] || 24),hdr:/bt2020|smpte2084|arib-std-b67/i.test(line),dolbyVision:/DOVI configuration/i.test(output),codecTag:/\((avc1|hvc1|hev1)\s*\//.exec(line)?.[1] || '',pixelFormat:/\b(yuvj?\d+p(?:\d+le)?)(?:\(|,| )/.exec(line)?.[1] || '',audio});
    });
  });
}
export async function remuxSegment({ffmpeg,input,directory,index,start,duration,codec,audio,audioCopy,onChild,timeout=90000}){
  await mkdir(directory,{recursive:true});const temporary=path.join(directory,`copy-${index}.tmp.mp4`);
  const args=['-hide_banner','-loglevel','error','-nostdin','-y','-protocol_whitelist','file,http,tcp,crypto','-threads','1','-analyzeduration','500000','-probesize','524288','-seek2any','1','-ss',String(start),'-i',input,'-t',String(duration),'-map','0:v:0','-c:v','copy','-tag:v',codec==='hevc'?'hvc1':'avc1','-sn','-dn'];
  if(audio<0)args.push('-an');else{args.push('-map',`0:${audio}`,'-c:a',audioCopy?'copy':'aac');if(!audioCopy)args.push('-profile:a','aac_low','-ac','2','-ar','48000','-b:a','160k','-threads:a','1');}
  args.push('-avoid_negative_ts','make_zero','-movflags','frag_keyframe+empty_moov+default_base_moof+negative_cts_offsets','-f','mp4',temporary);
  await new Promise((resolve,reject)=>{const child=spawn(ffmpeg,args,{windowsHide:true,stdio:['ignore','ignore','pipe']});onChild?.(child);const timer=setTimeout(()=>child.kill(),timeout);child.once('error',()=>{clearTimeout(timer);reject(new BridgeError('The video packager could not start.',503));});child.once('close',code=>{clearTimeout(timer);onChild?.(null);code===0?resolve():reject(new BridgeError('Video packaging stalled. Try another source.',502));});});
  const file=await open(temporary,'r+');let position=0,split,track,first;
  try{
    const length=(await file.stat()).size,bytes=Buffer.alloc(16);
    while(position+8<=length){
      const r=await file.read(bytes,0,16,position);if(r.bytesRead<8)break;const type=bytes.toString('ascii',4,8),size=bytes.readUInt32BE(0)===1?Number(bytes.readBigUInt64BE(8)):bytes.readUInt32BE(0);
      if(size<8||!Number.isSafeInteger(size)||position+size>length)throw new BridgeError('Invalid video fragment.',422);
      if(type==='moof'){
        if(!split){split=position;if(split>1024**2)throw new BridgeError('Oversized video initialization.',422);const init=Buffer.alloc(split);await file.read(init,0,split,0);track=videoTrack(init);await writeFile(path.join(directory,`copy-init-${index}.tmp`),init);}
        if(size>1024**2)throw new BridgeError('Oversized fragment metadata.',422);const fragment=Buffer.alloc(size);await file.read(fragment,0,size,position);const timing=fragmentVideoTime(fragment,track);
        if(timing){if(first===undefined){if(!timing.keyframe)throw new BridgeError('The source did not seek to a keyframe.',422);first=timing.pts;}else if(timing.pts-first>=duration-0.01){await file.truncate(position);break;}}
      }
      position+=size;
    }
    if(!split||first===undefined)throw new BridgeError('The source has no valid MP4 fragment.',422);
  }finally{await file.close();}
  // Serve the media resource through a virtual byte view after its init section.
  // This avoids copying the potentially large mdat again.
  await rename(temporary,path.join(directory,`copy-segment-${index}.m4s`));await rename(path.join(directory,`copy-init-${index}.tmp`),path.join(directory,`copy-init-${index}.mp4`));
  const size=(await stat(path.join(directory,`copy-segment-${index}.m4s`))).size;
  return {offset:split,length:size-split,size};
}
export async function remuxBatch({ffmpeg,input,directory,segments,codec,audio,audioCopy,onChild,onSegment,timeout=180000}){
  await mkdir(directory,{recursive:true});const start=segments[0].start,end=segments.at(-1).start+segments.at(-1).duration,temporary=path.join(directory,`batch-copy-${segments[0].index}.tmp.mp4`);
  const args=['-hide_banner','-loglevel','error','-nostdin','-y','-protocol_whitelist','file,http,tcp,crypto','-threads','1','-analyzeduration','500000','-probesize','524288','-seek2any','1','-ss',String(start),'-i',input,'-t',String(end-start),'-map','0:v:0','-c:v','copy','-tag:v',codec==='hevc'?'hvc1':'avc1','-sn','-dn'];
  if(audio<0)args.push('-an');else{args.push('-map',`0:${audio}`,'-c:a',audioCopy?'copy':'aac');if(!audioCopy)args.push('-profile:a','aac_low','-ac','2','-ar','48000','-b:a','160k','-threads:a','1');}
  args.push('-avoid_negative_ts','make_zero','-movflags','frag_keyframe+empty_moov+default_base_moof+negative_cts_offsets','-f','mp4',temporary);
  let done=false,failure,position=0,init,track,first,groupStart,current=0;
  const child=spawn(ffmpeg,args,{windowsHide:true,stdio:['ignore','ignore','pipe']});onChild?.(child);
  // Drain diagnostics without retaining packet data or private input URLs.
  child.stderr.resume();const timer=setTimeout(()=>child.kill(),timeout);
  const completion=new Promise(resolve=>{child.once('error',()=>{failure=new BridgeError('The video packager could not start.',503);done=true;resolve();});child.once('close',code=>{if(code!==0)failure=new BridgeError('Video packaging stalled. Try another source.',502);done=true;clearTimeout(timer);onChild?.(null);resolve();});});
  const publish=async stop=>{
    const segment=segments[current],file=path.join(directory,`copy-segment-${segment.index}.m4s`),pending=file+'.tmp';
    await writeFile(pending,init);await pipeline(createReadStream(temporary,{start:groupStart,end:stop-1}),createWriteStream(pending,{flags:'a'}));await rename(pending,file);await writeFile(path.join(directory,`copy-init-${segment.index}.mp4`),init);
    await onSegment(segment.index,{offset:init.length,length:stop-groupStart,size:init.length+stop-groupStart});current++;groupStart=stop;
  };
  try{
    for(;;){
      if(failure)throw failure;let length=0;try{length=(await stat(temporary)).size;}catch(e){if(e.code!=='ENOENT')throw e;}
      if(length>position){const file=await open(temporary,'r');try{
        const header=Buffer.alloc(16);
        while(position+8<=length){
          const read=await file.read(header,0,16,position);if(read.bytesRead<8)break;const type=header.toString('ascii',4,8),size=header.readUInt32BE(0)===1?Number(header.readBigUInt64BE(8)):header.readUInt32BE(0);
          if(!size||position+size>length)break;if(size<8||!Number.isSafeInteger(size))throw new BridgeError('Invalid video fragment.',422);
          if(type==='moof'){
            if(!init){if(position>1024**2)throw new BridgeError('Oversized video initialization.',422);init=Buffer.alloc(position);await file.read(init,0,init.length,0);track=videoTrack(init);groupStart=position;}
            if(size>1024**2)throw new BridgeError('Oversized fragment metadata.',422);const fragment=Buffer.alloc(size);await file.read(fragment,0,size,position);const timing=fragmentVideoTime(fragment,track);
            if(timing){if(first===undefined){if(!timing.keyframe)throw new BridgeError('The source did not seek to a keyframe.',422);first=timing.pts;}
              else if(current<segments.length && timing.pts-first>=segments[current].start+segments[current].duration-start-0.01){await publish(position);if(current===segments.length)break;}}
          }
          position+=size;
        }
      }finally{await file.close();}}
      if(current===segments.length)break;
      if(done){if(!init||first===undefined||position!==length)throw new BridgeError('The source returned incomplete video.',422);await publish(length);if(current!==segments.length)throw new BridgeError('The source has incomplete seek boundaries.',422);break;}
      await new Promise(resolve=>setTimeout(resolve,40));
    }
    await completion;if(failure)throw failure;
  }finally{clearTimeout(timer);if(!done)child.kill();await completion;await rm(temporary,{force:true});}
}
export class JobQueue {
  constructor(concurrency=2,maxPending=16){this.concurrency=concurrency;this.maxPending=maxPending;this.running=0;this.pending=[];}
  run(work,signal) {
    if(signal?.aborted)return Promise.reject(new BridgeError('Obsolete playback work cancelled.',410));
    if(this.pending.length>=this.maxPending)return Promise.reject(new BridgeError('The bridge is busy. Retry shortly.',503));
    return new Promise((resolve,reject)=>{const item={work,resolve,reject,signal,cancel:null};item.cancel=()=>{const index=this.pending.indexOf(item);if(index>=0){this.pending.splice(index,1);reject(new BridgeError('Obsolete playback work cancelled.',410));}};signal?.addEventListener('abort',item.cancel,{once:true});this.pending.push(item);this.drain();});
  }
  drain(){while(this.running<this.concurrency && this.pending.length){const item=this.pending.shift();item.signal?.removeEventListener('abort',item.cancel);if(item.signal?.aborted){item.reject(new BridgeError('Obsolete playback work cancelled.',410));continue;}this.running++;Promise.resolve().then(item.work).then(item.resolve,item.reject).finally(()=>{this.running--;this.drain();});}}
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
