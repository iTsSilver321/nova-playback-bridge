import {viewingStream} from './rolling-store.mjs';
const LIMIT=16*1024**2;
function vint(b,at,id=false){
  const first=b[at];if(!first)throw Error('Invalid EBML integer.');
  let width=1,mask=128;while(!(first&mask)){width++;mask>>=1;}if(width>(id?4:8)||at+width>b.length)throw Error('Truncated EBML integer.');
  let n=BigInt(id?first:first&(mask-1));for(let i=1;i<width;i++)n=n*256n+BigInt(b[at+i]);
  const unknown=!id && n===(1n<<BigInt(7*width))-1n;
  if(!unknown && n>BigInt(Number.MAX_SAFE_INTEGER))throw Error('Oversized EBML integer.');
  return {value:unknown?Infinity:Number(n),width};
}
function header(b,at){const id=vint(b,at,true),size=vint(b,at+id.width);const start=at+id.width+size.width;return{id:id.value,start,end:start+size.value,size:size.value};}
function* children(b,start=0,end=b.length){for(let at=start;at<end;){const h=header(b,at);if(h.end>end)break;yield h;at=h.end;}}
const uint=b=>{let n=0;for(const v of b)n=n*256+v;if(!Number.isSafeInteger(n))throw Error('Invalid index offset.');return n;};
async function read(file,start,length){if(start<0 || length>LIMIT || start+length>file.length)throw Error('Invalid index range.');const chunks=[];for await(const c of viewingStream(file,{start,end:start+length-1}))chunks.push(c);const b=Buffer.concat(chunks);if(b.length!==length)throw Error('Truncated movie index.');return b;}
async function element(file,position){const h=header(await read(file,position,Math.min(16,file.length-position)),0);if(!Number.isFinite(h.size)||h.size>LIMIT)throw Error('Unsupported index size.');return{id:h.id,body:await read(file,position+h.start,h.size)};}
export function parseCues(bytes,videoTrack,scale,duration){
  const times=[];
  for(const point of children(bytes))if(point.id===0xBB){
    let time,hasVideo=false;
    for(const e of children(bytes,point.start,point.end)){
      if(e.id===0xB3)time=uint(bytes.subarray(e.start,e.end))*scale/1e9;
      if(e.id===0xB7)for(const t of children(bytes,e.start,e.end))if(t.id===0xF7 && uint(bytes.subarray(t.start,t.end))===videoTrack)hasVideo=true;
    }
    if(hasVideo && Number.isFinite(time) && time>=0 && time<duration)times.push(time);
  }
  const starts=[0];for(const time of [...new Set(times)].sort((a,b)=>a-b))if(time-starts.at(-1)>=2 && duration-time>0.05)starts.push(time);
  if(times.length<2)throw Error('No usable video seek index.');
  return starts.map((start,i)=>({start,duration:(starts[i+1]??duration)-start}));
}
export async function matroskaIndex(file,duration){
  if(!/\.(mkv|mka|webm)$/i.test(file.name))return null;
  try{
    const prefix=await read(file,0,Math.min(65536,file.length));let segment;
    for(let at=0;at<prefix.length;){const h=header(prefix,at);if(h.id===0x18538067){segment=h;break;}at=h.end;}
    if(!segment)return null;
    const positions=new Map();let info,tracks;
    for(const h of children(prefix,segment.start,prefix.length)){
      if(h.id===0x114D9B74)for(const seek of children(prefix,h.start,h.end))if(seek.id===0x4DBB){let id,position;for(const e of children(prefix,seek.start,seek.end)){if(e.id===0x53AB)id=uint(prefix.subarray(e.start,e.end));if(e.id===0x53AC)position=uint(prefix.subarray(e.start,e.end));}if(id && position!==undefined)positions.set(id,segment.start+position);}
      if(h.id===0x1549A966)info=prefix.subarray(h.start,h.end);
      if(h.id===0x1654AE6B)tracks=prefix.subarray(h.start,h.end);
    }
    if(!info && positions.has(0x1549A966))info=(await element(file,positions.get(0x1549A966))).body;
    if(!tracks && positions.has(0x1654AE6B))tracks=(await element(file,positions.get(0x1654AE6B))).body;
    let scale=1000000,videoTrack,privateData;
    for(const e of children(info || Buffer.alloc(0)))if(e.id===0x2AD7B1)scale=uint(info.subarray(e.start,e.end));
    for(const entry of children(tracks || Buffer.alloc(0)))if(entry.id===0xAE){let number,type,codec;for(const e of children(tracks,entry.start,entry.end)){if(e.id===0xD7)number=uint(tracks.subarray(e.start,e.end));if(e.id===0x83)type=uint(tracks.subarray(e.start,e.end));if(e.id===0x63A2)codec=tracks.subarray(e.start,e.end);}if(type===1 && videoTrack===undefined){videoTrack=number;privateData=codec;}}
    if(!videoTrack || !positions.has(0x1C53BB6B))return null;
    const cues=await element(file,positions.get(0x1C53BB6B));if(cues.id!==0x1C53BB6B)return null;
    return {segments:parseCues(cues.body,videoTrack,scale,duration),privateData};
  }catch{return null;}
}
export function codecString(codec,b){
  if(!b || b[0]!==1)return null;
  if(codec==='h264' && b.length>=4)return 'avc1.'+b.subarray(1,4).toString('hex');
  if(codec==='hevc' && b.length>=13){let compatibility=0;const flags=b.readUInt32BE(2);for(let i=0;i<32;i++)compatibility=compatibility*2+((flags>>>i)&1);const constraints=b.subarray(6,12).toString('hex').match(/../g);while(constraints.at(-1)==='00')constraints.pop();return `hvc1.${['','A','B','C'][b[1]>>6]}${b[1]&31}.${compatibility.toString(16)}.${b[1]&32?'H':'L'}${b[12]}${constraints.length?'.'+constraints.join('.').toUpperCase():''}`;}
  return null;
}
