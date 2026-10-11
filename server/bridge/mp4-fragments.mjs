import {BridgeError} from './protocol.mjs';
function* boxes(b,start=0,end=b.length){
  for(let at=start;at+8<=end;){const size=b.readUInt32BE(at),wide=size===1,bytes=wide?Number(b.readBigUInt64BE(at+8)):size;if(bytes<(wide?16:8)||!Number.isSafeInteger(bytes)||at+bytes>end)throw new BridgeError('Invalid video fragment.',422);yield{type:b.toString('ascii',at+4,at+8),start:at+(wide?16:8),end:at+bytes};at+=bytes;}
}
export function videoTrack(init){
  const moov=[...boxes(init)].find(b=>b.type==='moov');if(!moov)throw new BridgeError('Missing video initialization.',422);
  for(const trak of boxes(init,moov.start,moov.end))if(trak.type==='trak'){
    const fields=[...boxes(init,trak.start,trak.end)],tkhd=fields.find(b=>b.type==='tkhd'),mdia=fields.find(b=>b.type==='mdia');if(!tkhd||!mdia)continue;
    const media=[...boxes(init,mdia.start,mdia.end)],hdlr=media.find(b=>b.type==='hdlr'),mdhd=media.find(b=>b.type==='mdhd');if(!hdlr||!mdhd||init.toString('ascii',hdlr.start+8,hdlr.start+12)!=='vide')continue;
    return{id:init.readUInt32BE(tkhd.start+(init[tkhd.start]===1?20:12)),scale:init.readUInt32BE(mdhd.start+(init[mdhd.start]===1?20:12))};
  }
  throw new BridgeError('Missing video track.',422);
}
export function fragmentVideoTime(bytes,track){
  const moof=[...boxes(bytes)].find(b=>b.type==='moof');if(!moof)return null;
  for(const traf of boxes(bytes,moof.start,moof.end))if(traf.type==='traf'){
    const fields=[...boxes(bytes,traf.start,traf.end)],header=fields.find(b=>b.type==='tfhd'),time=fields.find(b=>b.type==='tfdt'),run=fields.find(b=>b.type==='trun');
    if(!header||!time||!run||bytes.readUInt32BE(header.start+4)!==track.id)continue;
    const flags=bytes.readUInt32BE(header.start)&0xffffff;let h=header.start+8;if(flags&1)h+=8;if(flags&2)h+=4;if(flags&8)h+=4;if(flags&16)h+=4;const defaults=flags&32?bytes.readUInt32BE(h):0;
    const f=bytes.readUInt32BE(run.start)&0xffffff;let p=run.start+8;if(f&1)p+=4;let sampleFlags=f&4?bytes.readUInt32BE(p):defaults;if(f&4)p+=4;if(f&256)p+=4;if(f&512)p+=4;if(f&1024){sampleFlags=bytes.readUInt32BE(p);p+=4;}
    const cts=f&2048?(bytes[run.start]===1?bytes.readInt32BE(p):bytes.readUInt32BE(p)):0;
    const decode=bytes[time.start]===1?Number(bytes.readBigUInt64BE(time.start+4)):bytes.readUInt32BE(time.start+4);
    if(!track.scale||!Number.isSafeInteger(decode))throw new BridgeError('Invalid video timestamps.',422);
    return{pts:(decode+cts)/track.scale,keyframe:!(sampleFlags&0x10000)};
  }
  return null;
}
