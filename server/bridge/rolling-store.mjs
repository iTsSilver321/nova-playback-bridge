import {mkdir,writeFile,open,unlink,rm} from 'node:fs/promises';
import path from 'node:path';
import {Readable} from 'node:stream';
import {BridgeError} from './protocol.mjs';

// Disk space is a reusable viewing window, not a maximum movie size. Active
// readers lease their current window against eviction by concurrent viewers.
export class RollingCache {
  constructor(root,maxBytes){this.root=root;this.maxBytes=maxBytes;this.bytes=0;this.entries=new Map();this.tail=Promise.resolve();this.evictions=0;}
  run(fn){const promise=this.tail.then(fn);this.tail=promise.catch(()=>{});return promise;}
  async remove(key,entry){
    this.entries.delete(key);this.bytes-=entry.bytes;this.evictions++;
    const t=entry.store.torrent;
    // WebTorrent 3.0.21 has no public piece invalidation API. This adapter is
    // isolated and dependency-pinned; real TCP tests cover re-fetch after seek.
    if(!t.destroyed && t.bitfield?.get(entry.index)){
      t._markUnverified(entry.index);t._reservations[entry.index]=[];t.done=false;
      // Peers must see the revised bitfield: retaining historic HAVE messages
      // would make a seed believe this rolling viewer still owns the whole file.
      for(const wire of t.wires || []){
        if(wire.type==='webSeed')continue;
        if(wire.lt_donthave?._peerSupports)wire.lt_donthave.donthave(entry.index);
        else wire.bitfield(t.bitfield.buffer);
      }
    }
    await unlink(entry.filename).catch(e=>{if(e.code!=='ENOENT')throw e;});
  }
  async reserve(bytes,except){
    if(bytes>this.maxBytes)throw new BridgeError('This torrent uses pieces larger than the available viewing cache.',503);
    while(this.bytes+bytes>this.maxBytes){
      const victim=[...this.entries].find(([key,e])=>key!==except && !e.store.pinned(e.index));
      if(!victim)throw new BridgeError('The viewing cache is busy with active readers. Retry shortly.',503);
      await this.remove(...victim);
    }
  }
}
export class RollingPieceStore {
  constructor(chunkLength,{torrent,pool,length}){
    if(!pool || !/^[a-f0-9]{40}$/.test(torrent.infoHash))throw new Error('Invalid piece cache.');
    this.chunkLength=chunkLength;this.length=length;this.torrent=torrent;this.pool=pool;this.leases=new Set();this.closed=false;
    this.root=path.join(pool.root,torrent.infoHash);this.ready=mkdir(this.root,{recursive:true});
  }
  key(index){return this.torrent.infoHash+':'+index;}
  pinned(index){return [...this.leases].some(([first,last])=>index>=first && index<=last);}
  lease(first,last){const range=[first,last];this.leases.add(range);return()=>this.leases.delete(range);}
  put(index,buffer,cb=()=>{}){
    this.pool.run(async()=>{
      if(this.closed)throw Error('Piece store closed.');
      const size=Math.min(this.chunkLength,this.length-index*this.chunkLength);
      if(!Number.isSafeInteger(index) || index<0 || size<=0 || buffer.length!==size)throw Error('Invalid torrent piece.');
      await this.ready;const key=this.key(index),previous=this.pool.entries.get(key);
      await this.pool.reserve(buffer.length-(previous?.bytes || 0),key);
      const filename=path.join(this.root,String(index));await writeFile(filename,buffer);
      this.pool.entries.delete(key);this.pool.entries.set(key,{store:this,index,filename,bytes:buffer.length});this.pool.bytes+=buffer.length-(previous?.bytes || 0);
    }).then(()=>cb(null),cb);
  }
  get(index,opts,cb){
    if(typeof opts==='function'){cb=opts;opts={};}opts ||= {};
    this.pool.run(async()=>{
      const key=this.key(index),entry=this.pool.entries.get(key);
      if(this.closed || !entry){const error=Error('Torrent piece is not cached.');error.notFound=true;throw error;}
      const start=opts.offset ?? 0,end=opts.length===undefined?entry.bytes:start+opts.length;
      if(!Number.isSafeInteger(start)||!Number.isSafeInteger(end)||start<0||end<start||end>entry.bytes)throw Error('Invalid piece range.');
      // Peer uploads usually request 16 KB. A subarray of a full multi-MB
      // piece retains that entire allocation while the upload is queued.
      // Read only the requested range so a busy swarm cannot multiply RAM.
      const bytes=Buffer.allocUnsafeSlow(end-start),file=await open(entry.filename,'r');
      try{let offset=0;while(offset<bytes.length){const read=await file.read(bytes,offset,bytes.length-offset,start+offset);if(!read.bytesRead)throw Error('Truncated cached piece.');offset+=read.bytesRead;}}finally{await file.close();}
      this.pool.entries.delete(key);this.pool.entries.set(key,entry);return bytes;
    }).then(bytes=>cb(null,bytes),cb);
  }
  close(cb=()=>{}){this.destroy(cb);}
  destroy(cb=()=>{}){
    this.closed=true;
    this.pool.run(async()=>{for(const [key,e] of this.pool.entries)if(e.store===this){this.pool.entries.delete(key);this.pool.bytes-=e.bytes;}await this.ready;await rm(this.root,{recursive:true,force:true});}).then(()=>cb(null),cb);
  }
}
export function viewingStream(file,{start=0,end=file.length-1,signal}={}){
  const t=file._torrent,store=t.store.store,pool=store.pool;
  const read=(start,end)=>{
    if(signal?.aborted)throw Error('Movie read cancelled.');
    const reader=file.createReadStream({start,end});
    if(signal){const abort=()=>reader.destroy(Error('Movie read cancelled.'));signal.addEventListener('abort',abort,{once:true});reader.once('close',()=>signal.removeEventListener('abort',abort));}
    return reader;
  };
  if(!pool)return read(start,end);
  const window=Math.max(t.pieceLength,Math.min(8*1024**2,Math.floor(pool.maxBytes/8)));
  return Readable.from((async function*(){
    for(let offset=start;offset<=end;offset+=window){
      const last=Math.min(end,offset+window-1),release=store.lease(Math.floor((file.offset+offset)/t.pieceLength),Math.floor((file.offset+last)/t.pieceLength));
      let reader;
      try{reader=read(offset,last);for await(const bytes of reader)yield bytes;}finally{reader?.destroy();release();}
    }
  })());
}
