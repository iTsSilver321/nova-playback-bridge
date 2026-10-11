import http from 'node:http';
import {randomBytes,timingSafeEqual} from 'node:crypto';
import {mkdir,stat,statfs,rm} from 'node:fs/promises';
import {createReadStream} from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import WebTorrent from 'webtorrent';
import ffmpegStatic from 'ffmpeg-static';
import {parseRange,videoMime} from '../range.mjs';
import {BridgeError,sourceRequest,selectVideo,availableQualities,variantPlaylist,masterPlaylist,BATCH_SEGMENTS,SEGMENT_SECONDS} from './protocol.mjs';
import {probe,encodeBatch,JobQueue} from './transcoder.mjs';

const TRACKERS=['udp://tracker.opentrackr.org:1337/announce','udp://open.stealth.si:80/announce','udp://tracker.torrent.eu.org:451/announce'];
const PRIVATE_RANGES=['0.0.0.0/8','10.0.0.0/8','127.0.0.0/8','169.254.0.0/16','172.16.0.0/12','192.168.0.0/16','224.0.0.0/4','240.0.0.0/4'];
const equal=(a,b)=>{const x=Buffer.from(a||''),y=Buffer.from(b||'');return x.length===y.length && timingSafeEqual(x,y);};
const json=(res,status,data)=>{res.writeHead(status,{'Content-Type':'application/json','Cache-Control':'no-store'});res.end(JSON.stringify(data));};
async function readBody(req){let bytes=0;const chunks=[];for await(const chunk of req){bytes+=chunk.length;if(bytes>8192)throw new BridgeError('Request too large.',413);chunks.push(chunk);}try{return JSON.parse(Buffer.concat(chunks).toString());}catch{throw new BridgeError('Use a JSON request.');}}
const bounded=(value,fallback,min,max)=>Number.isFinite(Number(value))?Math.max(min,Math.min(max,Math.floor(Number(value)))):fallback;

// This is a standalone server, deliberately outside the Sites Worker bundle.
export async function createBridge(options={}) {
  const secret=options.secret || process.env.NOVA_BRIDGE_KEY;
  if(!secret || secret.length<32)throw new Error('NOVA_BRIDGE_KEY must contain at least 32 characters.');
  const root=path.resolve(options.cachePath || process.env.NOVA_BRIDGE_CACHE || '.nova-bridge-cache');
  await mkdir(root,{recursive:true});
  const maxBytes=bounded(options.maxBytes ?? process.env.NOVA_BRIDGE_CACHE_GB,20,1,1000)*1024**3;
  const maxSessions=bounded(options.maxSessions ?? process.env.NOVA_BRIDGE_SESSIONS,4,1,100);
  const idleMs=options.idleMs ?? 20*60*1000,ttlMs=options.ttlMs ?? 6*60*60*1000;
  const ffmpeg=options.ffmpeg || process.env.NOVA_FFMPEG || ffmpegStatic;
  const origins=new Set(options.origins || (process.env.NOVA_ALLOWED_ORIGINS || 'https://nova-cinema-eduar.eduardochiana25.chatgpt.site').split(',').map(s=>s.trim()));
  const sessions=new Map(),torrents=new Map(),jobs=new JobQueue(bounded(options.concurrency ?? process.env.NOVA_BRIDGE_JOBS,2,1,16));
  const client=options.client || new WebTorrent({maxConns:50,lsd:false,natUpnp:false,natPmp:false,webSeeds:false,blocklist:PRIVATE_RANGES,uploadLimit:512*1024});
  client.on('error',()=>{for(const s of sessions.values())s.error='The torrent network stopped. Reconnect to another source.';});
  let closing=false;
  async function quota(extra=0){const info=await statfs(root);if(Number(info.bavail)*Number(info.bsize)<512*1024**2+extra)throw new BridgeError('The bridge has insufficient free disk space.',503);const total=[...torrents.values()].reduce((n,s)=>n+(s.torrent?.downloaded || 0),0)+[...sessions.values()].reduce((n,s)=>n+s.segmentBytes,0);if(total+extra>maxBytes)throw new BridgeError('The bridge cache is full. Retry after an inactive stream expires.',503);}
  async function getTorrent(hash){
    const existing=torrents.get(hash);if(existing){existing.used=Date.now();return existing.promise;}
    if(torrents.size>=maxSessions)throw new BridgeError('All torrent slots are in use. Retry shortly.',503);
    await quota();
    const state={used:Date.now(),torrent:null,promise:null};torrents.set(hash,state);
    state.promise=new Promise((resolve,reject)=>{
      const magnet=`magnet:?xt=urn:btih:${hash}&${TRACKERS.map(t=>'tr='+encodeURIComponent(t)).join('&')}`;
      const t=client.add(magnet,{path:path.join(root,'torrents'),addUID:true,deselect:true,storeCacheSlots:8,destroyStoreOnDestroy:true},t=>{clearTimeout(timer);if(t.files.length>10000){reject(new BridgeError('This torrent has too many files.',413));return;}t.deselect(0,t.pieces.length-1,0);for(const f of t.files)f.deselect();state.torrent=t;resolve(t);});
      const timer=setTimeout(()=>{reject(new BridgeError('No torrent peers supplied metadata. Try a better-seeded release.',504));},options.metadataTimeout || 90000);
      t.once('error',()=>{clearTimeout(timer);reject(new BridgeError('The torrent source could not be opened.',502));});
    }).catch(async e=>{torrents.delete(hash);await client.remove(hash,{destroyStore:true}).catch(()=>{});throw e;});
    return state.promise;
  }
  const raw=http.createServer((req,res)=>{
    const match=/^\/raw\/([a-f0-9]{32})\/([a-f0-9]{48})$/.exec(req.url?.split('?')[0] || '');
    const s=match && sessions.get(match[1]);
    if(!s || !equal(match[2],s.rawToken)){json(res,404,{error:'Not found.'});return;}
    let range;try{range=parseRange(req.headers.range,s.file.length);}catch{res.writeHead(416,{'Content-Range':`bytes */${s.file.length}`});res.end();return;}
    const start=range?.start ?? 0,end=range?.end ?? s.file.length-1,headers={'Content-Type':videoMime(s.file.name),'Content-Length':end-start+1,'Accept-Ranges':'bytes'};
    if(range)headers['Content-Range']=`bytes ${start}-${end}/${s.file.length}`;
    res.writeHead(range?206:200,headers);if(req.method==='HEAD'){res.end();return;}
    const stream=s.file.createReadStream({start,end});stream.once('error',()=>res.destroy());res.once('close',()=>stream.destroy());stream.pipe(res);
    res.setTimeout(180000,()=>{stream.destroy();res.destroy();});
  });
  await new Promise(resolve=>raw.listen(0,'127.0.0.1',resolve));
  const rawPort=raw.address().port;
  async function dispose(s){
    if(s.disposing)return;s.disposing=true;
    sessions.delete(s.id);for(const child of s.children)child.kill();
    await Promise.allSettled([...s.batches.values()]);
    const target=path.resolve(root,'sessions',s.id);
    if(!target.startsWith(root+path.sep))throw new Error('Cache path escaped its root.');
    await rm(target,{recursive:true,force:true});
    if(![...sessions.values()].some(other=>other.infoHash===s.infoHash)){
      torrents.delete(s.infoHash);await client.remove(s.infoHash,{destroyStore:true}).catch(()=>{});
    }
  }
  async function start(value){
    const request=sourceRequest(value),owner=String(value.owner || '');
    if(!/^[a-f0-9]{64}$/.test(owner))throw new BridgeError('Missing trusted viewer identifier.',403);
    if(sessions.size>=maxSessions || [...sessions.values()].filter(s=>s.owner===owner).length>=2)throw new BridgeError('The bridge has reached its active playback capacity.',429);
    // Reserve before awaiting metadata, so parallel requests cannot evade limits.
    const s={id:randomBytes(16).toString('hex'),token:randomBytes(24).toString('hex'),rawToken:randomBytes(24).toString('hex'),owner,infoHash:request.infoHash,used:Date.now(),expires:Date.now()+ttlMs,file:null,info:null,segmentBytes:0,batches:new Map(),children:new Set(),error:'',disposing:false};
    sessions.set(s.id,s);
    try {
      const t=await getTorrent(request.infoHash),chosen=selectVideo(t.files,request,maxBytes);
      s.file=chosen.file;
      s.input=`http://127.0.0.1:${rawPort}/raw/${s.id}/${s.rawToken}`;
      s.info=await jobs.run(()=>probe(s.input,ffmpeg,options.probeTimeout || 90000));
      if(closing || s.disposing)throw new BridgeError('The bridge is shutting down.',503);
      const qualities=availableQualities(s.info.height),audio=s.info.audio[0]?.id ?? -1;
      s.qualities=qualities;
      return {id:s.id,path:`/v1/media/${s.id}/${s.token}/master.m3u8?a=${audio}`,expires:s.expires,duration:s.info.duration,audio:s.info.audio,qualities,name:s.file.name};
    } catch(e){await dispose(s);throw e;}
  }
  async function batch(s,number,quality,audio){
    const batch=Math.floor(number/BATCH_SEGMENTS),key=`${quality}-${audio}-${batch}`;
    if(s.batches.has(key))return s.batches.get(key);
    const directory=path.join(root,'sessions',s.id,`${quality}-${audio}`);
    const promise=jobs.run(async()=>{
      if(s.disposing)throw new BridgeError('This stream has closed.',410);
      await quota(32*1024**2);
      await encodeBatch({ffmpeg,input:s.input,directory,batch,quality,audio,duration:s.info.duration,onChild:child=>{if(child)s.children.add(child);else for(const c of s.children)if(c.exitCode!==null || c.signalCode!==null)s.children.delete(c);}});
      for(let i=batch*BATCH_SEGMENTS;i<(batch+1)*BATCH_SEGMENTS && i*SEGMENT_SECONDS<s.info.duration;i++)s.segmentBytes+=(await stat(path.join(directory,`segment-${i}.ts`))).size;
    }).catch(e=>{s.batches.delete(key);throw e;});
    s.batches.set(key,promise);return promise;
  }
  const server=http.createServer(async(req,res)=>{
    res.setHeader('X-Content-Type-Options','nosniff');res.setHeader('Referrer-Policy','no-referrer');
    const origin=req.headers.origin;if(origin && !origins.has(origin)){json(res,403,{error:'Origin not allowed.'});return;}
    if(origin){res.setHeader('Access-Control-Allow-Origin',origin);res.setHeader('Vary','Origin');}
    if(req.method==='OPTIONS'){res.writeHead(204,{'Access-Control-Allow-Methods':'GET,HEAD,OPTIONS','Access-Control-Allow-Headers':'Range','Access-Control-Max-Age':'600'});res.end();return;}
    try {
      if(closing)throw new BridgeError('The bridge is shutting down.',503);
      const url=new URL(req.url,'http://bridge.invalid');
      if(url.pathname==='/health' && req.method==='GET'){json(res,200,{status:'ok',protocol:1});return;}
      if(url.pathname==='/v1/sessions' && req.method==='POST'){
        if(!equal(req.headers.authorization,`Bearer ${secret}`))throw new BridgeError('Not authorized.',401);
        json(res,200,await start(await readBody(req)));return;
      }
      const match=/^\/v1\/media\/([a-f0-9]{32})\/([a-f0-9]{48})\/(master\.m3u8|index\.m3u8|segment-\d+\.ts)$/.exec(url.pathname);
      if(!match || !['GET','HEAD'].includes(req.method))throw new BridgeError('Not found.',404);
      const s=sessions.get(match[1]);if(!s || !equal(match[2],s.token) || !s.info)throw new BridgeError('This stream has expired. Open the source again.',410);
      if(s.expires<Date.now()){await dispose(s);throw new BridgeError('This stream has expired. Open the source again.',410);}
      if(s.error)throw new BridgeError(s.error,503);
      s.used=Date.now();const a=Number(url.searchParams.get('a') ?? s.info.audio[0]?.id ?? -1),q=Number(url.searchParams.get('q') ?? 720);
      if(!Number.isInteger(a) || (a!==-1 && !s.info.audio.some(track=>track.id===a)))throw new BridgeError('Unknown audio track.');
      if(match[3]==='master.m3u8'){
        const value=masterPlaylist(s.qualities,a,s.info.width,s.info.height);res.writeHead(200,{'Content-Type':'application/vnd.apple.mpegurl','Cache-Control':'private, no-store'});res.end(req.method==='HEAD'?undefined:value);return;
      }
      if(!s.qualities.includes(q))throw new BridgeError('Unknown quality.');
      if(match[3]==='index.m3u8'){
        res.writeHead(200,{'Content-Type':'application/vnd.apple.mpegurl','Cache-Control':'private, no-store'});res.end(req.method==='HEAD'?undefined:variantPlaylist(s.info.duration,q,a));return;
      }
      const number=Number(/segment-(\d+)/.exec(match[3])[1]);if(!Number.isSafeInteger(number) || number*SEGMENT_SECONDS>=s.info.duration)throw new BridgeError('Segment not found.',404);
      await batch(s,number,q,a);
      if(res.destroyed)return;
      const filename=path.join(root,'sessions',s.id,`${q}-${a}`,`segment-${number}.ts`),info=await stat(filename);
      res.writeHead(200,{'Content-Type':'video/mp2t','Content-Length':info.size,'Cache-Control':'private, max-age=120'});
      if(req.method==='HEAD'){res.end();return;}
      const stream=createReadStream(filename);stream.once('error',()=>res.destroy());res.once('close',()=>stream.destroy());stream.pipe(res);
    } catch(e){options.onError?.(e);if(!res.headersSent)json(res,e instanceof BridgeError?e.status:500,{error:e instanceof BridgeError?e.message:'The playback bridge failed. Try another source.'});else res.destroy();}
  });
  server.requestTimeout=300000;server.headersTimeout=30000;
  const timer=setInterval(()=>{for(const s of sessions.values())if(Date.now()-s.used>idleMs || Date.now()>s.expires)dispose(s).catch(()=>{});},30000);timer.unref();
  return {server,sessions,client,async close(){closing=true;clearInterval(timer);await Promise.allSettled([...sessions.values()].map(dispose));await new Promise(resolve=>client.destroy(resolve));await new Promise(resolve=>raw.close(resolve));if(server.listening)await new Promise(resolve=>{server.close(resolve);server.closeAllConnections();});}};
}
if(process.argv[1] && path.resolve(process.argv[1])===fileURLToPath(import.meta.url)) {
  const bridge=await createBridge();const port=bounded(process.env.PORT || process.env.NOVA_BRIDGE_PORT,11570,1024,65535);
  bridge.server.listen(port,process.env.NOVA_BRIDGE_BIND || '127.0.0.1',()=>console.log(`NOVA playback bridge listening on port ${port}`));
  for(const signal of ['SIGINT','SIGTERM'])process.once(signal,()=>bridge.close().then(()=>process.exit(0)));
}
