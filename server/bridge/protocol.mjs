export const SEGMENT_SECONDS = 4;
export const BATCH_SEGMENTS = 4;
export const QUALITY_HEIGHTS = [480, 720, 1080];
export class BridgeError extends Error {
  constructor(message, status = 400) { super(message); this.status = status; }
}
export function sourceRequest(value) {
  if (!value || !/^[a-f0-9]{40}$/i.test(value.infoHash || '')) throw new BridgeError('Invalid torrent hash.');
  const index = value.fileIdx;
  if (index !== undefined && index !== null && (!Number.isSafeInteger(index) || index < 0 || index > 10000)) throw new BridgeError('Invalid video file index.');
  const episode = value.episode;
  if (episode !== undefined && (!Number.isInteger(episode.season) || !Number.isInteger(episode.episode) || episode.season < 0 || episode.episode < 1)) throw new BridgeError('Invalid episode.');
  return {infoHash:value.infoHash.toLowerCase(), fileIdx:index ?? undefined, episode};
}
export function selectVideo(files, request, maxBytes) {
  const videos = files.map((file,index)=>({file,index})).filter(({file})=>/\.(mp4|mkv|webm|m4v|mov|avi|ts)$/i.test(file.name));
  let chosen;
  if (request.fileIdx !== undefined) chosen=videos.find(v=>v.index===request.fileIdx);
  else if (request.episode) {
    const {season,episode}=request.episode;
    const pattern=new RegExp(`(?:s0*${season}[ ._-]*e0*${episode}(?!\\d)|(?:^|[^\\d])0*${season}x0*${episode}(?!\\d))`,'i');
    chosen=videos.find(({file})=>pattern.test(file.name));
    if (!chosen) throw new BridgeError('Choose an episode-specific torrent source. This season pack has no matching episode filename.',422);
  } else chosen=videos.filter(({file})=>!/(?:^|[ ._-])sample[ ._-]/i.test(file.name)).sort((a,b)=>b.file.length-a.file.length)[0];
  if(!chosen) throw new BridgeError('No matching video file in this source.',422);
  if(chosen.file.length>maxBytes) throw new BridgeError('This source exceeds the bridge disk budget. Choose a smaller release.',413);
  return chosen;
}
export function availableQualities(height) {
  const top=Math.min(1080,Math.max(2,Math.floor(height/2)*2));
  return [...new Set([...QUALITY_HEIGHTS.filter(h=>h<=top),top])].sort((a,b)=>a-b);
}
export function variantPlaylist(duration, quality, audio) {
  if(!Number.isFinite(duration)||duration<=0||duration>86400) throw new BridgeError('Invalid video duration.',422);
  const count=Math.ceil(duration/SEGMENT_SECONDS);
  const rows=['#EXTM3U','#EXT-X-VERSION:3','#EXT-X-TARGETDURATION:4','#EXT-X-MEDIA-SEQUENCE:0','#EXT-X-PLAYLIST-TYPE:VOD','#EXT-X-INDEPENDENT-SEGMENTS'];
  for(let i=0;i<count;i++) {
    if(i && i%BATCH_SEGMENTS===0) rows.push('#EXT-X-DISCONTINUITY');
    rows.push(`#EXTINF:${Math.min(SEGMENT_SECONDS,duration-i*SEGMENT_SECONDS).toFixed(6)},`,`segment-${i}.ts?q=${quality}&a=${audio}`);
  }
  return rows.concat('#EXT-X-ENDLIST','').join('\n');
}
export function masterPlaylist(qualities,audio,width,height) {
  const rows=['#EXTM3U','#EXT-X-VERSION:3','#EXT-X-INDEPENDENT-SEGMENTS'];
  for(const q of qualities) {
    const w=Math.max(2,Math.floor(width*q/height/2)*2),bandwidth=q>=1080?6000000:q>=720?3200000:1500000;
    rows.push(`#EXT-X-STREAM-INF:BANDWIDTH=${bandwidth},RESOLUTION=${w}x${q}`,`index.m3u8?q=${q}&a=${audio}`);
  }
  return rows.concat('').join('\n');
}
