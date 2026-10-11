import {viewingStream} from './rolling-store.mjs';

// Read container metadata, never the intervening mdat. This conservative path
// accepts unencrypted AVC 8-bit 4:2:0 + AAC-LC; other formats keep FFmpeg probing.
const LIMIT = 8 * 1024 ** 2;
function box(bytes, at, end = bytes.length) {
  if (at + 8 > end) throw Error('Truncated MP4 box');
  let size = bytes.readUInt32BE(at), header = 8;
  if (size === 1) {
    if (at + 16 > end) throw Error('Truncated extended MP4 box');
    const wide = bytes.readBigUInt64BE(at + 8);
    if (wide > BigInt(Number.MAX_SAFE_INTEGER)) throw Error('Oversized MP4 box');
    size = Number(wide); header = 16;
  } else if (size === 0) size = end - at;
  if (size < header || !Number.isSafeInteger(at + size) || at + size > end)
    throw Error('Invalid MP4 box bounds');
  return {type: bytes.toString('ascii', at + 4, at + 8), start: at + header, end: at + size, size};
}
function children(bytes, start = 0, end = bytes.length) {
  const rows = [];
  for (let at = start; at < end;) {
    if (rows.length >= 4096) throw Error('Too many MP4 boxes');
    const child = box(bytes, at, end); rows.push(child); at = child.end;
  }
  return rows;
}
function one(bytes, parent, name) {
  const result = children(bytes, parent.start, parent.end).find(b => b.type === name);
  if (!result) throw Error('Missing ' + name);
  return result;
}
function timeline(bytes, entry) {
  const version = bytes[entry.start];
  if (![0, 1].includes(version)) throw Error('Unsupported timeline version');
  const at = entry.start + (version ? 20 : 12);
  if (at + (version ? 12 : 8) > entry.end) throw Error('Truncated timeline');
  const scale = bytes.readUInt32BE(at);
  const ticks = version ? Number(bytes.readBigUInt64BE(at + 4)) : bytes.readUInt32BE(at + 4);
  if (!scale || !Number.isSafeInteger(ticks) || ticks <= 0) throw Error('Invalid timeline');
  return {scale, ticks, duration: ticks / scale};
}
function bitReader(buffer) {
  let position = 0;
  const bits = n => {
    if (n > 32 || position + n > buffer.length * 8) throw Error('Truncated bitstream');
    let value = 0;
    for (let i = 0; i < n; i++, position++) value = value * 2 + ((buffer[position >> 3] >> (7 - (position & 7))) & 1);
    return value;
  };
  const ue = () => {
    let zeros = 0;
    while (bits(1) === 0) if (++zeros > 24) throw Error('Invalid Exp-Golomb value');
    return 2 ** zeros - 1 + bits(zeros);
  };
  return {bits, ue, se: () => {const n = ue(); return n & 1 ? (n + 1) / 2 : -n / 2;}};
}
function avc(bytes) {
  if (bytes.length < 9 || bytes[0] !== 1 || ![66, 77, 88, 100].includes(bytes[1])) throw Error('Unsupported AVC configuration');
  const count = bytes[5] & 31;
  if (count !== 1) throw Error('Multiple AVC parameter sets');
  const length = bytes.readUInt16BE(6);
  if (length < 4 || length > 4096 || 8 + length > bytes.length) throw Error('Invalid AVC parameter set');
  const nal = bytes.subarray(8, 8 + length);
  if ((nal[0] & 31) !== 7) throw Error('Missing AVC SPS');
  const rbsp = [];
  for (let i = 1; i < nal.length; i++) {
    if (i >= 3 && nal[i] === 3 && nal[i - 1] === 0 && nal[i - 2] === 0) continue;
    rbsp.push(nal[i]);
  }
  const r = bitReader(Buffer.from(rbsp)), profile = r.bits(8);
  if (profile !== bytes[1] || r.bits(8) !== bytes[2] || r.bits(8) !== bytes[3]) throw Error('AVC configuration mismatch');
  r.ue();
  if (profile === 100) {
    if (r.ue() !== 1 || r.ue() !== 0 || r.ue() !== 0) throw Error('AVC needs conversion');
    r.bits(1);
    if (r.bits(1)) for (let i = 0; i < 8; i++) if (r.bits(1)) {
      let last = 8, next = 8;
      for (let j = 0; j < (i < 6 ? 16 : 64); j++) {
        if (next) next = (last + r.se() + 256) % 256;
        last = next || last;
      }
    }
  }
  r.ue();
  const order = r.ue();
  if (order === 0) r.ue();
  else if (order === 1) {
    r.bits(1); r.se(); r.se(); const cycles = r.ue();
    if (cycles > 256) throw Error('Oversized AVC cycle');
    for (let i = 0; i < cycles; i++) r.se();
  } else if (order !== 2) throw Error('Invalid AVC order');
  r.ue(); r.bits(1);
  const columns = r.ue() + 1, rows = r.ue() + 1, frame = r.bits(1);
  if (!frame) r.bits(1);
  r.bits(1);
  let left = 0, right = 0, top = 0, bottom = 0;
  if (r.bits(1)) {left = r.ue(); right = r.ue(); top = r.ue(); bottom = r.ue();}
  const width = columns * 16 - (left + right) * 2;
  const height = rows * 16 * (2 - frame) - (top + bottom) * 2 * (2 - frame);
  if (r.bits(1)) {
    if (r.bits(1) && r.bits(8) === 255) {r.bits(16); r.bits(16);}
    if (r.bits(1)) r.bits(1);
    if (r.bits(1)) {
      r.bits(3); r.bits(1);
      if (r.bits(1)) {
        const primaries = r.bits(8), transfer = r.bits(8); r.bits(8);
        if (primaries === 9 || [16, 18].includes(transfer)) throw Error('HDR needs full probing');
      }
    }
  }
  if (width < 16 || width > 7680 || height < 16 || height > 4320) throw Error('Invalid AVC dimensions');
  return {width, height, codecString: 'avc1.' + bytes.subarray(1, 4).toString('hex')};
}
function descriptor(bytes, at, end) {
  if (at >= end) throw Error('Missing audio descriptor');
  const tag = bytes[at++]; let size = 0, complete = false;
  for (let i = 0; i < 4; i++) {
    if (at >= end) throw Error('Truncated audio descriptor');
    const value = bytes[at++]; size = size * 128 + (value & 127);
    if (!(value & 128)) {complete = true; break;}
  }
  if (!complete || at + size > end) throw Error('Invalid audio descriptor');
  return {tag, start: at, end: at + size};
}
function aac(bytes, entry) {
  const es = descriptor(bytes, entry.start + 4, entry.end);
  if (es.tag !== 3 || es.start + 3 > es.end) throw Error('Invalid ES descriptor');
  let at = es.start + 3; const flags = bytes[es.start + 2];
  if (flags & 128) at += 2;
  if (flags & 64) {if (at >= es.end) throw Error('Invalid audio URL'); at += 1 + bytes[at];}
  if (flags & 32) at += 2;
  const decoder = descriptor(bytes, at, es.end);
  if (decoder.tag !== 4 || decoder.end - decoder.start < 13 || bytes[decoder.start] !== 0x40) throw Error('Unsupported audio object');
  const config = descriptor(bytes, decoder.start + 13, decoder.end);
  if (config.tag !== 5) throw Error('Missing AAC configuration');
  const r = bitReader(bytes.subarray(config.start, config.end));
  if (r.bits(5) !== 2) throw Error('Only AAC-LC uses the fast path');
  const frequency = r.bits(4), channels = r.bits(4);
  if (frequency > 12 || channels < 1 || channels > 6) throw Error('Unsupported AAC configuration');
}
export function parseMp4Metadata(bytes) {
  const root = {start: 0, end: bytes.length};
  const entries = children(bytes), movie = timeline(bytes, one(bytes, root, 'mvhd'));
  if (entries.some(b => b.type === 'mvex') || movie.duration > 86400) throw Error('Fragmented or unsupported movie');
  const audio = []; let video;
  const tracks = entries.filter(b => b.type === 'trak');
  if (tracks.length > 32) throw Error('Too many movie tracks');
  for (const [id, track] of tracks.entries()) {
    const header = one(bytes, track, 'tkhd'), mdia = one(bytes, track, 'mdia');
    const enabled = !!(bytes.readUInt32BE(header.start) & 1);
    const handler = one(bytes, mdia, 'hdlr');
    if (handler.start + 12 > handler.end) throw Error('Truncated handler');
    const type = bytes.toString('ascii', handler.start + 8, handler.start + 12);
    if (!['vide', 'soun'].includes(type)) continue;
    const mdhd = one(bytes, mdia, 'mdhd'), clock = timeline(bytes, mdhd);
    const languageAt = mdhd.start + (bytes[mdhd.start] ? 32 : 20);
    if (languageAt + 2 > mdhd.end) throw Error('Truncated language');
    const language = bytes.readUInt16BE(languageAt);
    const lang = [10, 5, 0].map(n => String.fromCharCode(((language >> n) & 31) + 96)).join('');
    const minf = one(bytes, mdia, 'minf'), stbl = one(bytes, minf, 'stbl'), stsd = one(bytes, stbl, 'stsd');
    if (stsd.start + 8 > stsd.end || bytes.readUInt32BE(stsd.start + 4) !== 1) throw Error('Unsupported sample descriptions');
    const sample = box(bytes, stsd.start + 8, stsd.end);
    if (sample.end !== stsd.end) throw Error('Multiple sample descriptions');
    if (type === 'vide') {
      if (video || !enabled || sample.type !== 'avc1' || sample.start + 78 > sample.end) throw Error('Unsupported video track');
      const extensions = children(bytes, sample.start + 78, sample.end);
      if (extensions.some(b => ['sinf', 'dvcC', 'dvvC', 'mdcv', 'clli'].includes(b.type))) throw Error('Encrypted or HDR video');
      for (const colour of extensions.filter(b => b.type === 'colr')) {
        if (colour.end - colour.start < 10 || !['nclc', 'nclx'].includes(bytes.toString('ascii', colour.start, colour.start + 4))) throw Error('Unknown colour information');
        if (bytes.readUInt16BE(colour.start + 4) === 9 || [16, 18].includes(bytes.readUInt16BE(colour.start + 6))) throw Error('HDR colour information');
      }
      const configuration = extensions.find(b => b.type === 'avcC');
      if (!configuration) throw Error('Missing AVC configuration');
      video = avc(bytes.subarray(configuration.start, configuration.end));
      if (video.width !== bytes.readUInt16BE(sample.start + 24) || video.height !== bytes.readUInt16BE(sample.start + 26)) throw Error('Video dimension mismatch');
      const matrixAt = header.start + (bytes[header.start] === 1 ? 52 : 40);
      if (![0, 1].includes(bytes[header.start]) || matrixAt + 36 > header.end) throw Error('Truncated track matrix');
      const identity = [65536, 0, 0, 0, 65536, 0, 0, 0, 1073741824];
      if (identity.some((n, i) => bytes.readInt32BE(matrixAt + i * 4) !== n)) throw Error('Rotated video needs full probing');
      const timing = one(bytes, stbl, 'stts');
      if (timing.start + 8 > timing.end) throw Error('Missing sample timing');
      const count = bytes.readUInt32BE(timing.start + 4);
      if (count > 100000 || timing.start + 8 + count * 8 !== timing.end) throw Error('Invalid sample timing');
      let samples = 0, ticks = 0;
      for (let i = 0; i < count; i++) {const at = timing.start + 8 + i * 8, n = bytes.readUInt32BE(at), delta = bytes.readUInt32BE(at + 4); samples += n; ticks += n * delta;}
      video.fps = samples * clock.scale / ticks;
      if (!Number.isFinite(video.fps) || video.fps < 1 || video.fps > 240) throw Error('Invalid frame rate');
    } else {
      if (sample.type !== 'mp4a' || sample.start + 28 > sample.end || bytes.readUInt16BE(sample.start + 8) !== 0) throw Error('Unsupported audio sample');
      const extensions = children(bytes, sample.start + 28, sample.end);
      if (extensions.some(b => b.type === 'sinf')) throw Error('Encrypted audio');
      const config = extensions.find(b => b.type === 'esds');
      if (!config) throw Error('Missing AAC descriptor');
      aac(bytes, config);
      audio.push({id, lang: /^[a-z]{3}$/.test(lang) ? lang : 'und', default: enabled, codec: 'aac', name: (lang || 'Original') + ' · AAC-LC'});
    }
  }
  if (!video) throw Error('Missing supported video');
  const defaultId = (audio.find(a => a.default) || audio[0])?.id;
  for (const a of audio) a.default = a.id === defaultId;
  return {...video, duration: movie.duration, codec: 'h264', codecTag: 'avc1', pixelFormat: 'yuv420p', hdr: false, dolbyVision: false, audio, metadataMethod: 'mp4-header'};
}
export async function mp4Metadata(file, {timeout = 8000} = {}) {
  if (!/\.(mp4|m4v)$/i.test(file.name)) return null;
  let reader, total = 0, boxes = 0;
  const deadline = AbortSignal.timeout(timeout);
  const abort = () => reader?.destroy(Error('MP4 metadata read timed out'));
  deadline.addEventListener('abort', abort);
  const read = async (start, length) => {
    if (deadline.aborted || length <= 0 || total + length > LIMIT + 16384 || start + length > file.length) throw Error('Invalid metadata read');
    total += length; const chunks = []; let received = 0;
    reader = viewingStream(file, {start, end: start + length - 1, signal: deadline});
    for await (const chunk of reader) {received += chunk.length; if (received > length) throw Error('Oversized metadata read'); chunks.push(chunk);}
    reader = null;
    if (received !== length) throw Error('Truncated metadata read');
    return Buffer.concat(chunks, received);
  };
  const keep = {moov: ['mvhd', 'trak', 'mvex'], trak: ['tkhd', 'mdia'], mdia: ['mdhd', 'hdlr', 'minf'], minf: ['stbl'], stbl: ['stsd', 'stts']};
  const compact = async (type, start, end) => {
    const parts = [];
    for (let at = start; at < end;) {
      if (++boxes > 512) throw Error('Too many metadata boxes');
      const prefix = await read(at, Math.min(16, end - at));
      const child = box(prefix, 0, end - at);
      if (type === 'moov' && child.type === 'mvex') throw Error('Fragmented MP4 needs full probing');
      if (keep[type].includes(child.type)) {
        const body = keep[child.type]
          ? await compact(child.type, at + child.start, at + child.end)
          : await read(at + child.start, child.end - child.start);
        const header = Buffer.alloc(8); header.writeUInt32BE(body.length + 8); header.write(child.type, 4, 'ascii');
        parts.push(header, body);
      }
      at += child.size;
    }
    return Buffer.concat(parts);
  };
  try {
    for (let at = 0, count = 0; at + 8 <= file.length && count < 128; count++) {
      const prefix = await read(at, Math.min(16, file.length - at));
      // Bounds are relative to the entire file; only the small box header is in memory.
      const entry = box(prefix, 0, file.length - at);
      if (count === 0 && entry.type !== 'ftyp') return null;
      if (entry.type === 'moov') {
        const length = entry.end - entry.start;
        if (length > LIMIT) return null;
        // Keep only codec, dimensions, language and sample timing. Large stsz,
        // stco, ctts and stsc tables remain in the source for native playback.
        const metadata = length <= 128 * 1024
          ? await read(at + entry.start, length)
          : await compact('moov', at + entry.start, at + entry.end);
        return {...parseMp4Metadata(metadata), metadataReadBytes: total};
      }
      if (entry.type === 'moof') return null;
      at += entry.size;
    }
    return null;
  } catch {return null;}
  finally {reader?.destroy(); deadline.removeEventListener('abort', abort);}
}
