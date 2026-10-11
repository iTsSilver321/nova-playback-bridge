import http from "node:http";
import { randomBytes, timingSafeEqual } from "node:crypto";
import { mkdir, stat, statfs, rm } from "node:fs/promises";
import { createReadStream } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import WebTorrent from "webtorrent";
import ffmpegStatic from "ffmpeg-static";
import { parseRange, videoMime } from "../range.mjs";
import {
  BridgeError,
  sourceRequest,
  selectVideo,
  availableQualities,
  variantPlaylist,
  masterPlaylist,
  originalPlayable,
  BATCH_SEGMENTS,
  SEGMENT_SECONDS,
} from "./protocol.mjs";
import { probe, encodeBatch, remuxBatch, JobQueue } from "./transcoder.mjs";
import {
  RollingCache,
  RollingPieceStore,
  viewingStream,
} from "./rolling-store.mjs";
import { matroskaIndex, codecString } from "./matroska.mjs";
import { mp4Metadata } from "./mp4-metadata.mjs";
import { startWarmWatchdog } from "./warm-watchdog.mjs";

const TRACKERS = [
  "udp://tracker.opentrackr.org:1337/announce",
  "udp://open.stealth.si:80/announce",
  "udp://tracker.torrent.eu.org:451/announce",
];
// Approved CC film metadata also supplies its official HTTPS web seed. Ordinary
// provider hashes still use only DHT/trackers; callers cannot provide a URL.
const OPEN_TORRENTS = {
  "08ada5a7a6183aae1e09d831df6748d566095a10":
    "https://webtorrent.io/torrents/sintel.torrent",
  // Independently authored CC0 3840x2160 diagnostic clip for hosted verification.
  "8223c04e2bdf0f9f81f19bf47009f6fa1326b928":
    "https://raw.githubusercontent.com/iTsSilver321/nova-playback-bridge/main/fixtures/NOVA-4K.torrent",
  ed9047bca56aafa5cb5b9ef3362b6604c2a9ac32:
    "https://raw.githubusercontent.com/iTsSilver321/nova-playback-bridge/main/fixtures/NOVA-HD.torrent",
  "82e4facca52144ebff065413622676bafe3d69b3":
    "https://raw.githubusercontent.com/iTsSilver321/nova-playback-bridge/main/fixtures/NOVA-HEVC-4K.torrent",
};
const PRIVATE_RANGES = [
  "0.0.0.0/8",
  "10.0.0.0/8",
  "127.0.0.0/8",
  "169.254.0.0/16",
  "172.16.0.0/12",
  "192.168.0.0/16",
  "224.0.0.0/4",
  "240.0.0.0/4",
];
const equal = (a, b) => {
  const x = Buffer.from(a || ""),
    y = Buffer.from(b || "");
  return x.length === y.length && timingSafeEqual(x, y);
};
const json = (res, status, data) => {
  res.writeHead(status, {
    "Content-Type": "application/json",
    "Cache-Control": "no-store",
  });
  res.end(JSON.stringify(data));
};
async function readBody(req) {
  let bytes = 0;
  const chunks = [];
  for await (const chunk of req) {
    bytes += chunk.length;
    if (bytes > 8192) throw new BridgeError("Request too large.", 413);
    chunks.push(chunk);
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString());
  } catch {
    throw new BridgeError("Use a JSON request.");
  }
}
const bounded = (value, fallback, min, max) =>
  Number.isFinite(Number(value))
    ? Math.max(min, Math.min(max, Math.floor(Number(value))))
    : fallback;

// This is a standalone server, deliberately outside the Sites Worker bundle.
export async function createBridge(options = {}) {
  const secret = options.secret || process.env.NOVA_BRIDGE_KEY;
  if (!secret || secret.length < 32)
    throw new Error("NOVA_BRIDGE_KEY must contain at least 32 characters.");
  const root = path.resolve(
    options.cachePath || process.env.NOVA_BRIDGE_CACHE || ".nova-bridge-cache",
  );
  await mkdir(root, { recursive: true });
  const cacheBytes =
    options.cacheBytes ??
    bounded(process.env.NOVA_BRIDGE_CACHE_MB, 1024, 64, 64000) * 1024 ** 2;
  const pieceCache = new RollingCache(path.join(root, "pieces"), cacheBytes);
  // Zero removes the fixed session-count cap. Actual memory/disk and converter
  // capacity are still enforced; unlimited compute is not implied.
  const configuredSessions = bounded(
      options.maxSessions ?? process.env.NOVA_BRIDGE_SESSIONS,
      0,
      0,
      10000,
    ),
    maxSessions = configuredSessions || Infinity;
  const maxRss =
    bounded(
      options.maxRssMb ?? process.env.NOVA_BRIDGE_RSS_MB,
      384,
      64,
      64000,
    ) *
    1024 ** 2;
  const idleMs =
      options.idleMs ??
      bounded(process.env.NOVA_BRIDGE_IDLE_MINUTES, 20, 1, 120) * 60 * 1000,
    ttlMs = options.ttlMs ?? 6 * 60 * 60 * 1000;
  const ffmpeg = options.ffmpeg || process.env.NOVA_FFMPEG || ffmpegStatic;
  const origins = new Set(
    options.origins ||
      (
        process.env.NOVA_ALLOWED_ORIGINS ||
        "https://nova-cinema-eduar.eduardochiana25.chatgpt.site"
      )
        .split(",")
        .map((s) => s.trim()),
  );
  const packs = new Map(),
    packSources = new Map(),
    probeCache = new Map(),
    preparations = new Map(),
    cancelledPreparations = new Map();
  const bootId = randomBytes(8).toString("hex"),
    startedAt = Date.now();
  const sessions = new Map(),
    torrents = new Map(),
    jobs = new JobQueue(
      bounded(options.concurrency ?? process.env.NOVA_BRIDGE_JOBS, 2, 1, 16),
    );
  const client =
    options.client ||
    new WebTorrent({
      maxConns: 50,
      lsd: false,
      natUpnp: false,
      natPmp: false,
      webSeeds: true,
      blocklist: PRIVATE_RANGES,
      uploadLimit: 512 * 1024,
    });
  client.on("error", () => {
    for (const s of sessions.values())
      s.error = "The torrent network stopped. Reconnect to another source.";
  });
  let closing = false;
  const warm = startWarmWatchdog({
    url:
      Number(process.env.NOVA_BRIDGE_SELF_WARM_SECONDS) > 0
        ? process.env.RENDER_EXTERNAL_URL
        : "",
    intervalMs:
      bounded(process.env.NOVA_BRIDGE_SELF_WARM_SECONDS, 240, 60, 600) * 1000,
  });
  function memoryAvailable() {
    if (
      process.memoryUsage().rss > maxRss ||
      (process.availableMemory?.() ?? Infinity) < 64 * 1024 ** 2
    )
      throw new BridgeError(
        "The bridge is busy. Retry when memory becomes available.",
        503,
      );
  }
  async function quota(extra = 0) {
    const info = await statfs(root);
    if (Number(info.bavail) * Number(info.bsize) < 512 * 1024 ** 2 + extra)
      throw new BridgeError(
        "The bridge has insufficient free disk space.",
        503,
      );
  }
  async function getTorrent(hash) {
    const existing = torrents.get(hash);
    if (existing) {
      existing.used = Date.now();
      return existing.promise;
    }
    memoryAvailable();
    if (torrents.size >= maxSessions)
      throw new BridgeError(
        "All torrent slots are in use. Retry shortly.",
        503,
      );
    await quota();
    // Another metadata request may have acquired this hash during the disk check.
    if (torrents.has(hash)) return torrents.get(hash).promise;
    if (torrents.size >= maxSessions)
      throw new BridgeError(
        "All torrent slots are in use. Retry shortly.",
        503,
      );
    const state = {
      used: Date.now(),
      torrent: null,
      promise: null,
      probes: new Map(),
    };
    torrents.set(hash, state);
    state.promise = new Promise((resolve, reject) => {
      const magnet = `magnet:?xt=urn:btih:${hash}&${TRACKERS.map((t) => "tr=" + encodeURIComponent(t)).join("&")}`;
      const t = client.add(
        OPEN_TORRENTS[hash] || magnet,
        {
          path: path.join(root, "torrents"),
          addUID: true,
          deselect: true,
          store: RollingPieceStore,
          storeOpts: { pool: pieceCache },
          storeCacheSlots: 0,
          destroyStoreOnDestroy: true,
        },
        (t) => {
          clearTimeout(timer);
          if (t.infoHash !== hash) {
            reject(
              new BridgeError("The approved source metadata changed.", 502),
            );
            return;
          }
          if (t.files.length > 10000) {
            reject(new BridgeError("This torrent has too many files.", 413));
            return;
          }
          t.deselect(0, t.pieces.length - 1, 0);
          for (const f of t.files) f.deselect();
          state.torrent = t;
          resolve(t);
        },
      );
      const timer = setTimeout(() => {
        reject(
          new BridgeError(
            "No torrent peers supplied metadata. Try a better-seeded release.",
            504,
          ),
        );
      }, options.metadataTimeout || 90000);
      t.once("error", () => {
        clearTimeout(timer);
        reject(new BridgeError("The torrent source could not be opened.", 502));
      });
    }).catch(async (e) => {
      torrents.delete(hash);
      await client.remove(hash, { destroyStore: true }).catch(() => {});
      throw e;
    });
    return state.promise;
  }
  const raw = http.createServer((req, res) => {
    const match = /^\/raw\/([a-f0-9]{32})\/([a-f0-9]{48})$/.exec(
      req.url?.split("?")[0] || "",
    );
    const s = match && (sessions.get(match[1]) || packSources.get(match[1]));
    if (!s || !equal(match[2], s.rawToken)) {
      json(res, 404, { error: "Not found." });
      return;
    }
    let range;
    try {
      range = parseRange(req.headers.range, s.file.length);
    } catch {
      res.writeHead(416, { "Content-Range": `bytes */${s.file.length}` });
      res.end();
      return;
    }
    const start = range?.start ?? 0,
      end = range?.end ?? s.file.length - 1,
      headers = {
        "Content-Type": videoMime(s.file.name),
        "Content-Length": end - start + 1,
        "Accept-Ranges": "bytes",
      };
    if (range)
      headers["Content-Range"] = `bytes ${start}-${end}/${s.file.length}`;
    res.writeHead(range ? 206 : 200, headers);
    if (req.method === "HEAD") {
      res.end();
      return;
    }
    const stream = viewingStream(s.file, { start, end });
    s.readers.add(stream);
    stream.once("error", () => res.destroy());
    res.once("close", () => {
      s.readers.delete(stream);
      stream.destroy();
    });
    stream.pipe(res);
    res.setTimeout(180000, () => {
      stream.destroy();
      res.destroy();
    });
  });
  await new Promise((resolve) => raw.listen(0, "127.0.0.1", resolve));
  const rawPort = raw.address().port;
  async function disposePack(p) {
    if (p.disposing) return;
    p.disposing = true;
    packs.delete(p.key);
    packSources.delete(p.id);
    for (const job of p.work.values()) {
      job.controller.abort();
      job.child?.kill();
    }
    for (const child of p.children) child.kill();
    for (const r of p.readers) r.destroy();
    await Promise.allSettled([...p.batches.values(), ...p.copyBatches]);
    const target = path.resolve(root, "sessions", p.id);
    if (!target.startsWith(root + path.sep)) throw Error("Invalid cache path");
    await rm(target, { recursive: true, force: true });
  }
  async function prunePacks() {
    const limit =
      options.packCacheBytes ??
      bounded(process.env.NOVA_BRIDGE_PACK_CACHE_MB, 256, 32, 2048) * 1024 ** 2;
    let bytes = [...packs.values()].reduce(
      (n, p) =>
        n +
        p.segmentBytes +
        [...p.copies.values()].reduce((v, e) => v + (e.info?.size || 0), 0),
      0,
    );
    for (const p of [...packs.values()].sort((a, b) => a.used - b.used)) {
      if (!p.users.size && (Date.now() - p.used > 120000 || bytes > limit)) {
        bytes -=
          p.segmentBytes +
          [...p.copies.values()].reduce((n, e) => n + (e.info?.size || 0), 0);
        await disposePack(p);
        continue;
      }
      for (const [key, e] of [...p.copies].sort(
        (a, b) => a[1].used - b[1].used,
      )) {
        if (bytes <= limit) break;
        if (!e.info || p.copyReaders.get(key) || Date.now() - e.used < 15000)
          continue;
        const divider = key.lastIndexOf("-"),
          audio = key.slice(0, divider),
          index = key.slice(divider + 1);
        await rm(
          path.join(
            root,
            "sessions",
            p.id,
            "copy-" + audio,
            "copy-init-" + index + ".mp4",
          ),
          { force: true },
        );
        await rm(
          path.join(
            root,
            "sessions",
            p.id,
            "copy-" + audio,
            "copy-segment-" + index + ".m4s",
          ),
          { force: true },
        );
        bytes -= e.info.size;
        p.copies.delete(key);
      }
    }
  }
  function cancelObsolete(p, index) {
    const active = [...p.users].filter(
      (id) => sessions.get(id)?.active !== false,
    );
    if (active.length > 1) return;
    for (const job of p.work.values())
      if (index < job.first - 30 || index > job.last + 30) {
        job.controller.abort();
        job.child?.kill();
      }
  }
  async function dispose(s) {
    if (s.disposing) return;
    s.disposing = true;
    sessions.delete(s.id);
    for (const reader of s.readers) reader.destroy();
    if (s.pack) {
      s.pack.users.delete(s.id);
      s.pack.used = Date.now();
      if (!s.pack.users.size)
        for (const job of s.pack.work.values()) {
          job.controller.abort();
          job.child?.kill();
        }
    } else for (const child of s.children) child.kill();
    const target = path.resolve(root, "sessions", s.id);
    if (!target.startsWith(root + path.sep))
      throw new Error("Cache path escaped its root.");
    await rm(target, { recursive: true, force: true });
    if (
      ![...sessions.values()].some((other) => other.infoHash === s.infoHash)
    ) {
      const state = torrents.get(s.infoHash);
      if (state) state.used = Date.now();
    }
  }
  async function start(value) {
    const request = sourceRequest(value),
      owner = String(value.owner || "");
    if (!/^[a-f0-9]{64}$/.test(owner))
      throw new BridgeError("Missing trusted viewer identifier.", 403);
    const ticket = /^[a-f0-9-]{36}$/.test(value.requestId || "")
      ? value.requestId
      : null;
    const phase = (name) => {
      if (ticket)
        preparations.set(owner + ":" + ticket, {
          phase: name,
          updated: Date.now(),
        });
    };
    phase("metadata");
    if (ticket && cancelledPreparations.has(owner + ":" + ticket))
      throw new BridgeError("Preparation was cancelled.", 410);
    memoryAvailable();
    if (sessions.size >= maxSessions)
      throw new BridgeError(
        "The bridge has reached its configured playback capacity.",
        429,
      );
    // Reserve before awaiting metadata, so parallel requests cannot evade limits.
    const s = {
      id: randomBytes(16).toString("hex"),
      token: randomBytes(24).toString("hex"),
      rawToken: randomBytes(24).toString("hex"),
      owner,
      requestId: ticket,
      infoHash: request.infoHash,
      used: Date.now(),
      expires: Date.now() + ttlMs,
      file: null,
      info: null,
      segmentBytes: 0,
      batches: new Map(),
      completedBatches: new Map(),
      segmentReaders: new Map(),
      copies: new Map(),
      copyBatches: new Set(),
      copyReaders: new Map(),
      children: new Set(),
      readers: new Set(),
      error: "",
      disposing: false,
    };
    sessions.set(s.id, s);
    try {
      const t = await getTorrent(request.infoHash),
        chosen = selectVideo(t.files, request);
      s.file = chosen.file;
      let warmBytes = 0;
      if (value.warm) {
        const file = s.file;
        s.file = {
          name: file.name,
          length: file.length,
          offset: file.offset,
          _torrent: file._torrent,
          createReadStream: (range) => {
            const reader = file.createReadStream(range);
            reader.on("data", (chunk) => {
              warmBytes += chunk.length;
              if (warmBytes > 24 * 1024 ** 2)
                reader.destroy(
                  new BridgeError(
                    "Metadata warmup reached its byte budget.",
                    413,
                  ),
                );
            });
            return reader;
          },
        };
      }
      s.input = `http://127.0.0.1:${rawPort}/raw/${s.id}/${s.rawToken}`;
      const state = torrents.get(request.infoHash);
      phase("index");
      const probeKey = request.infoHash + ":" + chosen.index;
      const cached = probeCache.get(probeKey);
      let info =
        state.probes.get(chosen.index) ||
        (cached && Date.now() - cached.used < 1800000
          ? Promise.resolve(cached.info)
          : null);
      if (!info) {
        // A compatible MP4 needs no decoder or converter slot to describe it.
        // Unknown, malformed, HDR and other formats still use the existing probe.
        info = (async () => {
          const controller = new AbortController();
          let release;
          const gate = new Promise(resolve => { release = resolve; });
          const timer = setTimeout(() => release(), 250);
          controller.signal.addEventListener("abort", release, { once: true });
          const direct = mp4Metadata(s.file, { signal: controller.signal }).then(value => {
            if (value) return value;
            release();
            throw new BridgeError("This file needs full media analysis.", 422);
          });
          // A slow header read must not add its entire timeout to cold startup.
          // Start the fallback after a short head start; cancel whichever loses.
          const full = gate.then(() => jobs.run(async () => {
            const metadata = await probe(
              s.input,
              ffmpeg,
              options.probeTimeout || 90000,
              controller.signal,
            );
            if (
              !metadata.dolbyVision &&
              ((metadata.codec === "h264" &&
                /^(yuv420p|yuvj420p)$/.test(metadata.pixelFormat)) ||
                (metadata.codec === "hevc" &&
                  /^(yuv420p|yuv420p10le)$/.test(metadata.pixelFormat)))
            ) {
              const index = await matroskaIndex(s.file, metadata.duration);
              const codec =
                index && codecString(metadata.codec, index.privateData);
              if (codec) metadata.copy = { segments: index.segments, codec };
            }
            return { ...metadata, metadataMethod: "ffmpeg" };
          }, controller.signal));
          try { return await Promise.any([direct, full]); }
          catch (error) { throw error.errors?.[1] || error; }
          finally { clearTimeout(timer); controller.abort(); }
        })()
          .catch((e) => {
            state.probes.delete(chosen.index);
            probeCache.delete(probeKey);
            throw e;
          });
        state.probes.set(chosen.index, info);
      }
      s.info = await info;
      if (
        closing ||
        s.disposing ||
        (ticket && cancelledPreparations.has(owner + ":" + ticket))
      )
        throw new BridgeError("Preparation was cancelled.", 410);
      s.file = chosen.file;
      probeCache.delete(probeKey);
      probeCache.set(probeKey, { info: s.info, used: Date.now() });
      if (probeCache.size > 64)
        probeCache.delete(probeCache.keys().next().value);
      const packKey = probeKey;
      let pack = packs.get(packKey);
      if (!pack) {
        pack = {
          ...s,
          id: randomBytes(16).toString("hex"),
          rawToken: randomBytes(24).toString("hex"),
          readers: new Set(),
          key: packKey,
          users: new Set(),
          work: new Map(),
          readahead: 60,
        };
        pack.input =
          "http://127.0.0.1:" +
          rawPort +
          "/raw/" +
          pack.id +
          "/" +
          pack.rawToken;
        packs.set(packKey, pack);
        packSources.set(pack.id, pack);
      }
      pack.used = Date.now();
      pack.users.add(s.id);
      s.pack = pack;
      s.active = true;
      s.input = pack.input;
      for (const key of [
        "batches",
        "completedBatches",
        "segmentReaders",
        "copies",
        "copyBatches",
        "copyReaders",
        "children",
      ])
        s[key] = pack[key];
      phase("ready");
      if (closing || s.disposing)
        throw new BridgeError("The bridge is shutting down.", 503);
      const qualities = availableQualities(s.info.height),
        audio = s.info.audio[0]?.id ?? -1;
      s.qualities = qualities;
      s.original = originalPlayable(s.file, s.info);
      const prefix = `/v1/media/${s.id}/${s.token}/`;
      const originalAudio = s.info.audio.find((a) => a.default)?.id ?? audio;
      return {
        id: s.id,
        path: prefix + `master.m3u8?a=${audio}`,
        expires: s.expires,
        duration: s.info.duration,
        audio: s.info.audio,
        qualities,
        name: s.file.name,
        bitrate: Math.round((s.file.length * 8) / s.info.duration),
        fps: s.info.fps,
        hdr: !!s.info.hdr,
        dolbyVision: !!s.info.dolbyVision,
        metadataMethod: s.info.metadataMethod,
        metadataReadBytes: s.info.metadataReadBytes,
        ...(s.info.copy
          ? {
              copy: {
                path: prefix + "copy-master.m3u8",
                height: s.info.height,
                width: s.info.width,
                codec: s.info.copy.codec,
              },
            }
          : {}),
        ...(s.original
          ? {
              original: {
                path: prefix + "file.mp4",
                height: s.info.height,
                width: s.info.width,
                codec: s.info.codec,
                codecString: s.info.codecString,
                audioId: originalAudio,
              },
            }
          : {}),
      };
    } catch (e) {
      phase("failed");
      await dispose(s);
      throw e;
    }
  }
  async function copied(s, index, audio) {
    cancelObsolete(s, s.info.copy.segments[index].start);
    s.used = Date.now();
    const key = `${audio}-${index}`;
    if (s.copies.has(key)) {
      s.copies.get(key).used = Date.now();
      return s.copies.get(key).promise;
    }
    const plan = [];
    for (
      let n = index;
      n < Math.min(index + 8, s.info.copy.segments.length);
      n++
    ) {
      const timing = s.info.copy.segments[n],
        k = `${audio}-${n}`;
      if (
        s.copies.has(k) ||
        (n > index &&
          timing.start + timing.duration - s.info.copy.segments[index].start >
            s.readahead)
      )
        break;
      const entry = {
        used: Date.now(),
        promise: null,
        info: null,
        resolve: null,
        reject: null,
      };
      entry.promise = new Promise((resolve, reject) => {
        entry.resolve = resolve;
        entry.reject = reject;
      });
      entry.promise.catch(() => {});
      s.copies.set(k, entry);
      plan.push({ index: n, ...timing });
    }
    const directory = path.join(root, "sessions", s.id, `copy-${audio}`);
    const controller = new AbortController(),
      work = {
        controller,
        child: null,
        first: plan[0].start,
        last: plan.at(-1).start + plan.at(-1).duration,
      };
    s.work.set(key, work);
    const pending = jobs
      .run(async () => {
        if (s.disposing) throw new BridgeError("This stream has closed.", 410);
        await quota(
          Math.max(
            32 * 1024 ** 2,
            (s.file.length / s.info.duration) *
              (plan.at(-1).start + plan.at(-1).duration - plan[0].start) *
              2,
          ),
        );
        const track = s.info.audio.find((a) => a.id === audio);
        await remuxBatch({
          ffmpeg,
          input: s.input,
          directory,
          segments: plan,
          codec: s.info.codec,
          audio,
          audioCopy: track?.codec === "aac" && /aac \(LC\)/i.test(track.name),
          onSegment: (n, info) => {
            const e = s.copies.get(`${audio}-${n}`);
            e.info = info;
            e.resolve(info);
          },
          onChild: (child) => {
            if (child) {
              work.child = child;
              s.children.add(child);
              if (controller.signal.aborted) child.kill();
            } else
              for (const c of s.children)
                if (c.exitCode !== null || c.signalCode !== null)
                  s.children.delete(c);
          },
        });
        for (const [old, e] of [...s.copies].sort(
          (a, b) => a[1].used - b[1].used,
        )) {
          if (s.copies.size <= 12) break;
          if (old === key || !e.info || s.copyReaders.get(old)) continue;
          const divider = old.lastIndexOf("-"),
            a = old.slice(0, divider),
            n = old.slice(divider + 1);
          await rm(
            path.join(
              root,
              "sessions",
              s.id,
              `copy-${a}`,
              `copy-init-${n}.mp4`,
            ),
            { force: true },
          );
          await rm(
            path.join(
              root,
              "sessions",
              s.id,
              `copy-${a}`,
              `copy-segment-${n}.m4s`,
            ),
            { force: true },
          );
          s.copies.delete(old);
        }
        await prunePacks();
      }, controller.signal)
      .catch((e) => {
        for (const part of plan) {
          const k = `${audio}-${part.index}`,
            entry = s.copies.get(k);
          if (entry && !entry.info) {
            entry.reject(e);
            s.copies.delete(k);
          }
        }
      })
      .finally(() => {
        s.copyBatches.delete(pending);
        s.work.delete(key);
      });
    s.copyBatches.add(pending);
    return s.copies.get(key).promise;
  }
  async function batch(s, number, quality, audio) {
    if (s.info.hdr || s.info.dolbyVision)
      throw new BridgeError(
        "Choose an SDR release for compatible playback. This source needs HDR support.",
        422,
      );
    cancelObsolete(s, number * SEGMENT_SECONDS);
    s.used = Date.now();
    const batch = Math.floor(number / BATCH_SEGMENTS),
      key = `${quality}-${audio}-${batch}`;
    if (s.batches.has(key)) return s.batches.get(key);
    const directory = path.join(root, "sessions", s.id, `${quality}-${audio}`);
    const controller = new AbortController(),
      work = {
        controller,
        child: null,
        first: batch * BATCH_SEGMENTS * SEGMENT_SECONDS,
        last: (batch + 1) * BATCH_SEGMENTS * SEGMENT_SECONDS,
      };
    s.work.set("convert-" + key, work);
    const promise = jobs
      .run(async () => {
        if (s.disposing) throw new BridgeError("This stream has closed.", 410);
        await quota(32 * 1024 ** 2);
        await encodeBatch({
          ffmpeg,
          input: s.input,
          directory,
          batch,
          quality,
          audio,
          duration: s.info.duration,
          threads: bounded(process.env.NOVA_BRIDGE_THREADS, 2, 1, 8),
          preset: process.env.NOVA_BRIDGE_PRESET || "veryfast",
          onChild: (child) => {
            if (child) {
              work.child = child;
              s.children.add(child);
              if (controller.signal.aborted) child.kill();
            } else
              for (const c of s.children)
                if (c.exitCode !== null || c.signalCode !== null)
                  s.children.delete(c);
          },
        });
        let size = 0;
        for (
          let i = batch * BATCH_SEGMENTS;
          i < (batch + 1) * BATCH_SEGMENTS &&
          i * SEGMENT_SECONDS < s.info.duration;
          i++
        )
          size += (await stat(path.join(directory, `segment-${i}.ts`))).size;
        s.segmentBytes += size;
        s.completedBatches.set(key, {
          directory,
          batch,
          size,
          used: Date.now(),
          readers: 0,
        });
        for (const [old, entry] of [...s.completedBatches].sort(
          (a, b) => a[1].used - b[1].used,
        )) {
          if (s.completedBatches.size <= 3) break;
          if (old === key || s.segmentReaders.get(old)) continue;
          for (
            let i = entry.batch * BATCH_SEGMENTS;
            i < (entry.batch + 1) * BATCH_SEGMENTS;
            i++
          )
            await rm(path.join(entry.directory, `segment-${i}.ts`), {
              force: true,
            });
          s.segmentBytes -= entry.size;
          s.completedBatches.delete(old);
          s.batches.delete(old);
        }
      }, controller.signal)
      .catch((e) => {
        s.batches.delete(key);
        throw e;
      })
      .finally(() => s.work.delete("convert-" + key));
    s.batches.set(key, promise);
    return promise;
  }
  // FFmpeg atomically renames each completed segment. Serve that segment as soon
  // as it is ready instead of waiting for the remaining batch to finish.
  async function readySegment(filename, pending, closed) {
    let finished = false,
      failure;
    pending.then(
      () => {
        finished = true;
      },
      (e) => {
        failure = e;
        finished = true;
      },
    );
    for (;;) {
      if (closed()) return null;
      try {
        const info = await stat(filename);
        if (info.size > 0) return info;
      } catch (e) {
        if (e.code !== "ENOENT") throw e;
      }
      if (failure) throw failure;
      if (finished)
        throw new BridgeError(
          "This source did not produce a playable segment.",
          502,
        );
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }
  const server = http.createServer(async (req, res) => {
    res.setHeader("X-Content-Type-Options", "nosniff");
    res.setHeader("Referrer-Policy", "no-referrer");
    const origin = req.headers.origin;
    if (origin && !origins.has(origin)) {
      json(res, 403, { error: "Origin not allowed." });
      return;
    }
    if (origin) {
      res.setHeader("Access-Control-Allow-Origin", origin);
      res.setHeader("Vary", "Origin");
      res.setHeader(
        "Access-Control-Expose-Headers",
        "Content-Range, Content-Length, Accept-Ranges",
      );
    }
    if (req.method === "OPTIONS") {
      res.writeHead(204, {
        "Access-Control-Allow-Methods": "GET,HEAD,OPTIONS",
        "Access-Control-Allow-Headers": "Range",
        "Access-Control-Max-Age": "600",
      });
      res.end();
      return;
    }
    try {
      if (closing) throw new BridgeError("The bridge is shutting down.", 503);
      const url = new URL(req.url, "http://bridge.invalid");
      if (url.pathname === "/health" && req.method === "GET") {
        json(res, 200, {
          status: "ok",
          protocol: 1,
          version: "4.6.2",
          bootId,
          startedAt,
          warm: warm.state,
        });
        return;
      }
      if (url.pathname === "/v1/sessions" && req.method === "POST") {
        if (!equal(req.headers.authorization, `Bearer ${secret}`))
          throw new BridgeError("Not authorized.", 401);
        json(res, 200, await start(await readBody(req)));
        return;
      }
      if (url.pathname === "/v1/resources" && req.method === "GET") {
        if (!equal(req.headers.authorization, "Bearer " + secret))
          throw new BridgeError("Not authorized.", 401);
        const disk = await statfs(root);
        json(res, 200, {
          memoryBytes: process.memoryUsage().rss,
          diskFreeBytes:Number(disk.bavail)*Number(disk.bsize),
          memoryBudget: maxRss,
          pieceBytes: pieceCache.bytes,
          pieceBudget: cacheBytes,
          packBytes: [...packs.values()].reduce(
            (n, p) =>
              n +
              p.segmentBytes +
              [...p.copies.values()].reduce(
                (v, e) => v + (e.info?.size || 0),
                0,
              ),
            0,
          ),
          sessions: sessions.size,
          torrents: torrents.size,
          packs: packs.size,
          queue: jobs.pending.length,
          running: jobs.running,
          uptime: process.uptime(),
          warm: warm.state,
          freeHours: null,
          transferRemaining: null,
        });
        return;
      }
      if (url.pathname === "/v1/preparation" && req.method === "POST") {
        if (!equal(req.headers.authorization, "Bearer " + secret))
          throw new BridgeError("Not authorized.", 401);
        const value = await readBody(req),
          key = value.owner + ":" + value.requestId,
          entry = preparations.get(key);
        if (value.cancel) {
          cancelledPreparations.set(key, Date.now());
          const owned = [...sessions.values()].find(
            (s) =>
              s.requestId === value.requestId && equal(s.owner, value.owner),
          );
          if (owned) await dispose(owned);
          preparations.delete(key);
        }
        json(res, 200, { phase: entry?.phase || "connecting" });
        return;
      }
      const statusSession = /^\/v1\/sessions\/([a-f0-9]{32})\/status$/.exec(
        url.pathname,
      );
      if (statusSession && req.method === "POST") {
        if (!equal(req.headers.authorization, "Bearer " + secret))
          throw new BridgeError("Not authorized.", 401);
        const value = await readBody(req),
          s = sessions.get(statusSession[1]);
        if (!s)
          throw new BridgeError(
            "Playback server restarted. Renew the source to continue.",
            410,
          );
        if (!equal(s.owner, value.owner))
          throw new BridgeError("This stream belongs to another viewer.", 403);
        s.used = Date.now();
        s.pack.used = Date.now();
        s.active = value.paused !== true;
        s.pack.readahead = s.active
          ? Number(value.buffer) < 12
            ? 90
            : 60
          : 20;
        const torrent = torrents.get(s.infoHash)?.torrent;
        let sample;
        if (value.measure) {
          if (s.measuredAt && Date.now() - s.measuredAt < 20000)
            throw new BridgeError(
              "Wait before measuring this source again.",
              429,
            );
          s.measuredAt = Date.now();
          const before = torrent?.downloaded || 0,
            started = Date.now(),
            start = Math.floor(s.file.length * 0.25),
            reader = viewingStream(s.file, {
              start,
              end: Math.min(s.file.length - 1, start + 2 * 1024 ** 2 - 1),
            });
          s.readers.add(reader);
          let bytes = 0;
          const timer = setTimeout(
            () =>
              reader.destroy(
                new BridgeError("The source speed check timed out.", 504),
              ),
            10000,
          );
          try {
            for await (const chunk of reader) bytes += chunk.length;
          } finally {
            clearTimeout(timer);
            reader.destroy();
            s.readers.delete(reader);
          }
          const elapsed = (Date.now() - started) / 1000,
            received = Math.max(0, (torrent?.downloaded || 0) - before);
          sample = {
            bytes,
            seconds: elapsed,
            networkBytes: received,
            verified: received >= 65536 && elapsed > 0.2,
            speed: Math.round(received / Math.max(0.2, elapsed)),
          };
        }
        json(res, 200, {
          peers: torrent?.numPeers || 0,
          downloadSpeed: Math.round(torrent?.downloadSpeed || 0),
          bitrate: Math.round((s.file.length * 8) / s.info.duration),
          queue: jobs.pending.length,
          mode: s.original ? "original" : s.info.copy ? "copy" : "conversion",
          codec: s.info.codec,
          hdr: !!s.info.hdr,
          ...(sample ? { sample } : {}),
        });
        return;
      }
      const closingSession = /^\/v1\/sessions\/([a-f0-9]{32})\/close$/.exec(
        url.pathname,
      );
      if (closingSession && req.method === "POST") {
        if (!equal(req.headers.authorization, `Bearer ${secret}`))
          throw new BridgeError("Not authorized.", 401);
        const value = await readBody(req),
          s = sessions.get(closingSession[1]);
        if (s && !equal(s.owner, String(value.owner || "")))
          throw new BridgeError("This stream belongs to another viewer.", 403);
        if (s) dispose(s).catch(() => {});
        json(res, 202, { closed: true });
        return;
      }
      const match =
        /^\/v1\/media\/([a-f0-9]{32})\/([a-f0-9]{48})\/(master\.m3u8|index\.m3u8|file\.mp4|segment-\d+\.ts|copy-master\.m3u8|copy-index\.m3u8|copy-init-\d+\.mp4|copy-segment-\d+\.m4s)$/.exec(
          url.pathname,
        );
      if (!match || !["GET", "HEAD"].includes(req.method))
        throw new BridgeError("Not found.", 404);
      const s = sessions.get(match[1]);
      if (!s || !equal(match[2], s.token) || !s.info)
        throw new BridgeError(
          "This stream has expired. Open the source again.",
          410,
        );
      if (s.expires < Date.now()) {
        await dispose(s);
        throw new BridgeError(
          "This stream has expired. Open the source again.",
          410,
        );
      }
      if (s.error) throw new BridgeError(s.error, 503);
      s.used = Date.now();
      if (match[3] === "file.mp4") {
        if (!s.original)
          throw new BridgeError("This source needs browser conversion.", 422);
        await quota();
        let range;
        try {
          range = parseRange(req.headers.range, s.file.length);
        } catch {
          res.setHeader("Content-Range", `bytes */${s.file.length}`);
          throw new BridgeError("Invalid byte range.", 416);
        }
        const start = range?.start ?? 0,
          end = range?.end ?? s.file.length - 1;
        const headers = {
          "Content-Type": "video/mp4",
          "Content-Length": end - start + 1,
          "Accept-Ranges": "bytes",
          "Cache-Control": "private, no-store",
        };
        if (range)
          headers["Content-Range"] = `bytes ${start}-${end}/${s.file.length}`;
        res.writeHead(range ? 206 : 200, headers);
        if (req.method === "HEAD") {
          res.end();
          return;
        }
        const stream = viewingStream(s.file, { start, end });
        s.readers.add(stream);
        const quotaTimer = setInterval(() => {
          s.used = Date.now();
          quota().catch(() => {
            stream.destroy();
            res.destroy();
          });
        }, 5000);
        quotaTimer.unref();
        const clean = () => {
          clearInterval(quotaTimer);
          s.readers.delete(stream);
          stream.destroy();
        };
        stream.once("error", () => res.destroy());
        res.once("close", clean);
        res.setTimeout(180000, () => res.destroy());
        stream.pipe(res);
        return;
      }
      const a = Number(url.searchParams.get("a") ?? s.info.audio[0]?.id ?? -1),
        q = Number(url.searchParams.get("q") ?? 720);
      if (
        !Number.isInteger(a) ||
        (a !== -1 && !s.info.audio.some((track) => track.id === a))
      )
        throw new BridgeError("Unknown audio track.");
      if (match[3].startsWith("copy-")) {
        if (!s.info.copy)
          throw new BridgeError(
            "This source has no compatible indexed video.",
            422,
          );
        if (match[3] === "copy-master.m3u8") {
          const codecs = s.info.copy.codec + (a >= 0 ? ",mp4a.40.2" : ""),
            rate = Math.max(
              256000,
              Math.ceil(((s.file.length * 8) / s.info.duration) * 1.35),
            );
          res.writeHead(200, {
            "Content-Type": "application/vnd.apple.mpegurl",
            "Cache-Control": "private, no-store",
          });
          res.end(
            req.method === "HEAD"
              ? undefined
              : `#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-STREAM-INF:BANDWIDTH=${rate},RESOLUTION=${s.info.width}x${s.info.height},CODECS="${codecs}"\ncopy-index.m3u8?a=${a}\n`,
          );
          return;
        }
        if (match[3] === "copy-index.m3u8") {
          const rows = [
            "#EXTM3U",
            "#EXT-X-VERSION:7",
            `#EXT-X-TARGETDURATION:${Math.ceil(s.info.copy.segments.reduce((max, p) => Math.max(max, p.duration), 0) + 0.25)}`,
            "#EXT-X-MEDIA-SEQUENCE:0",
            "#EXT-X-PLAYLIST-TYPE:VOD",
            "#EXT-X-INDEPENDENT-SEGMENTS",
          ];
          s.info.copy.segments.forEach((p, n) => {
            if (n) rows.push("#EXT-X-DISCONTINUITY");
            rows.push(
              `#EXT-X-MAP:URI="copy-init-${n}.mp4?a=${a}"`,
              `#EXTINF:${p.duration.toFixed(6)},`,
              `copy-segment-${n}.m4s?a=${a}`,
            );
          });
          rows.push("#EXT-X-ENDLIST", "");
          res.writeHead(200, {
            "Content-Type": "application/vnd.apple.mpegurl",
            "Cache-Control": "private, no-store",
          });
          res.end(req.method === "HEAD" ? undefined : rows.join("\n"));
          return;
        }
        const n = Number(/copy-(?:init|segment)-(\d+)/.exec(match[3])[1]);
        if (!Number.isSafeInteger(n) || !s.info.copy.segments[n])
          throw new BridgeError("Segment not found.", 404);
        const key = `${a}-${n}`;
        s.copyReaders.set(key, (s.copyReaders.get(key) || 0) + 1);
        res.once("close", () => {
          const count = (s.copyReaders.get(key) || 1) - 1;
          if (count) s.copyReaders.set(key, count);
          else s.copyReaders.delete(key);
        });
        const info = await copied(s.pack, n, a);
        if (res.destroyed) return;
        const init = match[3].startsWith("copy-init"),
          filename = path.join(
            root,
            "sessions",
            s.pack.id,
            `copy-${a}`,
            match[3],
          ),
          length = init ? info.offset : info.length,
          offset = init ? 0 : info.offset;
        let range;
        try {
          range = parseRange(req.headers.range, length);
        } catch {
          res.setHeader("Content-Range", `bytes */${length}`);
          throw new BridgeError("Invalid byte range.", 416);
        }
        const start = range?.start ?? 0,
          end = range?.end ?? length - 1,
          headers = {
            "Content-Type": "video/mp4",
            "Content-Length": end - start + 1,
            "Accept-Ranges": "bytes",
            "Cache-Control": "private, max-age=120",
          };
        if (range) headers["Content-Range"] = `bytes ${start}-${end}/${length}`;
        res.writeHead(range ? 206 : 200, headers);
        if (req.method === "HEAD") {
          res.end();
          return;
        }
        const reader = createReadStream(filename, {
          start: offset + start,
          end: offset + end,
        });
        s.readers.add(reader);
        reader.once("error", () => res.destroy());
        res.once("close", () => {
          s.readers.delete(reader);
          reader.destroy();
        });
        reader.pipe(res);
        return;
      }
      if (match[3] === "master.m3u8") {
        const value = masterPlaylist(
          s.qualities,
          a,
          s.info.width,
          s.info.height,
        );
        res.writeHead(200, {
          "Content-Type": "application/vnd.apple.mpegurl",
          "Cache-Control": "private, no-store",
        });
        res.end(req.method === "HEAD" ? undefined : value);
        return;
      }
      if (!s.qualities.includes(q)) throw new BridgeError("Unknown quality.");
      if (match[3] === "index.m3u8") {
        res.writeHead(200, {
          "Content-Type": "application/vnd.apple.mpegurl",
          "Cache-Control": "private, no-store",
        });
        res.end(
          req.method === "HEAD"
            ? undefined
            : variantPlaylist(s.info.duration, q, a),
        );
        return;
      }
      const number = Number(/segment-(\d+)/.exec(match[3])[1]);
      if (
        !Number.isSafeInteger(number) ||
        number * SEGMENT_SECONDS >= s.info.duration
      )
        throw new BridgeError("Segment not found.", 404);
      const batchKey = `${q}-${a}-${Math.floor(number / BATCH_SEGMENTS)}`;
      s.segmentReaders.set(batchKey, (s.segmentReaders.get(batchKey) || 0) + 1);
      res.once("close", () => {
        const remaining = (s.segmentReaders.get(batchKey) || 1) - 1;
        if (remaining) s.segmentReaders.set(batchKey, remaining);
        else s.segmentReaders.delete(batchKey);
      });
      const retained = s.completedBatches.get(batchKey);
      if (retained) retained.used = Date.now();
      const pending = batch(s.pack, number, q, a);
      const filename = path.join(
          root,
          "sessions",
          s.pack.id,
          `${q}-${a}`,
          `segment-${number}.ts`,
        ),
        info = await readySegment(
          filename,
          pending,
          () => res.destroyed || s.disposing,
        );
      if (!info) return;
      res.writeHead(200, {
        "Content-Type": "video/mp2t",
        "Content-Length": info.size,
        "Cache-Control": "private, max-age=120",
      });
      if (req.method === "HEAD") {
        res.end();
        return;
      }
      const stream = createReadStream(filename);
      stream.once("error", () => res.destroy());
      res.once("close", () => stream.destroy());
      stream.pipe(res);
    } catch (e) {
      options.onError?.(e);
      if (!res.headersSent)
        json(res, e instanceof BridgeError ? e.status : 500, {
          error:
            e instanceof BridgeError
              ? e.message
              : "The playback bridge failed. Try another source.",
        });
      else res.destroy();
    }
  });
  server.requestTimeout = 300000;
  server.headersTimeout = 30000;
  const timer = setInterval(() => {
    prunePacks().catch(() => {});
    for (const [key, time] of cancelledPreparations)
      if (Date.now() - time > 300000) cancelledPreparations.delete(key);
    for (const [key, value] of preparations)
      if (Date.now() - value.updated > 300000) preparations.delete(key);
    for (const [key, value] of probeCache)
      if (Date.now() - value.used > 1800000) probeCache.delete(key);
    for (const s of sessions.values())
      if (Date.now() - s.used > idleMs || Date.now() > s.expires)
        dispose(s).catch(() => {});
    for (const [hash, state] of torrents)
      if (
        Date.now() - state.used > 120000 &&
        ![...sessions.values()].some((s) => s.infoHash === hash) &&
        ![...packs.values()].some((p) => p.infoHash === hash && !p.disposing)
      ) {
        torrents.delete(hash);
        client.remove(hash, { destroyStore: true }).catch(() => {});
      }
  }, 30000);
  timer.unref();
  return {
    server,
    sessions,
    client,
    pieceCache,
    packs,
    jobs,
    async close() {
      closing = true;
      warm.stop();
      clearInterval(timer);
      await Promise.allSettled([...sessions.values()].map(dispose));
      await Promise.allSettled([...packs.values()].map(disposePack));
      await new Promise((resolve) => client.destroy(resolve));
      await new Promise((resolve) => raw.close(resolve));
      if (server.listening)
        await new Promise((resolve) => {
          server.close(resolve);
          server.closeAllConnections();
        });
    },
  };
}
if (
  process.argv[1] &&
  path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  const bridge = await createBridge();
  const port = bounded(
    process.env.PORT || process.env.NOVA_BRIDGE_PORT,
    11570,
    1024,
    65535,
  );
  bridge.server.listen(port, process.env.NOVA_BRIDGE_BIND || "127.0.0.1", () =>
    console.log(`NOVA playback bridge listening on port ${port}`),
  );
  for (const signal of ["SIGINT", "SIGTERM"])
    process.once(signal, () => bridge.close().then(() => process.exit(0)));
}
