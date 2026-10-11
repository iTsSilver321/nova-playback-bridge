# NOVA playback bridge

An independently authored, ad-free torrent playback server for NOVA. Browser-safe H.264/AAC and hvc1 HEVC/AAC MP4 files can play at their original resolution, including 4K, without entering the conversion queue. Authenticated capability links expose Safari-compatible byte ranges. Sources requiring conversion use requested 16-second H.264/AAC batches with 480p/720p/1080p/1440p/2160p choices bounded by source resolution. The complete VOD timeline supports seeking and audio selection. No local engine is required.

Version 4.6 reads supported AVC 8-bit 4:2:0 / AAC-LC MP4 metadata directly from its bounded movie header. It skips the intervening movie payload, works with front or tail headers, preserves the actual AVC codec string, and does not wait for an occupied converter queue. Unknown formats, rotation, encryption, HDR, excessive metadata or failed reads retain full FFmpeg analysis. Header reads have an eight-second deadline and cancel their underlying torrent readers. This optimizes one startup stage; it does not guarantee a percentage improvement in network startup or uninterrupted playback.

Version 4.6.1 starts full analysis after a 250 ms head start if direct metadata is still pending. The two paths race; the loser is cancelled, including queued or running FFmpeg work. Unsupported containers start full analysis immediately. This avoids serially adding a slow header read's timeout to FFmpeg startup.

Version 4.4 also repackages indexed H.264 and HEVC MKV video into seekable fragmented-MP4 HLS without video encoding. The Matroska seek index supplies actual keyframe boundaries. Audio is copied when AAC-LC, otherwise converted to AAC-LC stereo. Actual avc1/hvc1 codec strings guide browser support; unsupported formats and packaging failures fall back to H.264/AAC conversion. Dolby Vision and unsupported pixel formats use conversion. Copy segments are packaged on demand and the twelve most recent fragments are retained per viewer. Recently closed torrent metadata and verified pieces remain reusable for two minutes.

Consecutive copied segments share one packager job, bounded to four indexed segments and normally sixty seconds. The larger time budget also groups films with long keyframe intervals. Each completed fragment is published while the job continues. Random seeks start a fresh batch at the requested index. Completed fragments preserve source frames exactly once; queued batch failures are handled without unhandled promise rejections. The browser's MSE copy player builds an eight-second starting buffer where possible; a bounded timeout and manual Play remain available. Native Safari HLS uses its own loading strategy.

NOVA's Worker creates owned sessions with a server-only shared secret. Playback is available to guests; a signed HttpOnly cookie binds each viewer to their sessions without creating an account. Signed-in profile PINs and family filters remain enforced. The bridge accepts hashes and file indices rather than arbitrary URLs. Access uses expiring random capability links. Internal range reads stay on a separate loopback listener; approved original MP4 bytes use the public capability route. Fixed session counts are disabled by default. Memory checks, queued conversions, disk checks, timeouts, idle cleanup and a private-address torrent peer blocklist bound actual resource use. Metadata probes are reused for viewers of the same file. Links may be shared with a watch party, so possession of a playback URL grants playback until expiration. Never publish NOVA_BRIDGE_KEY or log playback paths.

There is no fixed movie-file size ceiling. A shared rolling disk cache retains recently requested, verified torrent pieces. Active read windows are leased against eviction; evicted pieces become missing and are downloaded again when seeking backwards. WebTorrent 3.0.21 is pinned because its piece-state adapter uses internal invalidation alongside the standard lt_donthave extension, with revised bitfield fallback for peers without that extension. Conversion retains the three most recently used completed batches per viewer, and protects batches currently being read. Physical disk and memory remain finite.

Piece reads allocate only the requested byte range, including the small blocks used by peer uploads. Queued uploads do not retain entire multi-megabyte pieces. A regression checks both byte identity and bounded retained allocation for a 16 KB request from a 2 MB piece.

Browser byte probes are also bounded. WebTorrent's zero-end sentinel is normalized before reading, and the wrapper trims each requested window. A `bytes=0-0` request returns exactly one byte without accidentally selecting the entire movie for download.

This server does not supply movie licenses, peers, translations or an exhaustive catalog. Use media you have permission to stream. Torrent availability and the host's resources still determine whether a source plays. The software is free and contains no advertising. Hosting has separate resource policies: Render's free plan sleeps, shares compute, has monthly transfer limits, and may suspend high-volume outbound traffic. It cannot provide unlimited public streaming. No paid plan is configured.

Render Hobby currently includes 5 GB of outbound bandwidth per workspace per month. Without a saved payment method, exceeding the allowance suspends free services instead of billing overages. If a payment method is added later, usage can generate charges. There is no mechanism in NOVA that upgrades hosting, buys credits or starts paid subscriptions. Account-level allowances include other services and cannot be inferred from NOVA's cache size. Check https://render.com/docs/outbound-bandwidth and https://render.com/docs/free before changing billing or interpreting the free plan as unlimited.

## Deploy

The isolated repository is generated by `node scripts/export-bridge.mjs` from NOVA's source. Its Dockerfile installs FFmpeg and the exact Node dependencies. The service binds `0.0.0.0:$PORT`. Runtime settings:

| Variable | Purpose |
| --- | --- |
| NOVA_BRIDGE_KEY | At least 32 random characters; the same value is stored privately in NOVA's Worker. |
| NOVA_ALLOWED_ORIGINS | Comma-separated HTTPS NOVA origins. |
| NOVA_BRIDGE_CACHE_MB | Shared rolling piece window; default/free deployment 1024 MB. This is reusable storage, not a maximum movie length or file size. The obsolete CACHE_GB variable is ignored. |
| NOVA_BRIDGE_JOBS | Concurrent converter jobs; free deployment 1. |
| NOVA_BRIDGE_SELF_WARM_SECONDS | HTTPS health watchdog; free deployment 240 seconds. First check after 5 seconds, failures retry after 30 seconds. Only the exact NOVA Render origin is accepted from RENDER_EXTERNAL_URL; unset/0 disables it. |
| NOVA_BRIDGE_SESSIONS | Optional count cap; 0 disables it, including on the free deployment. |
| NOVA_BRIDGE_RSS_MB | Node memory guard, default 384 MB, free deployment 320 MB to reserve converter headroom; low available system memory also prevents new sessions. |
| NOVA_BRIDGE_PRESET | `veryfast` by default; `ultrafast` on the free CPU for quicker conversion. Faster presets use more bandwidth. |
| NOVA_BRIDGE_IDLE_MINUTES | Inactive session expiry; default/free deployment 20 minutes. Closing a normal player releases its slot immediately. |
| NOVA_BRIDGE_CACHE | Cache directory, default `.nova-bridge-cache`; Docker `/cache`. |
| NOVA_FFMPEG | Optional FFmpeg executable override. |

Set `NOVA_BRIDGE_URL` to the service's returned HTTPS origin and `NOVA_BRIDGE_KEY` through Sites runtime environment settings. `/api/playback` in NOVA reports configuration and readiness and creates the anonymous ownership cookie. Guests can prepare and close their own sessions. Private library APIs still require ChatGPT sign-in. New sessions respect signed-in profile PINs. Requests are atomically limited per account or guest, with an additional hashed network cooldown for guests.

The public repository includes a GitHub Actions health check every five minutes on standard free public-repository runners. It retries a sleeping bridge and validates its protocol response. This is best-effort warming, not an uptime guarantee: schedules can be delayed/dropped and public scheduled workflows stop after 60 days of repository inactivity. Warm Render time consumes the workspace's shared 750 free instance hours each calendar month. The visible NOVA app also checks readiness periodically. No paid runner, billing change, infinite CI process, or advertisement is configured.

The in-process watchdog additionally sends a real HTTPS request through Render's public ingress every four minutes, with bounded timeout and retry. `/health` reports successful checks and last-success time. It stops cleanly during shutdown. This removes reliance on timely CI scheduling while the process is running; external CI and the visible app can wake a stopped process. Free-hour quotas, host restarts and service suspensions still apply.

The HLS capability links expire after six hours; idle sessions expire after twenty minutes. A sleeping/restarted host loses transient cache and sessions, so reopen the source. Cloud libraries remain in NOVA's separate database. Hardware decoding, AirPlay and casting depend on browser/receiver support. Conversion on a small free instance can buffer on demanding files.

## Verification and provenance

`npm run test:bridge` in NOVA generates an HEVC/MKV fixture with two audio streams and uses an authored CC0 4K MP4 fixture. Real TCP transfers verify rolling eviction with a source larger than the cache, backwards seeks, conversion, three simultaneous viewers from the same owner, byte-identical original 3840x2160 playback, actual 2160p conversion, HEAD/206/416 responses, authorization and expiration. `node tests/rolling-tcp.mjs` independently verifies a 4 MB byte-identical source through a 256 KB cache, including discarded-piece re-fetch after backward and forward seeks. Browser and physical-device verification are separate from these checks. Dependencies retain their own licenses: [WebTorrent](https://github.com/webtorrent/webtorrent), [FFmpeg](https://ffmpeg.org/legal.html), [ffmpeg-static](https://github.com/eugeneware/ffmpeg-static). The Docker FFmpeg build includes GPL components; upstream sources and notices remain in Debian's package distribution.
