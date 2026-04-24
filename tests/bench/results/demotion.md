# Auto-demotion stall benchmark

- Cache size: 4096 KiB (fixed)
- Flash capacity: 4096 MiB (fixed)
- Demotion pipeline: phase-1 submit on the event loop, phase-2 NVMe
  write on `AsyncThreadPool`, phase-3 commit drained at the top of
  the next event-loop tick.
- Measurement window: 30 s per cell
- Background reader load: 200 GET/s on a native-RAM key

Client GET p99 is the external-observer stall proxy: the reader hits a
native-tier key, so any p99 bump reflects event-loop contention from
the demotion tick's phase-1 work (payload clone + pool submit), not
flash I/O. Demotion throughput columns show the rate at which keys
reach NVMe cold tier under sustained cache overflow.

## STRING

| Value size | Demotions/s | Tiered keys | Client GET p50 | Client GET p99 | Client GET p999 | Storage used |
|---|---|---|---|---|---|---|
| 200B | 79 | 2,368 | 24.8 µs | 62.9 µs | 237.6 µs | 694 MiB |
| 1KiB | 80 | 2,488 | 25.6 µs | 64.5 µs | 206.6 µs | 167 MiB |
| 4KiB | 80 | 108 | 26.2 µs | 72.0 µs | 255.9 µs | 96 MiB |
| 16KiB | 80 | 88 | 24.7 µs | 62.8 µs | 195.1 µs | 60 MiB |

## HASH

| Value size | Demotions/s | Tiered keys | Client GET p50 | Client GET p99 | Client GET p999 | Storage used |
|---|---|---|---|---|---|---|
| 200B | 79 | 2,376 | 24.1 µs | 62.8 µs | 236.3 µs | 694 MiB |
| 1KiB | 80 | 2,488 | 24.4 µs | 64.3 µs | 168.0 µs | 167 MiB |
| 4KiB | 80 | 104 | 24.5 µs | 64.6 µs | 204.0 µs | 96 MiB |
| 16KiB | 80 | 88 | 26.8 µs | 70.3 µs | 205.6 µs | 60 MiB |

## LIST

| Value size | Demotions/s | Tiered keys | Client GET p50 | Client GET p99 | Client GET p999 | Storage used |
|---|---|---|---|---|---|---|
| 200B | 79 | 2,376 | 24.1 µs | 60.9 µs | 165.4 µs | 694 MiB |
| 1KiB | 80 | 2,488 | 24.4 µs | 61.3 µs | 205.7 µs | 167 MiB |
| 4KiB | 80 | 104 | 24.2 µs | 63.9 µs | 161.6 µs | 96 MiB |
| 16KiB | 80 | 88 | 23.5 µs | 61.3 µs | 235.9 µs | 60 MiB |

## ZSET

| Value size | Demotions/s | Tiered keys | Client GET p50 | Client GET p99 | Client GET p999 | Storage used |
|---|---|---|---|---|---|---|
| 200B | 78 | 2,336 | 26.4 µs | 69.7 µs | 235.3 µs | 694 MiB |
| 1KiB | 80 | 2,480 | 24.6 µs | 60.5 µs | 196.2 µs | 167 MiB |
| 4KiB | 80 | 1,920 | 26.5 µs | 68.4 µs | 231.7 µs | 48 MiB |
| 16KiB | 80 | 96 | 23.6 µs | 62.0 µs | 203.4 µs | 36 MiB |

