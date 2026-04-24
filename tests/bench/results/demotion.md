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
| 200B | 79 | 2,368 | 23.7 µs | 59.6 µs | 222.1 µs | 694 MiB |
| 1KiB | 80 | 2,488 | 23.7 µs | 59.8 µs | 146.3 µs | 167 MiB |
| 4KiB | 80 | 105 | 24.5 µs | 61.4 µs | 189.0 µs | 96 MiB |
| 16KiB | 80 | 88 | 23.5 µs | 58.4 µs | 176.3 µs | 60 MiB |

## HASH

| Value size | Demotions/s | Tiered keys | Client GET p50 | Client GET p99 | Client GET p999 | Storage used |
|---|---|---|---|---|---|---|
| 200B | 79 | 2,376 | 24.5 µs | 64.8 µs | 175.7 µs | 694 MiB |
| 1KiB | 80 | 2,488 | 26.5 µs | 68.8 µs | 223.4 µs | 167 MiB |
| 4KiB | 80 | 106 | 24.5 µs | 65.7 µs | 209.7 µs | 96 MiB |
| 16KiB | 79 | 95 | 24.1 µs | 64.0 µs | 259.6 µs | 60 MiB |

## LIST

| Value size | Demotions/s | Tiered keys | Client GET p50 | Client GET p99 | Client GET p999 | Storage used |
|---|---|---|---|---|---|---|
| 200B | 79 | 2,368 | 24.0 µs | 60.2 µs | 138.7 µs | 694 MiB |
| 1KiB | 80 | 2,488 | 23.6 µs | 62.0 µs | 182.6 µs | 167 MiB |
| 4KiB | 80 | 104 | 24.6 µs | 67.9 µs | 227.6 µs | 96 MiB |
| 16KiB | 80 | 96 | 26.6 µs | 69.8 µs | 202.7 µs | 60 MiB |

## ZSET

| Value size | Demotions/s | Tiered keys | Client GET p50 | Client GET p99 | Client GET p999 | Storage used |
|---|---|---|---|---|---|---|
| 200B | 78 | 2,336 | 24.4 µs | 61.2 µs | 240.7 µs | 694 MiB |
| 1KiB | 80 | 2,480 | 24.2 µs | 64.3 µs | 242.9 µs | 167 MiB |
| 4KiB | 80 | 1,928 | 25.0 µs | 65.2 µs | 246.7 µs | 48 MiB |
| 16KiB | 80 | 104 | 24.0 µs | 65.0 µs | 253.0 µs | 36 MiB |

