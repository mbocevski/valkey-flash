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
| 200B | 4,949 | 25,234 | 23.6 µs | 1179.2 µs | 2115.3 µs | 696 MiB |
| 1KiB | 2,526 | 5,185 | 23.6 µs | 341.8 µs | 1406.3 µs | 169 MiB |
| 4KiB | 0 | 0 | 24.3 µs | 63.0 µs | 219.6 µs | 96 MiB |
| 16KiB | 0 | 0 | 23.3 µs | 63.8 µs | 158.4 µs | 60 MiB |

## HASH

| Value size | Demotions/s | Tiered keys | Client GET p50 | Client GET p99 | Client GET p999 | Storage used |
|---|---|---|---|---|---|---|
| 200B | 4,949 | 27,759 | 24.7 µs | 1292.7 µs | 2282.2 µs | 696 MiB |
| 1KiB | 2,526 | 5,822 | 24.9 µs | 352.7 µs | 1455.3 µs | 167 MiB |
| 4KiB | 0 | 0 | 25.6 µs | 65.6 µs | 171.3 µs | 96 MiB |
| 16KiB | 0 | 0 | 24.1 µs | 62.5 µs | 181.8 µs | 60 MiB |

## LIST

| Value size | Demotions/s | Tiered keys | Client GET p50 | Client GET p99 | Client GET p999 | Storage used |
|---|---|---|---|---|---|---|
| 200B | 4,949 | 26,987 | 24.2 µs | 1031.8 µs | 2138.3 µs | 696 MiB |
| 1KiB | 2,526 | 4,194 | 24.5 µs | 255.3 µs | 1209.6 µs | 169 MiB |
| 4KiB | 0 | 0 | 24.1 µs | 65.8 µs | 247.4 µs | 96 MiB |
| 16KiB | 0 | 0 | 23.8 µs | 61.2 µs | 243.3 µs | 60 MiB |

## ZSET

| Value size | Demotions/s | Tiered keys | Client GET p50 | Client GET p99 | Client GET p999 | Storage used |
|---|---|---|---|---|---|---|
| 200B | 4,864 | 18,922 | 25.0 µs | 1108.4 µs | 2075.0 µs | 696 MiB |
| 1KiB | 2,526 | 9,403 | 24.8 µs | 268.6 µs | 1082.4 µs | 169 MiB |
| 4KiB | 2,514 | 2,384 | 24.6 µs | 310.5 µs | 1287.2 µs | 48 MiB |
| 16KiB | 0 | 0 | 23.9 µs | 67.1 µs | 225.2 µs | 36 MiB |

