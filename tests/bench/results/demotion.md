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
| 200B | 1,254 | 12,079 | 23.6 µs | 67.2 µs | 510.5 µs | 694 MiB |
| 1KiB | 1,265 | 4,800 | 25.0 µs | 74.7 µs | 626.0 µs | 167 MiB |
| 4KiB | 0 | 0 | 25.1 µs | 63.8 µs | 200.8 µs | 96 MiB |
| 16KiB | 0 | 0 | 24.8 µs | 59.1 µs | 180.8 µs | 60 MiB |

## HASH

| Value size | Demotions/s | Tiered keys | Client GET p50 | Client GET p99 | Client GET p999 | Storage used |
|---|---|---|---|---|---|---|
| 200B | 1,254 | 12,991 | 25.3 µs | 80.0 µs | 806.9 µs | 694 MiB |
| 1KiB | 1,267 | 4,458 | 26.3 µs | 83.8 µs | 841.5 µs | 167 MiB |
| 4KiB | 0 | 0 | 26.4 µs | 67.3 µs | 217.6 µs | 96 MiB |
| 16KiB | 0 | 0 | 26.2 µs | 66.0 µs | 196.4 µs | 60 MiB |

## LIST

| Value size | Demotions/s | Tiered keys | Client GET p50 | Client GET p99 | Client GET p999 | Storage used |
|---|---|---|---|---|---|---|
| 200B | 1,254 | 13,410 | 24.7 µs | 71.0 µs | 695.8 µs | 694 MiB |
| 1KiB | 1,264 | 4,979 | 25.1 µs | 76.7 µs | 659.3 µs | 167 MiB |
| 4KiB | 0 | 0 | 26.7 µs | 65.9 µs | 215.5 µs | 96 MiB |
| 16KiB | 0 | 0 | 24.1 µs | 62.5 µs | 217.8 µs | 60 MiB |

## ZSET

| Value size | Demotions/s | Tiered keys | Client GET p50 | Client GET p99 | Client GET p999 | Storage used |
|---|---|---|---|---|---|---|
| 200B | 1,233 | 13,840 | 24.0 µs | 70.7 µs | 538.7 µs | 694 MiB |
| 1KiB | 1,267 | 8,072 | 25.2 µs | 77.7 µs | 641.5 µs | 167 MiB |
| 4KiB | 636 | 1,920 | 26.3 µs | 68.6 µs | 308.0 µs | 48 MiB |
| 16KiB | 0 | 0 | 25.6 µs | 65.0 µs | 257.6 µs | 36 MiB |

