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

| Value size | Demotions/s | Tick p50 | Tick p99 | Stalls | Client GET p99 | NVMe B / demote | Storage used |
|---|---|---|---|---|---|---|---|
| 200B | 79 | 11.0 µs | 17.0 µs | 0 | 62.5 µs | 290,214 B | 694 MiB |
| 1KiB | 80 | 12.0 µs | 16.0 µs | 0 | 61.6 µs | 55,924 B | 167 MiB |
| 4KiB | 80 | 161.0 µs | 221.0 µs | 0 | 66.1 µs | 28,056 B | 96 MiB |
| 16KiB | 80 | 30.0 µs | 121.0 µs | 0 | 63.3 µs | 17,476 B | 60 MiB |
| 64KiB | 0 | 27.0 µs | 33.0 µs | 0 | 68.5 µs | 0 B | 51 MiB |

## HASH

| Value size | Demotions/s | Tick p50 | Tick p99 | Stalls | Client GET p99 | NVMe B / demote | Storage used |
|---|---|---|---|---|---|---|---|
| 200B | 79 | 13.0 µs | 16.0 µs | 0 | 64.2 µs | 289,237 B | 694 MiB |
| 1KiB | 80 | 14.0 µs | 21.0 µs | 0 | 67.5 µs | 55,924 B | 167 MiB |
| 4KiB | 80 | 156.0 µs | 193.0 µs | 0 | 65.8 µs | 28,056 B | 96 MiB |
| 16KiB | 80 | 42.0 µs | 154.0 µs | 0 | 63.8 µs | 17,542 B | 60 MiB |
| 64KiB | 0 | 25.0 µs | 36.0 µs | 0 | 66.1 µs | 0 B | 51 MiB |

## LIST

| Value size | Demotions/s | Tick p50 | Tick p99 | Stalls | Client GET p99 | NVMe B / demote | Storage used |
|---|---|---|---|---|---|---|---|
| 200B | 79 | 12.0 µs | 15.0 µs | 0 | 65.6 µs | 290,214 B | 694 MiB |
| 1KiB | 80 | 13.0 µs | 17.0 µs | 0 | 64.7 µs | 55,924 B | 167 MiB |
| 4KiB | 80 | 154.0 µs | 213.0 µs | 0 | 68.0 µs | 28,056 B | 96 MiB |
| 16KiB | 80 | 38.0 µs | 141.0 µs | 0 | 63.2 µs | 17,484 B | 60 MiB |
| 64KiB | 0 | 25.0 µs | 33.0 µs | 0 | 65.4 µs | 0 B | 51 MiB |

## ZSET

| Value size | Demotions/s | Tick p50 | Tick p99 | Stalls | Client GET p99 | NVMe B / demote | Storage used |
|---|---|---|---|---|---|---|---|
| 200B | 78 | 12.0 µs | 15.0 µs | 0 | 63.4 µs | 294,190 B | 694 MiB |
| 1KiB | 80 | 12.0 µs | 17.0 µs | 0 | 63.9 µs | 55,924 B | 167 MiB |
| 4KiB | 80 | 15.0 µs | 26.0 µs | 0 | 64.8 µs | 14,028 B | 48 MiB |
| 16KiB | 80 | 26.0 µs | 85.0 µs | 0 | 62.6 µs | 10,486 B | 36 MiB |
| 64KiB | 79 | 66.0 µs | 94.0 µs | 0 | 83.5 µs | 8,041 B | 27 MiB |


### Column glossary

- **Tick p50 / p99** — phase-1 wall time sampled from `flash_demotion_tick_last_us` at 100 ms intervals over the cell's measurement window. Tracks the event-loop stall budget the adaptive AIMD controller uses; see `STALL_BUDGET_US` in `src/demotion.rs`.
- **Stalls** — delta of `flash_demotion_stall_events_total` over the cell. Non-zero means at least one tick exceeded `STALL_BUDGET_US` and the AIMD controller halved the effective batch cap.
- **Client GET p99** — latency of a background native-RAM GET loop; an external-observer proxy for event-loop contention from demotion.
- **NVMe B / demote** — write amplification: `storage_used_bytes` delta divided by the number of committed demotions. For the default 4 KiB-aligned allocator this should hover near `max(4096, value_size rounded up)`; substantial drift indicates a leak or accounting bug.

