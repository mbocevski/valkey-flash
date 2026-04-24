# Auto-demotion stall benchmark

- Cache size: 4096 KiB (fixed)
- Flash capacity: 4096 MiB (fixed)
- Demotion batch: 128 keys / tick (default `MAX_DEMOTIONS_PER_TICK`)
- Tick interval: 100 ms (default `TICK_INTERVAL`)
- Measurement window: 30 s per cell
- Background reader load: 200 GET/s on a native-RAM key

Client GET p99 is the external-observer stall proxy: the reader hits a
native-tier key, so any p99 bump reflects event-loop contention from
the demotion tick, not flash I/O.

## Headline finding

Demotion throughput is capped at ~1250 keys/s across all shapes at the
200 B and 1 KiB size tiers — exactly `MAX_DEMOTIONS_PER_TICK * (1 s /
TICK_INTERVAL)` = 128 × 10 = 1280. Under sustained overwrite pressure
faster than that, S3-FIFO silently auto-evicts hot-cache entries
before the demotion tick can back them up to NVMe. This is not data
loss (the Valkey-native `Tier::Hot` payload stays resident — FlashCache
is a shadow lookup cache), but it is a *tiering failure*: the module's
value proposition — spilling RAM to NVMe — does not fire.

At 4 KiB and larger, client writes are slow enough per-op that the
demotion tick cannot keep up at all: the candidate queue drains but
every popped key has already been silently dropped from the cache by
the time the tick inspects it, so `auto_demotions_total` never
increments. Keys remain Hot in Valkey's native keyspace; no RAM is
freed. Client p99 stays flat (60–70 µs) precisely *because* demotion
never runs.

The ZSET / 4 KiB cell is the only mid-size outlier (636 keys/s, 2k
tiered) — zset encoding is denser per in-RAM byte than hash/list, so
the same 4 KiB external payload produces fewer cache-resident entries
and the queue drain stays ahead of the silent-eviction rate.

Implication for the async-write refactor: the bottleneck is not
stall-per-tick (GET p99 is modest even in the throughput-limited
cells) but total demotion throughput. Moving `storage.put` to the
thread pool will parallelise demotion across `flash.io-threads`,
scaling throughput by Nx and keeping tiering functional under
realistic write rates.

## STRING

| Value size | Demotions/s | Tiered keys | Client GET p50 | Client GET p99 | Client GET p999 | Storage used |
|---|---|---|---|---|---|---|
| 200B | 1,254 | 11,704 | 23.7 µs | 89.3 µs | 802.9 µs | 694 MiB |
| 1KiB | 1,267 | 4,875 | 24.1 µs | 109.6 µs | 970.8 µs | 167 MiB |
| 4KiB | 0 | 0 | 24.9 µs | 62.0 µs | 157.8 µs | 96 MiB |
| 16KiB | 0 | 0 | 24.9 µs | 65.3 µs | 237.3 µs | 60 MiB |

## HASH

| Value size | Demotions/s | Tiered keys | Client GET p50 | Client GET p99 | Client GET p999 | Storage used |
|---|---|---|---|---|---|---|
| 200B | 1,254 | 12,045 | 24.4 µs | 107.3 µs | 981.9 µs | 694 MiB |
| 1KiB | 1,267 | 5,239 | 24.8 µs | 157.4 µs | 1058.8 µs | 167 MiB |
| 4KiB | 0 | 0 | 25.9 µs | 74.2 µs | 254.7 µs | 96 MiB |
| 16KiB | 0 | 0 | 25.4 µs | 66.5 µs | 179.6 µs | 60 MiB |

## LIST

| Value size | Demotions/s | Tiered keys | Client GET p50 | Client GET p99 | Client GET p999 | Storage used |
|---|---|---|---|---|---|---|
| 200B | 1,254 | 13,518 | 25.1 µs | 116.8 µs | 1056.7 µs | 694 MiB |
| 1KiB | 1,267 | 4,545 | 26.1 µs | 167.7 µs | 858.3 µs | 167 MiB |
| 4KiB | 0 | 0 | 24.7 µs | 63.9 µs | 214.7 µs | 96 MiB |
| 16KiB | 0 | 0 | 24.2 µs | 65.8 µs | 238.4 µs | 60 MiB |

## ZSET

| Value size | Demotions/s | Tiered keys | Client GET p50 | Client GET p99 | Client GET p999 | Storage used |
|---|---|---|---|---|---|---|
| 200B | 1,233 | 13,375 | 25.8 µs | 105.2 µs | 886.4 µs | 694 MiB |
| 1KiB | 1,267 | 8,404 | 26.1 µs | 155.5 µs | 923.7 µs | 167 MiB |
| 4KiB | 636 | 1,986 | 25.0 µs | 80.3 µs | 634.4 µs | 48 MiB |
| 16KiB | 0 | 0 | 24.1 µs | 62.9 µs | 152.6 µs | 36 MiB |

