# valkey-flash Architecture

This document captures the architectural decisions behind valkey-flash.

## 1. Overview

valkey-flash is a Valkey module that tiers key/value data to NVMe storage, letting a Valkey instance serve a working set larger than RAM by transparently demoting cold data to flash. Hot entries live in an in-memory cache; cold entries reside on an NVMe-backed file. Valkey proper remains unaware of the flash tier — the module exposes `FLASH.*` commands that users opt into per-key.

**At a glance:**
- **Data types**: four custom types — `FlashString`, `FlashHash`, `FlashList`, `FlashZSet`. Each registered as its own `ValkeyType` with separate RDB layout and callback set.
- **Commands**: ~50 `FLASH.*` commands covering basic CRUD on strings + hashes, full list operations (push/pop/range/move/blocking variants), full sorted-set operations (add/range/score/store/blocking variants), plus admin / cluster / debug commands. See [§3 Command reference](#3-command-reference) for the complete list.
- **Storage**: file-backed `io_uring` NVMe backend; S3-FIFO cache layer.
- **Durability**: append-only WAL with three sync modes (`always` / `everysec` / `no`) plus standard Valkey RDB + AOF.
- **Cluster**: native `MIGRATE` support for slot migration, per-key atomic, probe-gated by module availability.
- **Known gaps (planned for v1.1.1)**: `FLASH.EXISTS`, `FLASH.EXPIRE` family, `FLASH.MGET` / `FLASH.MSET`, `FLASH.COPY` / `FLASH.RENAME`, `FLASH.INCR` / `FLASH.APPEND` family. Full list in [§12 Known limitations](#12-known-limitations).
- **Out of scope** (data types deliberately not in flash tier): sets, streams, bitmaps, HyperLogLog, geo. Use native Valkey for these.

## 2. Component map

```
            ┌──────────────────────── FLASH.* command handlers ────────────────────────┐
            │  Strings    SET  GET  DEL                                                │
            │  Hashes     HSET  HGET  HGETALL  HDEL  HEXISTS  HLEN                     │
            │  Lists      LPUSH  RPUSH  LPOP  RPOP  LRANGE  LSET  LINSERT  LMOVE  …   │
            │  Sorted set ZADD  ZRANGE  ZSCORE  ZRANK  ZINCRBY  ZUNIONSTORE  …        │
            │  Admin      AUX.INFO  CONVERT  DRAIN  COMPACTION.{STATS,TRIGGER}  …     │
            │  Cluster    MIGRATE  MIGRATE.PROBE                                       │
            └───┬──────────────┬──────────────┬──────────────┬───────────────┬─────────┘
                │              │              │              │               │
                ▼              ▼              ▼              ▼               ▼
            ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
            │ FlashString  │ │ FlashHash    │ │ FlashList    │ │ FlashZSet    │
            │ (rdb/aof/    │ │ (rdb/aof/    │ │ (rdb/aof/    │ │ (rdb/aof/    │
            │  defrag/copy │ │  defrag/copy │ │  defrag/copy │ │  defrag/copy │
            │  /mem_usage2)│ │  /mem_usage2)│ │  /mem_usage2)│ │  /mem_usage2)│
            └──────┬───────┘ └──────┬───────┘ └──────┬───────┘ └──────┬───────┘
                   │                │                │                │
                   └────────────────┴────────┬───────┴────────────────┘
                                             ▼
                                ┌─────────────────────┐
                                │  FlashCache         │   ← S3-FIFO (quick_cache)
                                │  (hot tier, RAM)    │
                                └──────────┬──────────┘
                                           │ miss / demote / reclaim
                                           ▼
                                ┌─────────────────────┐
                                │  StorageBackend     │   ← trait; Mock for tests
                                │  FileIoUringBackend │      real backend (io-uring + O_DIRECT)
                                └──────────┬──────────┘
                                           │
                               ┌───────────┴────────────┐
                               ▼                        ▼
                        ┌──────────────┐          ┌──────────────┐
                        │  WAL          │         │  backing     │
                        │  (CRC32C,     │         │  file (NVMe) │
                        │   3 sync modes)│        │              │
                        └──────────────┘          └──────────────┘
```

Background threads: a compaction worker reclaims freed blocks; the WAL flusher thread handles `everysec` fsync; the async I/O thread pool serves cache-miss reads and cold writes via `BlockClient` / `ThreadSafeContext`.

## 3. Data types

**Per-shape types, not a unified type.** Each shape registers its own `ValkeyType` with its own module-type-id, encoding version, and callback set:

- `FlashString` — `src/types/string.rs` — holds `Tier::Hot(Vec<u8>)` or `Tier::Cold { key_hash, backend_offset, num_blocks, value_len }`
- `FlashHash` — `src/types/hash.rs` — same Tier shape, payload is serialized `HashMap<Vec<u8>, Vec<u8>>`
- `FlashList` — `src/types/list.rs` — same Tier shape, payload is a `VecDeque<Vec<u8>>` of list elements
- `FlashZSet` — `src/types/zset.rs` — same Tier shape, payload is a sorted skip-list backed by `BTreeMap<f64, Vec<Vec<u8>>>` for score lookup plus `HashMap<Vec<u8>, f64>` for member→score

Rationale: clean command dispatch, separate RDB layouts (simpler forward-compat when a shape gains features), per-type defrag cursor. The Tier enum is the single source of truth for whether a value is in RAM or NVMe; every callback dispatches on it.

### Command reference

The module currently registers ~50 `FLASH.*` commands. Grouped by data type:

#### Strings (FlashString)
`FLASH.SET`, `FLASH.GET`, `FLASH.DEL`

#### Hashes (FlashHash)
`FLASH.HSET`, `FLASH.HGET`, `FLASH.HGETALL`, `FLASH.HDEL`, `FLASH.HEXISTS`, `FLASH.HLEN`

#### Lists (FlashList)
`FLASH.LPUSH`, `FLASH.RPUSH`, `FLASH.LPUSHX`, `FLASH.RPUSHX`, `FLASH.LPOP`, `FLASH.RPOP`, `FLASH.LRANGE`, `FLASH.LLEN`, `FLASH.LINDEX`, `FLASH.LSET`, `FLASH.LINSERT`, `FLASH.LREM`, `FLASH.LTRIM`, `FLASH.LMOVE`, `FLASH.BLPOP`, `FLASH.BRPOP`, `FLASH.BLMOVE`

#### Sorted sets (FlashZSet)
`FLASH.ZADD`, `FLASH.ZCARD`, `FLASH.ZCOUNT`, `FLASH.ZINCRBY`, `FLASH.ZLEXCOUNT`, `FLASH.ZRANGE`, `FLASH.ZRANGEBYLEX`, `FLASH.ZRANGEBYSCORE`, `FLASH.ZRANGESTORE`, `FLASH.ZRANK`, `FLASH.ZREM`, `FLASH.ZREVRANGEBYLEX`, `FLASH.ZREVRANGEBYSCORE`, `FLASH.ZREVRANK`, `FLASH.ZSCAN`, `FLASH.ZSCORE`, `FLASH.ZUNIONSTORE`, `FLASH.ZINTERSTORE`, `FLASH.ZDIFFSTORE`, `FLASH.ZPOPMIN`, `FLASH.ZPOPMAX`, `FLASH.BZPOPMIN`, `FLASH.BZPOPMAX`

#### Admin
`FLASH.AUX.INFO`, `FLASH.CONVERT`, `FLASH.DRAIN`, `FLASH.DEBUG.DEMOTE`, `FLASH.DEBUG.STATE`, `FLASH.COMPACTION.STATS`, `FLASH.COMPACTION.TRIGGER`

#### Cluster
`FLASH.MIGRATE`, `FLASH.MIGRATE.PROBE`

### Out of scope (deferred indefinitely)

The module deliberately does not implement these data types — keep them in native Valkey:

- **Sets** (`SADD` / `SREM` / `SMEMBERS` / `SISMEMBER`): no flash-tier set type. Use native sets.
- **Streams** (`XADD` / `XREAD` / consumer groups): time-series append patterns are a poor fit for the demotion model.
- **Bitmaps** (`SETBIT` / `BITCOUNT` / `BITOP`): operate on small fixed buffers; little benefit from tiering.
- **HyperLogLog** (`PFADD` / `PFCOUNT`): fixed-size sketches, RAM-only is correct.
- **Geo** (`GEOADD` / `GEOSEARCH`): backed by sorted sets natively; if needed in flash, use `FLASH.Z*` directly.

Workloads that depend on these data types should run them in native Valkey alongside the flash module — the two coexist on the same instance.

## 4. Storage backend

**File-backed `io_uring`** via the `io-uring` crate. Single backing file at `flash.path`, pre-allocated with `fallocate` to `flash.capacity-bytes`, 4 KiB-aligned blocks with `O_DIRECT`.

- Rejected alternatives: raw block device + io_uring (requires `CAP_SYS_RAWIO`, problematic in containers); RocksDB (write-amplification and heavy build dep); SpeeDB (small user base).
- Kernel gate: module load fails loudly if `io_uring_setup` is unsupported (< 5.6). No silent fallback.
- Block allocator: bump allocator with a persisted free-list (`AuxState.free_blocks`), coalesced by the compaction thread.
- Abstract trait `StorageBackend` (`src/storage/backend.rs`) defines `get`/`put`/`delete`/`iter`/`flush`. A `MockStorage` impl supports unit tests; the real impl is `FileIoUringBackend` (`src/storage/file_io_uring.rs`).

On-disk record format per value: `[u64 value_len][value bytes][zero padding to 4 KiB boundary]`.

## 5. Cache layer

**S3-FIFO via `quick_cache`** (`quick_cache::sync::Cache`).

- Originally specified as W-TinyLFU; `quick_cache` actually implements S3-FIFO internally. Both are scan-resistant with equivalent hit ratios.
- Interior sharding; no external Mutex needed.
- Cache decisions are local to each node (primary and replica each maintain their own).
- Capacity knob: `flash.cache-size-bytes` — mutable at runtime via `CONFIG SET`. Shrinking evicts eagerly.

Hot/cold integration: `FlashCache::get()` is the fast path for `FLASH.GET` (inline reply, no `BlockClient`). On miss, the command hands off to the async I/O pool, reads from `FileIoUringBackend`, promotes the result back to Hot, and replies via `UnblockClient`.

**Automatic demotion** (`src/demotion.rs`) runs as a three-phase pipeline so the NVMe write never blocks the Valkey event loop:

- **Phase 1 — event loop (tick, every 100 ms).** When `FlashCache::approx_bytes()` reaches 95 % of `flash.cache-size-bytes`, the tick pops up to `MAX_DEMOTIONS_PER_TICK` (default 512) candidates from the insertion-ordered eviction queue (`FlashCache::evict_candidate()`). For each, it opens a writable key handle, clones the hot payload, computes a value hash, and submits a `storage.alloc_and_write_cold` task to the `AsyncThreadPool`. The key handle is dropped before the task runs — the `&mut Tier<T>` borrow does not cross the async boundary.
- **Phase 2 — pool worker.** `alloc_and_write_cold` allocates NVMe blocks and writes the payload. It does not touch the storage index; ownership of the blocks is carried in a `CompletedDemotion` record pushed onto a commit queue.
- **Phase 3 — event loop (next tick).** Before submitting new work, the tick drains the commit queue. For each completion it re-opens the key, race-checks the current Hot payload against the phase-1 value hash, and on a match appends the WAL record, inserts into `TIERING_MAP`, transitions `obj.tier` to `Cold`, evicts the hot cache entry, and reclaims the stale durability-write index entry via `storage.delete`. On mismatch (client wrote during the in-flight window), missing key, or type change, the newly-allocated blocks are released via `release_cold_blocks` and the candidate is dropped.

The 95 % trigger is proactive rather than strict-overflow because `FlashCache::approx_bytes()` delegates to quick_cache's native `weight()`, which S3-FIFO keeps at or below capacity at all times — without the lead-in, S3-FIFO would silently discard hot entries before the demotion path could write them to NVMe.

`MAX_DEMOTIONS_PER_TICK` bounds phase-1 event-loop work (payload clone + pool submit, measured in µs per iteration). `MAX_INFLIGHT` (default 1024) bounds transient NVMe footprint during the in-flight window: each in-flight demotion holds both a Cold block and the stale durability-write block until phase 3 reclaims it.

Replica-mode nodes short-circuit the tick (`IS_REPLICA`) and only begin demoting after a `REPLICAOF NO ONE` promotion. `deinitialize()` flips a shutdown flag before tearing down the pool, so pending timers and in-flight tasks land safely. Monitoring: `INFO flash` exposes `flash_auto_demotions_total` (cumulative successful commits) and `flash_auto_demotions_inflight` (current phase-2 depth for back-pressure observation).

A synchronous `demote_bytes` helper is retained for `FLASH.DEBUG.DEMOTE` — the deterministic admin command preferred in integration tests that need immediate completion.

**Crash between phase 2 and phase 3.** In the ≤ 100 ms window where the NVMe payload exists but the WAL record does not, a crash leaves those blocks allocated but unreferenced. The Hot payload is still in Valkey's keyspace (RDB/AOF recovers it), so no data is lost — but the allocator's free-list cannot reclaim the orphaned blocks until a future compaction pass notices they are unreferenced. Bounded by `MAX_INFLIGHT × block_size`, at most a few MB per crash.

## 6. Async I/O

A fixed-size thread pool (`src/async_io.rs`) services operations that might block on NVMe.

- Default pool size: `num_cpus::get()`; overridable via `flash.io-threads`.
- Task submission via `crossbeam-channel`; workers pull tasks and execute on behalf of a `ValkeyModuleBlockedClient*`.
- Each worker creates a `ThreadSafeContext` for its reply, calls `UnblockClient` on completion, and frees the context.
- `catch_unwind` wraps every task body; a panic in one task does not poison the worker thread or leak the blocked client.
- Command handlers wrap `handle.complete()` in `catch_unwind` too — even the completion dispatch can't hang a client.

Loom tests validate the handoff, the race between concurrent `put`/`evict_candidate`/`delete`, WAL append concurrency, and role-change during in-flight writes.

## 7. Durability: WAL + Valkey persistence

valkey-flash operates two complementary durability layers.

### 7.1 Flash WAL

A module-local append-only log covering NVMe-side mutations.

**Wire format:** `[u32 LE length][u32 LE CRC32C][payload]`

- `payload = [u8 op_ver][u8 op_code][...op body]`
- `WalOp::{Put, Delete, Checkpoint}`; `Put` carries `(key_hash, offset, value_hash)` — not the full value.
- CRC32C via the `crc32c` crate (hardware-accelerated).

**Sync modes** (`flash.sync` config):
- `always` — `sync_data()` before reply. Zero loss on crash.
- `everysec` — background flusher syncs every second. ≤ 1 s loss on crash.
- `no` — rely on OS flush only.

**Torn-tail handling:** records that fail CRC on replay truncate the WAL at that offset; prior records apply. Data-file partial 4 KiB writes are detected during recovery by comparing against the WAL post-image for that block.

### 7.2 Valkey RDB + AOF

**Per-key `rdb_save`/`rdb_load`**, inline. Format: `[u8 encoding_version][u8 shape_tag][i64 ttl_ms (-1=None)][shape-specific payload]`. Loader rejects unknown `encoding_version` (widened-cast check to prevent bypass).

**`aux_save`/`aux_load`** — module-level metadata at `BEFORE_RDB` and `AFTER_RDB`:
- BEFORE_RDB: tiering map (`key_hash → {Hot | Cold, bytes}`), backend config snapshot, WAL cursor
- AFTER_RDB: `{saved_at_unix_ms, rdb_crc}` marker

**`aof_rewrite`** emits `FLASH.SET` / `FLASH.HSET` per key with `PXAT <absolute_ms>` for TTL preservation across restart.

### 7.3 Recovery flow

On module load:
1. Open the backing file + WAL.
2. Read `LOADED_AUX_STATE` from `aux_load` (may be `None` on fresh start).
3. Replay WAL from the cursor in aux.
4. Apply each op idempotently — each record carries `(key_hash, offset, payload_hash)` for idempotent re-application.
5. Truncate on CRC mismatch (torn tail). Fail loud on any other decode error.
6. Mark module `ready`; log recovery stats.

`FLASH.DEBUG.STATE` returns `recovering|ready|error` for test observability.

### 7.4 Durability matrix

| `flash.sync` | Flash WAL guarantee | Combined (WAL + AOF `everysec`) |
|---|---|---|
| `always` | Last ACKed write durable | Same — Flash WAL is the stricter ceiling |
| `everysec` | ≤ 1 s loss | ≤ 1 s loss |
| `no` | OS-dependent | OS-dependent on module state; AOF still durable per AOF's config |

## 8. Replication

**Primary-only tiering by default.** Replicas keep all FLASH keys in RAM; no flash tier on the replica side.

- No wire protocol changes — `ctx.replicate("FLASH.SET", full_args)` sends byte-identical commands to replicas.
- PSYNC / diskless resync compatible — replica materializes every value into RAM as writes arrive.
- Failover (replica → primary): `on_role_changed` promotes the node. If `flash.replica-tier-enabled=false`, the primary initializes its NVMe backend at promotion time. If `flash.replica-tier-enabled=true`, the backend was already open — no initialization step at all.

**Opt-in symmetric tiering** (`flash.replica-tier-enabled`) — immutable config. Each node with it enabled opens its own flash backing file; tier decisions remain local to the node. Required constraint: **each node must have a unique `flash.path`**; there is no file locking.

## 9. Cluster

### 9.1 Cluster-aware init

On load: detect cluster mode via `CTX_FLAGS_CLUSTER`. Config knob `flash.cluster-mode-enabled` accepts `auto` (default), `yes`, `no`. Subscribes to `ValkeyModuleEvent_AtomicSlotMigration` (subevent id 19; hardcoded with a graceful-degrade path for pre-9.0 servers whose module crate header hasn't caught up).

### 9.2 Slot migration

**Per-key atomic.** Not chunked streaming.

- Source probes target via `FLASH.MIGRATE.PROBE` before migration — verifies flash module loaded, `flash.path` configured, and target has sufficient `free_bytes` for the specific key being migrated. Probe result cached for 60 s (`flash.migration-probe-cache-sec`).
- Keys larger than `flash.migration-max-key-bytes` (default 64 MiB) are skipped from the Phase 1 pre-warm and migrated via the NVMe-read path during the standard MIGRATE — no rejection, just a different code path that avoids doubling RAM pressure.
- `DUMP/RESTORE` extension uses the same on-disk format as `rdb_save` (encoding_version + shape_tag + ttl_ms + value bytes).
- **Atomicity:** source keeps ownership until target ACKs the RESTORE. On failure, source retains; nothing is migrated partially.
- **Throttle:** `flash.migration-bandwidth-mbps` (default 100, `0` = unlimited, mutable at runtime). Cumulative expected-vs-actual bytes algorithm with microsecond precision — correct even when individual key migrations take sub-millisecond.
- **Timeout** (`flash.migration-chunk-timeout-sec`, default 30): migration aborted on timeout, source retains.

Error surface (all exact strings in code):
- `ERR FLASH-MIGRATE target <addr> does not have flash-module loaded`
- `ERR FLASH-MIGRATE target <addr> has flash-module but flash.path not configured`
- `ERR FLASH-MIGRATE target <addr> insufficient flash capacity (need N bytes free, has M)` — triggered by the per-key capacity probe comparing the key's serialized size against the target's probe-reported `free_bytes` before MIGRATE.
- `ERR FLASH-MIGRATE timeout after Ns`

### 9.3 Redirect-safe dispatch

Valkey core's MOVED/ASK handling works natively for module commands whose `key_spec` is correctly declared. An audit verified key_specs for all 10 key-bearing FLASH.* commands; cluster-aware clients (valkey-py in cluster mode) transparently follow redirects.

### 9.4 Resharding correctness

Validated under stress: 4 concurrent clients, mixed SET/GET/HSET/HDEL, 1 or 16 simultaneous slot migrations. Zero ACKed data loss, zero stale reads post-migration. Source ownership retained until target ACKs; no torn reads during the migration window.

### 9.5 Failover in cluster

`docker kill` on a primary → replica promotes within cluster-node-timeout (5 s). Promoted node immediately serves FLASH.SET (lazy NVMe init when `replica-tier-enabled=false`; already initialized when `true`). No data loss for ACKed writes; new writes propagate to remaining replica for that shard.

## 10. Operational observability

`INFO flash` section exposes 23 fields. Partial list:

- Cache: `cache_hit_ratio`, `cache_hits`, `cache_misses`, `cache_size_bytes`, `cache_capacity_bytes`
- Storage: `storage_used_bytes`, `storage_free_bytes`, `storage_capacity_bytes`, `wal_size_bytes`
- Compaction: `compaction_runs`, `compaction_bytes_reclaimed`
- Tiering: `tiered_keys`, `eviction_count`
- Cluster: `cluster_mode`, `migration_slots_in_progress`, `migration_bytes_sent`, `migration_bytes_received`, `migration_keys_migrated`, `migration_keys_rejected`, `migration_last_duration_ms`, `migration_errors`, `migration_bandwidth_mbps`
- Drain: `convert_total`, `drain_in_progress`, `drain_last_converted`, `drain_last_skipped`, `drain_last_errors`, `drain_last_scanned`
- State: `module_state` (`recovering|ready|error`)

Keyspace notifications: `flash.set`, `flash.del`, `flash.hset`, `flash.hdel`, `flash.evict`, `flash.convert`. `flash.expire` and `flash.persist` ship with the v1.1.1 `FLASH.EXPIRE` family work.

ACL: `@flash` category scopes every FLASH.* command; admin/debug commands are `@admin @dangerous @flash`.

## 11. Unloading the module

`MODULE UNLOAD flash` is refused by Valkey while any custom-type key exists, so `FLASH.DRAIN` must run first to convert the tier back to native types.

### 11.1 `FLASH.CONVERT key`

Per-key primitive. Replies `:1` when a `FLASH.*` key is converted to its native counterpart, `:0` when the key is missing, already native, or belongs to a different module (idempotent). The execution path:

1. Open the key; inspect `key_type()`. Reject early if not a `FLASH.*` module type.
2. Materialise the payload into owned Rust values. Cold-tier payloads are synchronously read from NVMe via `read_at_offset` (same pattern as `FLASH.HDEL` / `promote_cold_list` / `promote_cold_zset`).
3. Capture TTL via `util_expire::preserve_ttl` so expiries set via native `EXPIRE` are preserved alongside `obj.ttl_ms`.
4. Drop the key handle, then issue sub-calls via `ctx.call_ext` with `CallOptions::replicate()` set:
   - `DEL key` — removes the flash key (triggers our `free` callback, which emits the WAL `Delete` and releases NVMe blocks for Cold entries).
   - `SET` / `HSET` / `RPUSH` / `ZADD` — reconstructs the native key.
   - `PEXPIREAT key ttl_ms` — restores the absolute TTL.
5. Emit a `flash.convert` `GENERIC` keyspace event on the key.

The `FLASH.CONVERT` command itself is **not** replicated. Only its sub-calls propagate via the `!` flag, so the AOF and replication stream contain only native commands. AOF replay after a subsequent `MODULE UNLOAD flash` succeeds.

### 11.2 `FLASH.DRAIN [MATCH pattern] [COUNT n] [FORCE]`

Scan-driven wrapper. Reply is an array `[converted, skipped, errors, scanned]`.

- **Pass 1** — `KeysCursor::new()` full scan, filtered in-process by `glob_match(pattern)`. Candidate names are collected into a `Vec<Vec<u8>>`.
- **Pass 2** — iterate candidates, call `extract_payload` + `apply_conversion`. Keys that vanish between passes or changed to a native type count toward `skipped`.
- **Headroom guard** — before pass 1, read `used_memory` and `maxmemory` via `ctx.server_info("memory")`. If `used_memory + storage_used > maxmemory` (and `maxmemory > 0`), refuse unless `FORCE`. The projection is worst-case (all Cold bytes become Hot).
- **COUNT** — caps pass-2 conversions per invocation. Operators loop externally for chunked drains.
- **`DRAIN_IN_PROGRESS` atomic** — surfaced as `flash_drain_in_progress`. The command runs synchronously on the event-loop thread; the flag exists primarily for `INFO flash` observability.

### 11.3 Failure semantics

`FLASH.CONVERT` is fail-forward, not transactional. After the internal `DEL` succeeds, the flash copy is gone; if the native create fails (e.g. OOM), the key is lost. Mitigations:

- `deny-oom` on the command → Valkey rejects the command up front when `maxmemory` is already exceeded.
- The materialised payload is constructed fully in pass 2 before the `DEL`, so deserialisation errors land pre-delete.
- Cold-tier NVMe read errors surface as `ValkeyError` pre-delete.

Operators should watch `flash_drain_last_errors` after a `FORCE` drain on a tightly-sized node.

## 12. Known limitations

### Missing commands (planned for v1.1.1)

The following commands are absent from the current module surface and are scheduled for the v1.1.1 release. Wrappers handle the gaps with workarounds in the meantime:

- **`FLASH.EXISTS`** — multi-key existence check. Wrappers currently fall back to per-key `FLASH.GET` loops which read full values just to check presence. Spec: backlog task `7873d9ed`.
- **`FLASH.EXPIRE` family** — `EXPIRE`, `EXPIREAT`, `PEXPIRE`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`, `EXPIRETIME`, `PEXPIRETIME`. Today TTL is settable only at write-time via `FLASH.SET` options; cannot bump TTL on existing keys. **Blocks the session-cache "extend on activity" pattern.** Spec: backlog task `57e13805`.
- **`FLASH.MGET` / `FLASH.MSET`** — batch read/write commands. Wrappers do per-key dispatch (N round-trips). Spec: backlog task `f5d207d2`.
- **`FLASH.COPY` / `FLASH.RENAME` / `FLASH.RENAMENX`** — atomic same-tier moves and atomic-via-WAL cross-tier moves. Wrappers currently do non-atomic GET+SET+DEL fallback for cross-tier moves (Bucket C in the wrapper backlogs). Spec: backlog task `82b87a71`.
- **`FLASH.INCR` / `FLASH.INCRBY` / `FLASH.DECR` / `FLASH.DECRBY` / `FLASH.APPEND`** — read-modify-write on flash-tier strings. Wrappers (Bucket B) return a typed error today. Spec: backlog task `db201438`.

### Long-standing tradeoffs

- **Cold-tier keyed-lookup during RDB save** for very large cold values: inline inclusion in RDB inflates file size (documented tradeoff; a future revision can use `aux_save`-based shared backing).
- **Migration key-size cap**: keys > `flash.migration-max-key-bytes` (default 64 MiB) must be drained manually before reshard; chunked streaming is planned.
- **FLASH.HSET TTL on cold hash materialization**: `obj.ttl_ms` round-trips correctly through RDB/AOF, but `rdb_save` on a Cold hash reconstructs from NVMe blocks — `backend_offset` must be set. Tested.
- **AOF under instrumented debug builds**: integration-coverage disables AOF tests because debug-speed instrumentation slows server restart enough to race NVMe reinit. Production (release) builds are unaffected.
- **Block allocator fragmentation over months**: current allocator is a bump pointer with persisted free-list, coalesced by the compaction thread. Sufficient for short-lived workloads; for production deployments with months of mixed SET/DEL on variable-sized values, copy-on-compact segment cleaning is needed. Spec: backlog task `8aaeac00` (v1.2+ material).
