# valkey-flash Architecture

This document captures the architectural decisions behind valkey-flash.

## 1. Overview

valkey-flash is a Valkey module that tiers key/value data to NVMe storage, letting a Valkey instance serve a working set larger than RAM by transparently demoting cold data to flash. Hot entries live in an in-memory cache; cold entries reside on an NVMe-backed file. Valkey proper remains unaware of the flash tier — the module exposes `FLASH.*` commands that users opt into per-key.

**At a glance:**
- **Commands**: `FLASH.SET`/`FLASH.GET`/`FLASH.DEL` for tiered strings; `FLASH.HSET`/`HGET`/`HGETALL`/`HDEL`/`HEXISTS`/`HLEN` for tiered hashes.
- **Storage**: file-backed `io_uring` NVMe backend; S3-FIFO cache layer.
- **Durability**: append-only WAL with three sync modes (`always` / `everysec` / `no`) plus standard Valkey RDB + AOF.
- **Cluster**: native `MIGRATE` support for slot migration, per-key atomic, probe-gated by module availability.

## 2. Component map

```
            ┌────────────────────── FLASH.* command handlers ──────────────────────┐
            │  SET  GET  DEL  HSET  HGET  HGETALL  HDEL  HEXISTS  HLEN             │
            └───┬──────────┬──────────┬─────────────────┬───────────────┬──────────┘
                │          │          │                 │               │
                ▼          ▼          ▼                 ▼               ▼
            ┌─────────────────────┐          ┌─────────────────────┐
            │  FlashString type   │          │  FlashHash type     │
            │  (rdb/aof/defrag/   │          │  (rdb/aof/defrag/   │
            │   copy/mem_usage2)  │          │   copy/mem_usage2)  │
            └──────────┬──────────┘          └──────────┬──────────┘
                       │                                 │
                       └──────────────┬──────────────────┘
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
                  │  WAL          │          │  backing     │
                  │  (CRC32C,     │          │  file (NVMe) │
                  │   3 sync modes)│         │              │
                  └──────────────┘          └──────────────┘
```

Background threads: a compaction worker reclaims freed blocks; the WAL flusher thread handles `everysec` fsync; the async I/O thread pool serves cache-miss reads and cold writes via `BlockClient` / `ThreadSafeContext`.

## 3. Data types

**Per-shape types, not a unified type.** Each shape registers its own `ValkeyType` with its own module-type-id, encoding version, and callback set:

- `FlashString` — `src/types/string.rs` — holds `Tier::Hot(Vec<u8>)` or `Tier::Cold { key_hash, backend_offset, num_blocks, value_len }`
- `FlashHash` — `src/types/hash.rs` — same Tier shape, payload is serialized `HashMap<Vec<u8>, Vec<u8>>`

Rationale: clean command dispatch, separate RDB layouts (simpler forward-compat when a shape gains features), per-type defrag cursor. The Tier enum is the single source of truth for whether a value is in RAM or NVMe; every callback dispatches on it.

`FlashList` and `FlashZSet` are deferred to a future release.

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

Keyspace notifications: `flash.set`, `flash.del`, `flash.hset`, `flash.hdel`, `flash.evict`, `flash.convert`. `flash.expire` deferred (no module API hook for Valkey's native TTL `free` path).

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

- **Cold-tier keyed-lookup during RDB save** for very large cold values: inline inclusion in RDB inflates file size (documented tradeoff; a future revision can use `aux_save`-based shared backing).
- **Migration key-size cap**: keys > `flash.migration-max-key-bytes` (default 64 MiB) must be drained manually before reshard; chunked streaming is planned.
- **FLASH.HSET TTL on cold hash materialization**: `obj.ttl_ms` round-trips correctly through RDB/AOF, but `rdb_save` on a Cold hash reconstructs from NVMe blocks — `backend_offset` must be set. Tested.
- **AOF under instrumented debug builds**: integration-coverage disables AOF tests because debug-speed instrumentation slows server restart enough to race NVMe reinit. Production (release) builds are unaffected.
