# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

#### Types and commands

- `FLASH.STRING` type with `FLASH.SET` (EX/PX/EXAT/PXAT/NX/XX/KEEPTTL), `FLASH.GET`, `FLASH.DEL`.
- `FLASH.HASH` type with `FLASH.HSET` (EX/PX/EXAT/PXAT/KEEPTTL), `FLASH.HGET`, `FLASH.HGETALL`, `FLASH.HDEL`, `FLASH.HEXISTS`, `FLASH.HLEN`.
- `FLASH.LIST` type with `FLASH.LPUSH`, `FLASH.RPUSH`, `FLASH.LPUSHX`, `FLASH.RPUSHX`, `FLASH.LPOP`, `FLASH.RPOP`, `FLASH.LRANGE`, `FLASH.LLEN`, `FLASH.LINDEX`, `FLASH.LSET`, `FLASH.LINSERT`, `FLASH.LREM`, `FLASH.LTRIM`, `FLASH.LMOVE`, `FLASH.RPOPLPUSH`, and blocking variants `FLASH.BLPOP`, `FLASH.BRPOP`, `FLASH.BLMOVE`.
- `FLASH.ZSET` type with `FLASH.ZADD` (NX/XX/GT/LT/CH/INCR), `FLASH.ZREM`, `FLASH.ZINCRBY`, `FLASH.ZPOPMIN`, `FLASH.ZPOPMAX`, `FLASH.ZSCORE`, `FLASH.ZRANK`/`FLASH.ZREVRANK` (+WITHSCORE), `FLASH.ZCARD`, `FLASH.ZCOUNT`, `FLASH.ZLEXCOUNT`, `FLASH.ZRANGE` (unified BYSCORE/BYLEX/REV/WITHSCORES/LIMIT), `FLASH.ZRANGEBYSCORE`, `FLASH.ZREVRANGEBYSCORE`, `FLASH.ZRANGEBYLEX`, `FLASH.ZREVRANGEBYLEX`, `FLASH.ZSCAN`, store ops `FLASH.ZUNIONSTORE`/`FLASH.ZINTERSTORE`/`FLASH.ZDIFFSTORE`/`FLASH.ZRANGESTORE` (WEIGHTS + AGGREGATE SUM|MIN|MAX), and blocking `FLASH.BZPOPMIN`/`FLASH.BZPOPMAX`.
- COPY / OBJECT COPY support for all four types (Cold sources materialize via NVMe read; TTL preserved).
- Active-defrag callbacks for all four types.

#### Storage, persistence, durability

- Two-tier storage: in-memory cache + NVMe backing file; async I/O via `io_uring` on background threads.
- WAL with CRC32C record framing, three sync modes: `always`, `everysec`, `no`. Corrupt or truncated records are detected and rejected on recovery.
- RDB save/load and AOF rewrite for all four types; Cold-tier entries are materialized from NVMe before save.
- `aux_save`/`aux_load` for cross-restart tiering metadata consistency.
- Background NVMe compaction with free-list block reclaim; TTL-expiry and overwrite reclaim.

#### Cluster

- Cluster-aware module init with `flash.cluster-mode-enabled` (`auto`/`yes`/`no`).
- `FLASH.MIGRATE.PROBE [host port]` — query local or remote node state, capacity, path.
- `FLASH.MIGRATE` — capacity-gated MIGRATE wrapper; returns `ERR FLASH-MIGRATE target ... insufficient flash capacity` before forwarding when the target lacks space, or `ERR FLASH-MIGRATE target ... does not have flash-module loaded` when the target is missing the module.
- `AtomicSlotMigration` event handler: pre-warms Cold Flash keys in migrating slots to Hot with bandwidth throttling.
- Opt-in symmetric tiering on replicas via `flash.replica-tier-enabled`; promotion to primary is instant when the tier is already initialized.
- MOVED/ASK redirect handling for all FLASH.* commands.

#### Operations

- `@flash` ACL category; FLASH.* commands scoped to `@read`/`@write`/`@fast`/`@slow`/`@admin @dangerous` as appropriate.
- Keyspace notifications for every mutation: `flash.set`, `flash.del`, `flash.hset`, `flash.hdel`, `flash.evict`, `flash.lpush`, `flash.rpush`, `flash.lpop`, `flash.rpop`, `flash.lset`, `flash.linsert`, `flash.lrem`, `flash.ltrim`, `flash.lmove`, `flash.zadd`, `flash.zrem`, `flash.zincrby`, `flash.zpopmin`, `flash.zpopmax`, `flash.zunionstore`, `flash.zinterstore`, `flash.zdiffstore`, `flash.zrangestore`.
- `INFO flash` section with cache / storage / WAL / migration stats (including 8 `flash_migration_*` counters and `flash_cluster_mode`).
- Replication: primary-only tiering by default; replicas serve Hot data from RAM. Promoted replica initializes the NVMe backend on `REPLICAOF NO ONE` without a restart.

#### Configuration

Module load args (`--loadmodule libvalkey_flash.so flash.<knob> <value>`):

- `flash.path`, `flash.capacity-bytes`, `flash.cache-size-bytes`, `flash.sync`, `flash.io-threads`, `flash.io-uring-entries`, `flash.compaction-interval-sec`, `flash.cluster-mode-enabled`, `flash.replica-tier-enabled`, `flash.migration-max-key-bytes` (64 MiB), `flash.migration-bandwidth-mbps` (`0` = unlimited), `flash.migration-chunk-timeout-sec`, `flash.migration-probe-cache-sec`.
- Runtime `CONFIG SET` is accepted for `flash.cache-size-bytes` (live resize), `flash.sync`, `flash.compaction-interval-sec`, and `flash.migration-bandwidth-mbps`; other knobs are immutable and require a restart.

#### Debug commands

- `FLASH.DEBUG.STATE`, `FLASH.DEBUG.DEMOTE`, `FLASH.COMPACTION.TRIGGER`, `FLASH.COMPACTION.STATS`.

#### Packaging

- Docker image based on `valkey/valkey:9.0.3-trixie` with `FLASH_*` environment variables; single-node and 3×3 cluster Compose stacks under `docker/`; `docker/seccomp-flash.json` extends Docker's default profile with the three `io_uring` syscalls.

### Changed

- `flash.migration-bandwidth-mbps = 0` now means "unlimited" (previously rejected; minimum was 1).
- Dependabot now tracks Python test-suite deps via `package-ecosystem: uv` on `pyproject.toml` + `uv.lock` (alongside the existing Cargo, GitHub Actions, and Docker entries).

### Fixed

- `aof_rewrite` for Cold-tier `FlashList` and `FlashZSet` keys previously returned early instead of materializing from NVMe, causing silent data loss on AOF-only restart. Both now read from NVMe and emit the full value.
- `FLASH.ZINCRBY` and `FLASH.ZADD INCR` now return `ERR resulting score is not a number (NaN)` when arithmetic produces NaN (e.g. `+inf + -inf`) instead of inserting a NaN score (matches Redis core).
- `FLASH.ZRANGESTORE` with `LIMIT` and neither `BYSCORE` nor `BYLEX` now returns `ERR syntax error` instead of silently storing the full range.
- `FlashZSet` RDB load is now capped in aggregate (512 MiB total) as well as per-member (512 MiB) and per-count (1M), preventing a `count × per-member` loophole that could allow ~1 GiB of untrusted input.
- `FLASH.ZADD` no-ops (NX on existing, XX on missing) no longer emit a spurious `flash.zadd` keyspace event.
- `FLASH.LMOVE` now signals the destination key as ready after pushing, so clients blocked on `FLASH.BLPOP` on that key wake immediately instead of waiting for their timeout.
- Migration bandwidth throttle corrected: the limit was computed as MiB/s instead of Mbps (≈8× too permissive) and sub-second windows were unthrottled; both fixed.
- `AtomicSlotMigration` struct layout (`version`, `num_slot_ranges`) corrected to match the C header, preventing a crash when reading `slot_ranges` during migration.
- `FLASH.MIGRATE.PROBE` no longer inserts into the cache when caching is disabled (`flash.migration-probe-cache-sec = 0`).

### Security

- RDB payload parsers are pure Rust with explicit size caps (512 MiB strings, 256 MiB hashes) and per-allocation remaining-bytes guards; `rdb_load` and the fuzz harness share the same validation path.
- WAL record CRC32C framing detects and rejects corrupt or truncated records on recovery.
- RDB version-byte guard widened to prevent integer overflow on untrusted input.
