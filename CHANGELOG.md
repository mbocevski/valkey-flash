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
- Minimum Python for the integration test suite bumped from 3.11 to 3.12. Python 3.11's `tempfile.mkdtemp(dir=<relative>)` returns a relative path (bpo-44836, fixed in 3.12); our fixtures passed the result to a Popen'd `valkey-server` that was started with `cwd=test-data`, so the double-relative path made the module's WAL open fail with ENOENT on first-test setup. `pyproject.toml` requires-python and ruff target-version both raised; CI runners pinned to 3.12.
- `Cargo.lock` and `fuzz/Cargo.lock` are now tracked in git (removed from `.gitignore`) for reproducible builds and so the Dockerfile `COPY Cargo.lock` step resolves.
- Test harness self-bootstraps: `tests/conftest.py` auto-detects `MODULE_PATH` (→ `target/release/libvalkey_flash.so`) and prepends `tests/build/binaries/$SERVER_VERSION/` to `LD_LIBRARY_PATH` when unset. `SERVER_VERSION=9.0 uv run pytest tests/` now works without going through `build.sh` first. CI unchanged (env still set explicitly).
- macOS CI job removed: `io-uring` is Linux-only and can't compile on macOS; the job was architecturally unsound.

### Fixed

- Module would not load into any Valkey server: `RedisModule_CreateCommand` rejected the `no-multi` and `slow` command flags (used for native commands but not in the module-API whitelist), failing the entire module init. Removed both flags from `FLASH.BLPOP`, `BRPOP`, `BLMOVE`, `BZPOPMIN`, `BZPOPMAX`, `ZUNIONSTORE`, `ZINTERSTORE`, `ZDIFFSTORE`, `ZRANGESTORE`. The equivalent `@slow` ACL category remains on these commands' ACL-category string, which is where it belongs.
- `aof_rewrite` for Cold-tier `FlashList` and `FlashZSet` keys previously returned early instead of materializing from NVMe, causing silent data loss on AOF-only restart. Both now read from NVMe and emit the full value.
- `FLASH.ZINCRBY` and `FLASH.ZADD INCR` now return `ERR resulting score is not a number (NaN)` when arithmetic produces NaN (e.g. `+inf + -inf`) instead of inserting a NaN score (matches Redis core).
- `FLASH.ZRANGESTORE` with `LIMIT` and neither `BYSCORE` nor `BYLEX` now returns `ERR syntax error` instead of silently storing the full range.
- `FlashZSet` RDB load is now capped in aggregate (512 MiB total) as well as per-member (512 MiB) and per-count (1M), preventing a `count × per-member` loophole that could allow ~1 GiB of untrusted input.
- `FLASH.ZADD` no-ops (NX on existing, XX on missing) no longer emit a spurious `flash.zadd` keyspace event.
- `FLASH.LMOVE` now signals the destination key as ready after pushing, so clients blocked on `FLASH.BLPOP` on that key wake immediately instead of waiting for their timeout.
- Migration bandwidth throttle corrected: the limit was computed as MiB/s instead of Mbps (≈8× too permissive) and sub-second windows were unthrottled; both fixed.
- `AtomicSlotMigration` struct layout (`version`, `num_slot_ranges`) corrected to match the C header, preventing a crash when reading `slot_ranges` during migration.
- `FLASH.MIGRATE.PROBE` no longer inserts into the cache when caching is disabled (`flash.migration-probe-cache-sec = 0`).
- `FLASH.HGET`, `FLASH.HGETALL`, `FLASH.HEXISTS`, `FLASH.HLEN`, `FLASH.LINDEX`, `FLASH.LRANGE`, and `FLASH.LLEN` now honour key-level TTL expiry: previously the in-memory cache short-circuited the type/existence check, so expired keys returned stale values until the next Valkey eviction pass. The type check (`open_key`) now runs first so Valkey's expiry hook fires, and stale cache entries for expired keys are dropped.
- `FLASH.ZADD`, `FLASH.ZREM`, `FLASH.ZINCRBY`, `FLASH.ZPOPMIN`/`ZPOPMAX`, `FLASH.BZPOPMIN`/`BZPOPMAX`, `FLASH.HSET`, `FLASH.HDEL`, `FLASH.LPUSH`/`RPUSH`/`LPUSHX`/`RPUSHX`, `FLASH.LPOP`/`RPOP`, `FLASH.LSET`, `FLASH.LTRIM`, `FLASH.LREM`, `FLASH.LINSERT`, `FLASH.LMOVE`/`BLMOVE`, `FLASH.BLPOP`/`BRPOP` no longer wipe a native `PEXPIRE`/`EXPIRE` TTL when updating an existing key. `VM_ModuleTypeSetValue` internally calls `VM_DeleteKey`, which clears the key's expiry unconditionally (no `SETKEY_KEEPTTL`-equivalent flag is exposed to modules). Writers now read the remaining TTL via `VM_GetExpire` before the write and re-apply it via `set_expire` after. `FLASH.HDEL` additionally now re-applies the TTL after removing a subset of fields (previously it dropped it).
- Module load crash on AOF/RDB replay of FLASH.* writes: write-path commands submit NVMe I/O via `ctx.block_client()`, but client contexts replayed from disk carry `DENY_BLOCKING=1`, which hits `serverAssert(!deny_blocking || (islua || ismulti))` in Valkey core. Writers now also gate on `LOADING`/`DENY_BLOCKING`/`ASYNC_LOADING` via a new `must_run_sync(ctx)` check and complete synchronously on the event loop during replay.
- `COPY ... REPLACE` on a FLASH.* destination key now overwrites the stored value. Previously the module's in-memory hot cache was not invalidated during the REPLACE, so subsequent `FLASH.GET`/`FLASH.HGET` served the pre-REPLACE cached bytes even though Valkey's keyspace object had been overwritten.
- NVMe free-list and `next_block` allocator cursor are now actually restored on restart. The `initialize()` → restore path read `LOADED_AUX_STATE` before `aux_load` had populated it (aux_load runs later during RDB replay, after module init); the call was a no-op in practice and freed NVMe space was leaked across every restart. `aux_load` now applies the snapshot via `storage.restore_state()` directly, which runs after STORAGE has been opened.
- `FLASH.MIGRATE.PROBE` against a remote flash-loaded target returned `ERR FLASH-MIGRATE target ... does not have flash-module loaded` even when the remote had the module. The RESP2 framing written to the target socket declared the command name as 18 bytes (`$18`) instead of 19, so the target parsed `FLASH.MIGRATE.PROB` and rejected it as unknown. Fixed framing to `$19`; remote probes now succeed.
- `FLASH.MIGRATE.PROBE` now accepts hostnames, not just literal IP:port addresses. The old `SocketAddr::parse` path rejected `host.docker.internal` or any DNS name with `ERR FLASH-MIGRATE target ... invalid address`. Connections now go through `ToSocketAddrs`, which resolves hostnames before dialing.
- `FLASH.DEL` now works on every FLASH.* type (string, hash, list, zset) instead of only flash-strings. Previously a hash / list / zset key rejected FLASH.DEL with WRONGTYPE, even though the module owned the key; callers had to dispatch to a type-specific command to delete. Behaviour now matches Valkey's own `DEL`, which is type-agnostic.
- Replication of FLASH.LIST and FLASH.ZSET writes (`LPUSH`/`RPUSH`/`LPOP`/`LSET`/`ZADD`/`ZREM`/`ZINCRBY`, …) now has integration coverage and passes end-to-end. The underlying write path was correct — the tests that previously `xfail`'d were spawning a second server via `self.create_server(replicaof=...)` instead of using the framework's `ReplicationTestCase`, which meant the replica got no logfile, no port tracker, and no sync-wait helper, so `FLASH.LRANGE` on the replica was consistently racing the primary's command. The test harness is now correct and all 6 cases pass.
- Server restart crash after a `BGSAVE` with any `FLASH.ZSET` key present: `zset::rdb_load` called `VM_SignalKeyAsReady` with the IO-time context returned by `VM_GetContextFromIO`, which has no attached client during RDB load. Valkey core then dereferenced `ctx->client->db` — SIGSEGV at `VM_SignalKeyAsReady+0x2b`, server aborted before accepting connections. The signal was only useful for waking `BZPOPMIN`/`BZPOPMAX` clients across a `DEBUG RELOAD` (an edge case those clients handle via their own timer anyway); removed from the load path. Real write paths (`FLASH.ZADD`, `FLASH.ZPOPMIN`, `FLASH.ZUNIONSTORE`, etc.) still signal correctly from their command handlers.
- Spurious WAL Delete records during AOF / RDB replay: the four tiered types' `free` callbacks appended `WalOp::Delete` on every Cold-tier Drop, including the intermediate frees Valkey performs while replaying (e.g. AOF `SET k v1` then `SET k v2` → v1's FlashType is freed as the keyspace slot is overwritten). Those Deletes corrupted the WAL for the next restart — recovery would replay them on top of the just-loaded keys. Gated on a new `IS_LOADING` atomic driven by the Loading server-event, so the WAL only records genuine mutations.
- `MODULE UNLOAD flash` SIGSEGV'd the Valkey server once any worker thread next woke. The `AsyncThreadPool`'s workers lived in a `static OnceLock` that Rust does not `Drop` on `dlclose`, so the `.so`'s text segment got unmapped while worker threads were still alive; the next `recv` loop iteration executed unmapped memory. `deinitialize()` now shuts the pool down (drops the sender to close the channel, then joins every worker) before returning, mirroring the compaction-thread shutdown.
- NVMe free-list coalescing inside `run_compaction_tick` previously ran under `free_list.lock()`, blocking every concurrent `alloc_blocks` / `release_cold_blocks` on the main thread for the O(N log N) sort. The vec is now swapped out under the lock (a pointer move), sorted on the compaction worker, and merged back with any ranges released during the unlock window.
- AOF rewrite silently dropped Cold-tier keys, breaking AOF's durability guarantee. `FlashString` and `FlashHash` `aof_rewrite` callbacks previously short-circuited on `Tier::Cold` with a "cannot fetch from NVMe without key" warning — every demoted key was missing from the rewritten AOF. On restart from that AOF the cold keys were lost. They now materialize from NVMe (same pattern `rdb_save` Cold already uses) and emit the full `FLASH.SET` / `FLASH.HSET` commands. `FlashList` and `FlashZSet` already materialized Cold in `aof_rewrite`; those paths are also now fork-safe (see next bullet).
- BGSAVE / BGREWRITEAOF Cold-tier reads now go through a new fork-safe `pread_at_offset` path instead of the io_uring-backed `read_at_offset`. io_uring rings are not safe to share across `fork()` — the forked child would otherwise corrupt the parent's submission queue or inherit a poisoned mutex. `pread_at_offset` touches only the inherited file descriptor and uses `posix_fadvise(POSIX_FADV_WILLNEED)` to warm the page cache. Now applied to both `aof_rewrite` and `rdb_save` Cold branches on all four types.

### Security

- RDB payload parsers are pure Rust with explicit size caps (512 MiB strings, 256 MiB hashes) and per-allocation remaining-bytes guards; `rdb_load` and the fuzz harness share the same validation path.
- WAL record CRC32C framing detects and rejects corrupt or truncated records on recovery.
- RDB version-byte guard widened to prevent integer overflow on untrusted input.
