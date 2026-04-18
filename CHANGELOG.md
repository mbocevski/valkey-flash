# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Active-defrag support for FLASH data types: `FlashString` and `FlashHash` defrag callbacks relocate the object struct and, for Hot-tier entries, each `Vec<u8>` backing buffer; Cold-tier entries (scalar metadata only) are a no-op; integration tests in `tests/test_flash_defrag.py` verify data integrity and server stability under activedefrag
- loom concurrency tests for async thread-pool, cache, WAL, and role-change paths: 7 `loom_tests::*` tests covering `RwLock<Arc<>>` resize races, candidates-queue Mutex, `approx_bytes` AtomicU64, WAL Mutex serialization, `sync_mode` AtomicU8 visibility, worker-claims-task-exactly-once, and `IS_REPLICA` Acquire/Release ordering; wired into CI via `.github/workflows/loom.yml` (`RUSTFLAGS='--cfg loom' cargo test -- loom_tests::`)
- Runtime CONFIG SET for mutable flash.* knobs: `flash.cache-size-bytes` (live resize with entry migration), `flash.sync` (WAL flusher observes new mode on next tick), `flash.compaction-interval-sec` (compaction thread woken immediately)
- `@flash` ACL category registered; FLASH.* commands scoped to `@read`, `@write`, or `@admin @dangerous` as appropriate
- Keyspace notifications for FLASH mutations: `flash.set`, `flash.del`, `flash.hset`, `flash.hdel`, `flash.evict` events via `notify-keyspace-events`
- FLASH.SET command — async NVMe write-through with replication support
- FLASH.GET command — hot-path cache hit and cold-path async NVMe read with hot promotion
- FLASH.DEL command — variadic async tombstone with replication
- FLASH.HASH type with HSET, HGET, HGETALL, HDEL, HEXISTS, and HLEN commands
- FLASH.DEBUG.DEMOTE command for manual hot→cold demotion (test/debug use)
- File-backed io_uring NVMe storage backend
- W-TinyLFU in-memory cache layer (via quick_cache)
- WAL with CRC32C record framing and three sync modes: `always`, `everysec`, `no`
- Crash recovery integrating WAL replay, aux metadata, and storage backend
- RDB save/load for FlashString and FlashHash types
- AOF rewrite for FlashString (with TTL preservation) and FlashHash
- `aux_save`/`aux_load` with tiering map and WAL cursor for cross-restart consistency
- Background compaction with free-list for NVMe block reclaim; FLASH.COMPACTION.TRIGGER and FLASH.COMPACTION.STATS commands
- TTL-expiry and overwrite NVMe block reclaim
- Replication role-change hook — primary-only tiering; replicas serve hot data from RAM
- INFO section with cache, storage, and WAL stats (14 `flash_*` fields; query with `INFO flash`)
- Runtime module configuration: `flash.path`, `flash.capacity-bytes`, `flash.cache-size-bytes`, `flash.sync`, `flash.io-threads`, `flash.io-uring-entries`, `flash.compaction-interval-sec`
- Async I/O thread pool with BlockClient for non-blocking NVMe reads and writes
- Docker image based on `valkey/valkey:9.0.3-trixie` for running the module in containers (CI and local dev); configurable via `FLASH_PATH`, `FLASH_CAPACITY_BYTES`, `FLASH_CACHE_SIZE_BYTES`, `FLASH_SYNC`, `FLASH_IO_THREADS`, `FLASH_IO_URING_ENTRIES`, `FLASH_COMPACTION_INTERVAL_SEC` environment variables
- Single-node Docker Compose (`docker/compose.single.yml`) with named volume persistence, healthcheck, and io_uring seccomp configuration; dev override (`compose.single.dev.yml`) for reduced capacity and fast sync
- Three-primary, three-replica cluster Docker Compose (`docker/compose.cluster.yml`) with automatic slot assignment via `cluster-init` oneshot service
- pytest fixtures for single-node and cluster Docker topologies (`docker_single`, `docker_cluster`); activated via `USE_DOCKER=1`
- CI job tier for Docker-based integration tests (`docker-integration`): matrix over topology (single, cluster), triggered on push to `main` and on PRs with the `slow-tests` label; Docker layer cache keyed on `Dockerfile + Cargo.lock` hash; compose logs uploaded as artifact on failure

### Changed

- Test matrix bumped to (unstable, 8.1, 9.0); Valkey 8.0 support removed

### Fixed

- Promoted replica now initializes NVMe backend on `REPLICAOF NO ONE`, enabling FLASH.SET writes without a server restart

### Security

- WAL record CRC32C framing — corrupt or truncated records are detected and rejected on recovery
- Field-count cap in hash deserializer — prevents OOM allocation on malformed RDB input
- RDB version guard widened cast — prevents integer overflow on untrusted version bytes
