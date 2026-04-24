# valkey-flash

Valkey module that tiers key/value data to NVMe storage, letting Valkey use flash as an extension of RAM.

Repository: https://github.com/mbocevski/valkey-flash

## Status

v1.0.0 shipped 2026-04-20 with strings, hashes, lists, sorted sets, full cluster support, WAL-durable writes, RDB + AOF, keyspace notifications, ACL categories. v1.1.0 shipped 2026-04-24 adding automatic hot → cold tier demotion via a three-phase async pipeline (`src/demotion.rs`): phase 1 event-loop submit, phase 2 NVMe write on `AsyncThreadPool`, phase 3 event-loop commit with race check. Trigger at ≥ 90 % of `flash.cache-size-bytes` or on any S3-FIFO eviction since the last tick. Corrected `flash_storage_{used,free}_bytes` formula; added `flash_auto_demotions_total` + `flash_auto_demotions_inflight`; swapped manual cache counters for `quick_cache` native `weight()` + `Lifecycle::on_evict`. v1.1.0 also introduced `flash.demotion-batch` + `flash.demotion-max-inflight` runtime configs (auto-sized from `flash.io-threads`), AIMD adaptive clamp on the effective batch cap (stall-budget-driven halve-on-overshoot / grow-on-idle), and aux-save-before-drain to prevent crash-window block leaks on `BGSAVE`. PR #17 on branch `feat/v1.1-test-bench-quality` lands three v1.1 follow-ups: FlashHash defrag bucket-table rebuild for small hashes, per-test docker-cluster health gate, and demotion tick instrumentation (`flash_demotion_tick_last_us`, `flash_demotion_stall_events_total` + expanded bench matrix in `tests/bench/demotion.py`). Deferred follow-up: pipelined cold-tier prefetch during RDB save.

## Architecture (committed decisions)

- **Language:** Rust (edition 2024), using the [`valkey-module`](https://crates.io/crates/valkey-module) crate — same stack as `valkey-bloom`.
- **Module namespace:** commands are prefixed `FLASH.*` (e.g. `FLASH.SET`, `FLASH.GET`, `FLASH.LPUSH`, `FLASH.ZADD`). Module short name `flash`.
- **Data model:** custom tiered data types (`FlashString`, `FlashHash`, `FlashList`, `FlashZSet`). The Valkey module API does not support transparent interception of built-in STRING/HASH/ZSET — `ValkeyModule_CreateDataType` only registers *new* types (`valkey/src/valkeymodule.h:1776`). Users opt in per key.
- **Tiering:** two-tier (RAM + NVMe). Hot entries in an in-memory cache (S3-FIFO via `quick_cache`); cold entries on NVMe addressed by `(backend_offset, value_len)`. Valkey proper is unaware of the flash tier.
- **I/O model:** NVMe reads/writes run on background threads via `BlockClient` + `GetThreadSafeContext`; `io_uring` submission queues per worker. The event loop never blocks on disk.
- **Persistence:** per-key `rdb_save` / `rdb_load` / `aof_rewrite`, plus `aux_save` / `aux_load` for cross-key tiering metadata. WAL framing is `[u32 LE length][u32 LE CRC32C][payload]`.
- **Cluster:** `FLASH.MIGRATE` hook bundles DUMP/RESTORE with tier state, `FLASH.MIGRATE.PROBE` capacity-gates migrations, per-key atomicity via `AtomicSlotMigration` events.

## Build & test

```fish
./build.sh              # fmt, clippy, unit + integration tests
cargo build --release   # .so only
cargo test --features enable-system-alloc  # unit tests
uv run pytest tests/ -q # integration tests (requires valkey-server + valkey-test-framework)
make coverage-integration  # combined unit + integration coverage → lcov.info
```

Integration tests spin up a matching `valkey-server` — `build.sh` clones and compiles it into `tests/build/` keyed on `SERVER_VERSION` (unstable, 8.1, 9.0).

Python tooling: `uv` for dependency management (`uv sync --frozen` in CI), `ruff` for lint + format (blocking gate). Deps are range-pinned in `pyproject.toml`; `uv.lock` is authoritative.

## Code layout

```
src/
  lib.rs              # valkey_module! macro, command & type registration
  commands/           # FLASH.* command handlers
  types/              # tiered data types (string, hash, list, zset)
  storage/            # NVMe backend (io_uring) + cache + WAL
  persistence/        # RDB / AOF / aux callbacks
  cluster/            # slot migration + replica tier plumbing
  replication.rs      # Replicate() plumbing
  config.rs           # cache size, NVMe path, policy knobs
  metrics.rs          # INFO section + stats
  async_io.rs         # BlockClient / ThreadSafeContext helpers
tests/                # Python integration tests (pytest + valkey-test-framework)
build.sh              # format, lint, unit + integration tests
.github/workflows/    # CI matrix over Valkey versions + ASAN + bench + fuzz + loom
docker/               # Dockerfile + compose (single + cluster) + seccomp profile
pyproject.toml        # Python deps + ruff config
uv.lock               # Pinned Python dep tree (authoritative)
```

## Conventions

- Rust edition 2024. CI builds with `RUSTFLAGS="-D warnings"`.
- `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings` are blocking CI gates.
- `ruff check` + `ruff format --check` are blocking gates for Python test code.
- Line coverage target ≥ 85% (enforced in CI via `cargo-llvm-cov`).
- Every public command must have integration tests covering: happy path, error paths, RDB + AOF round-trip, replication, keyspace notifications, Cold-tier demote/promote.
- Async NVMe paths run under ASAN in the nightly CI job; concurrency-critical code has `loom` tests.
- New commands go through a `backlog:spec` before `backlog:implement`.
- Commit style: [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) (`feat(storage): …`, `fix(cache): …`, `test(replication): …`).
- CHANGELOG entry required for every user-visible change (under `## [Unreleased]` until released).
- Agent reports (`.agent-reports/`) are gitignored — never force-add.

## References

- Valkey source: `/home/mbocevski/dev/cache/valkey`
  - Public module header: `src/valkeymodule.h`
  - Internal module plumbing: `src/module.h`, `src/module.c`
  - Example module: `src/modules/hellotype.c`
  - Eviction + memory accounting: `src/evict.c`, `src/object.c`
- Reference module (copy its conventions): `/home/mbocevski/dev/cache/valkey-bloom`
  - Entrypoint: `src/lib.rs`
  - Build: `build.sh`
  - CI: `.github/workflows/ci.yml`
  - Integration tests: `tests/`

## Workflow

- `backlog:tasks` — what's pending / in progress
- `backlog:spec <id>` — write a design doc (required before implementation for spec-tagged tasks)
- `backlog:implement <id>` — pick up and execute a task
- `backlog:refine` — groom the backlog
