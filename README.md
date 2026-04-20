# valkey-flash

[![CI](https://github.com/mbocevski/valkey-flash/actions/workflows/ci.yml/badge.svg)](https://github.com/mbocevski/valkey-flash/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/mbocevski/valkey-flash/branch/main/graph/badge.svg)](https://codecov.io/gh/mbocevski/valkey-flash)
[![License](https://img.shields.io/badge/license-BSD--3--Clause-blue)](LICENSE)

A Valkey module that tiers key/value data to NVMe storage, letting Valkey serve a working set larger than RAM. Hot entries live in an in-memory cache; cold entries reside on NVMe and are promoted back on read. The NVMe I/O path uses `io_uring` and runs on background threads — the Valkey event loop never blocks on disk.

valkey-flash ships full cluster support, durable writes via a per-record WAL, and native RDB + AOF persistence. **Strings, hashes, lists, and sorted sets** are all supported as tiered types in v1.0.0 — see the [command reference](#commands) below and [CHANGELOG](CHANGELOG.md) for details.

## Quick start

```sh
# Run the docker image (requires Linux kernel ≥5.6 for io_uring)
docker run --rm -p 6379:6379 \
  --security-opt seccomp=docker/seccomp-flash.json \
  -e FLASH_PATH=/data/flash -e FLASH_CAPACITY_BYTES=1073741824 \
  -v flash-data:/data \
  ghcr.io/mbocevski/valkey-flash:1.0.0

# From another shell
valkey-cli FLASH.SET hello world
valkey-cli FLASH.GET hello
# → "world"
valkey-cli INFO flash
```

See [docs/QUICK_START.md](docs/QUICK_START.md) for a hands-on walkthrough including cluster mode.

## Why valkey-flash

The practical ceiling on RAM-only Valkey deployments is either cost (RAM is expensive) or physical memory per node. valkey-flash extends the addressable working set by tiering colder data to NVMe, trading a microsecond-scale RAM access for a ~100 µs NVMe read on cold-path fetches. Hot-path reads (in-cache) stay RAM-speed.

Use cases where valkey-flash fits well:
- **Session stores** with long-tailed access patterns where 10–20% of sessions drive 80% of traffic
- **Feature stores** serving ML inference where the working set is orders of magnitude larger than RAM but per-request latency budget is generous
- **Content caches** where LRU eviction would thrash an under-sized cache — tiering keeps cold items addressable instead of evicting them
- **User-state stores** during reconnection storms where rehydration from upstream is expensive

It is not the right fit if every request is latency-critical at tail percentiles and the working set comfortably fits in RAM — native Valkey is cheaper and simpler.

## Will it help my workload? — `flash-sizer`

Before installing anything, point [`flash-sizer`](https://github.com/mbocevski/valkey-flash-sizer) at an existing Valkey and find out. The tool is read-only (only `SCAN` + `MEMORY USAGE` + `OBJECT IDLETIME`), samples up to 100 000 keys, and prints a Markdown report with a 95 % Wilson-score confidence interval on the fraction of your working set that's cold enough to tier.

```
uvx valkey-flash-sizer valkey://my-host:6379
```

No install, no write path, no phone-home. Sample output: [`tests/golden/report.md`](https://github.com/mbocevski/valkey-flash-sizer/blob/main/tests/golden/report.md).

## Installation

### Pre-built binaries

Releases publish `.so` artifacts for linux-x86_64 and linux-aarch64. Download from [GitHub Releases](https://github.com/mbocevski/valkey-flash/releases) and load with:

```
valkey-server --loadmodule /path/to/libvalkey_flash.so \
              flash.path /data/flash.bin \
              flash.capacity-bytes 10737418240
```

### Docker / Podman / Kubernetes

Multi-arch images are published to GHCR at `ghcr.io/mbocevski/valkey-flash`. See [Running in containers](#running-in-containers) below for the seccomp requirement and platform-specific notes.

### Build from source

```sh
# Full pipeline: fmt check, clippy, unit tests, integration tests
SERVER_VERSION=unstable ./build.sh

# Module .so only
cargo build --release
# → target/release/libvalkey_flash.so
```

Build requires Rust (pinned to `rust:1.95.0-trixie` in the Dockerfile; `cargo --version` should be ≥1.90 locally for edition-2024 support).

## Commands

All FLASH.* commands are opt-in per key — existing native Valkey data types are unchanged.

### Strings (`FlashString`)

| Command | Purpose |
|---|---|
| `FLASH.SET key value [EX\|PX\|EXAT\|PXAT ...] [NX\|XX]` | Set value, optional TTL and conditional flags |
| `FLASH.GET key` | Get value (nil if missing/expired) |
| `FLASH.DEL key [key ...]` | Delete one or more keys; returns count of deleted |

### Hashes (`FlashHash`)

| Command | Purpose |
|---|---|
| `FLASH.HSET key field value [field value ...] [EX\|PX\|EXAT\|PXAT ...] [KEEPTTL]` | Set fields with optional TTL on the key |
| `FLASH.HGET key field` | Get field value |
| `FLASH.HGETALL key` | Return all field-value pairs |
| `FLASH.HDEL key field [field ...]` | Delete fields; empty hash deletes the key |
| `FLASH.HEXISTS key field` | 0 or 1 |
| `FLASH.HLEN key` | Number of fields |

### Lists (`FlashList`)

| Command | Purpose |
|---|---|
| `FLASH.LPUSH key value [value ...]` / `FLASH.RPUSH key value [value ...]` | Push values to the head / tail |
| `FLASH.LPUSHX key value [value ...]` / `FLASH.RPUSHX key value [value ...]` | Push only if the key exists |
| `FLASH.LPOP key [count]` / `FLASH.RPOP key [count]` | Pop one or more values from the head / tail |
| `FLASH.LLEN key` | Number of elements |
| `FLASH.LRANGE key start stop` | Slice (inclusive, supports negative indices) |
| `FLASH.LINDEX key index` | Element at index |
| `FLASH.LSET key index value` | Overwrite element at index |
| `FLASH.LINSERT key BEFORE\|AFTER pivot value` | Insert relative to pivot |
| `FLASH.LREM key count value` | Remove occurrences |
| `FLASH.LTRIM key start stop` | Keep only `[start, stop]` slice |
| `FLASH.LMOVE src dst LEFT\|RIGHT LEFT\|RIGHT` | Atomic move between lists |
| `FLASH.RPOPLPUSH src dst` | Shorthand for LMOVE RIGHT LEFT |
| `FLASH.BLPOP key [key ...] timeout` / `FLASH.BRPOP ... timeout` | Blocking pop; `0` = block indefinitely |
| `FLASH.BLMOVE src dst LEFT\|RIGHT LEFT\|RIGHT timeout` | Blocking variant of LMOVE |

### Sorted sets (`FlashZSet`)

| Command | Purpose |
|---|---|
| `FLASH.ZADD key [NX\|XX] [GT\|LT] [CH] [INCR] score member [score member ...]` | Insert or update members |
| `FLASH.ZREM key member [member ...]` | Remove members |
| `FLASH.ZINCRBY key increment member` | Increment a member's score (NaN-guarded) |
| `FLASH.ZPOPMIN key [count]` / `FLASH.ZPOPMAX key [count]` | Pop lowest / highest-scored members |
| `FLASH.BZPOPMIN key [key ...] timeout` / `FLASH.BZPOPMAX ... timeout` | Blocking pop variants |
| `FLASH.ZSCORE key member` | Member's score (nil if absent) |
| `FLASH.ZRANK key member [WITHSCORE]` / `FLASH.ZREVRANK key member [WITHSCORE]` | Rank by score ascending / descending |
| `FLASH.ZCARD key` | Number of members |
| `FLASH.ZCOUNT key min max` / `FLASH.ZLEXCOUNT key min max` | Count by score / lex range |
| `FLASH.ZRANGE key start stop [BYSCORE\|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]` | Unified range query |
| `FLASH.ZRANGEBYSCORE` / `FLASH.ZREVRANGEBYSCORE` / `FLASH.ZRANGEBYLEX` / `FLASH.ZREVRANGEBYLEX` | Legacy range aliases |
| `FLASH.ZSCAN key cursor [MATCH pattern] [COUNT count]` | Incremental iteration |
| `FLASH.ZUNIONSTORE` / `FLASH.ZINTERSTORE` / `FLASH.ZDIFFSTORE` / `FLASH.ZRANGESTORE` | Set arithmetic with `WEIGHTS` / `AGGREGATE SUM\|MIN\|MAX` |

### Admin / debug

| Command | Purpose |
|---|---|
| `FLASH.DEBUG.STATE` | Return module state (`recovering`, `ready`, `error`) |
| `FLASH.DEBUG.DEMOTE key` | Force a Hot→Cold demotion (test-only; `@admin @dangerous`) |
| `FLASH.COMPACTION.TRIGGER` | Manually run NVMe compaction (test-only) |
| `FLASH.COMPACTION.STATS` | Current free-list state |
| `FLASH.MIGRATE.PROBE [host port]` | Query local or remote node state, capacity, path |
| `FLASH.MIGRATE` | Extended MIGRATE hook for FLASH.* keys — bundles DUMP/RESTORE with tier state, capacity-probe gated |

## Configuration

Loaded-module args take the form `flash.<knob> <value>` on the `valkey-server --loadmodule` line.

| Knob | Default | Mutable at runtime | Description |
|---|---|---|---|
| `flash.path` | `/tmp/valkey-flash.bin` | No | NVMe backing file path |
| `flash.capacity-bytes` | `1073741824` (1 GiB) | No | Backing file size |
| `flash.cache-size-bytes` | `268435456` (256 MiB) | Yes | Hot tier RAM cap |
| `flash.sync` | `everysec` | Yes | WAL fsync mode: `always`, `everysec`, `no` |
| `flash.io-threads` | `num_cpus()` | No | Async I/O worker pool size |
| `flash.io-uring-entries` | `256` | No | io_uring submission queue depth per worker |
| `flash.compaction-interval-sec` | `60` | Yes | NVMe compaction cadence |
| `flash.replica-tier-enabled` | `no` | No | Opt-in symmetric tiering on replicas (cluster recommendation: `yes`) |
| `flash.cluster-mode-enabled` | `auto` | No | `auto`/`yes`/`no` — cluster-mode detection |
| `flash.migration-bandwidth-mbps` | `100` (0 = unlimited) | Yes | Slot-migration rate cap |
| `flash.migration-max-key-bytes` | `67108864` (64 MiB) | No | Per-key pre-warm cap; larger keys migrate via NVMe-read path |
| `flash.migration-chunk-timeout-sec` | `30` | Yes | Per-chunk migration timeout |
| `flash.migration-probe-cache-sec` | `60` | No | `FLASH.MIGRATE.PROBE` result cache TTL |

Runtime changes for mutable knobs use the standard `CONFIG SET` interface:

```
valkey-cli CONFIG SET flash.cache-size-bytes 2147483648
```

Immutable knobs reject `CONFIG SET` with a clear error identifying the restart requirement.

## Deployment

- **[Single-node deployment](#installation)** — the default; see the Quick start above.
- **[Cluster deployment](docs/cluster.md)** — sizing, unique `flash.path` constraint per node, replica topologies, slot migration, failover, and troubleshooting.
- **[Migration runbook](docs/cluster-migration-runbook.md)** — operator step-by-step for resharding a live cluster.
- **[Running in containers](#running-in-containers)** — Docker, Podman, Kubernetes with the io_uring seccomp profile.
- **[Developer workflow with Docker](docs/docker-tests.md)** — local Compose stacks and integration test runner.

## Running in containers

### Why io_uring requires a seccomp override

valkey-flash's NVMe I/O path uses `io_uring` (syscalls `io_uring_setup`, `io_uring_enter`, `io_uring_register`). The default seccomp profiles shipped by Docker and Podman block these syscalls. A plain `docker run` will fail at module load with an io_uring setup error unless you override the seccomp policy.

**Kernel requirement:** Linux ≥5.6. Earlier kernels lack the required io_uring APIs entirely.

### Seccomp profile

[`docker/seccomp-flash.json`](docker/seccomp-flash.json) is the recommended profile. It is Docker's default syscall allowlist extended with only the three `io_uring` syscalls (`io_uring_setup`, `io_uring_enter`, `io_uring_register`, min kernel 5.1). All other restrictions from the default profile remain in place.

For quick-start / CI, `--security-opt seccomp=unconfined` also works but removes all syscall filtering.

### Docker

**With the custom profile (recommended):**

```sh
docker run --rm \
  --security-opt seccomp=docker/seccomp-flash.json \
  -e FLASH_PATH=/data/flash \
  -e FLASH_CAPACITY_BYTES=1073741824 \
  -v flash-data:/data \
  ghcr.io/mbocevski/valkey-flash:1.0.0
```

**Docker Compose** — the bundled [`docker/compose.single.yml`](docker/compose.single.yml) already uses the profile:

```yaml
security_opt:
  - seccomp:./seccomp-flash.json
```

**Cluster Compose**: [`docker/compose.cluster.yml`](docker/compose.cluster.yml) gives each of the six nodes (three primaries, three replicas) a separate named volume, satisfying the requirement that every flash-tier node has a **unique `flash.path`**. If you override `FLASH_PATH`, ensure each container maps to a different host path or volume — two nodes sharing the same file will silently corrupt each other's NVMe tier.

To revert to `unconfined` for quick iteration, overlay with the dev override:

```sh
docker compose -f docker/compose.single.yml -f docker/compose.single.dev.yml up
```

### Podman

**Rootful Podman** uses the same flag:

```sh
sudo podman run --rm \
  --security-opt seccomp=docker/seccomp-flash.json \
  -e FLASH_PATH=/data/flash \
  -e FLASH_CAPACITY_BYTES=1073741824 \
  -v flash-data:/data \
  ghcr.io/mbocevski/valkey-flash:1.0.0
```

**Rootless Podman** — same flag, with additional caveats:

- **Kernel <5.11:** the kernel blocks `io_uring` inside user namespaces; upgrade to ≥5.11 for rootless io_uring support.
- **SELinux (enforcing):** add `--security-opt label=disable`, or write a policy allowing io_uring from the container's label.
- **AppArmor:** if the default AppArmor profile is loaded, also pass `--security-opt apparmor=unconfined`.
- **systemd user units with `NoNewPrivileges=yes`:** override with `NoNewPrivileges=no` in the unit's `[Service]` section.

**podman-compose** — same `security_opt` syntax as Docker Compose.

### Kubernetes

Copy `docker/seccomp-flash.json` to each node's seccomp profile directory (typically `/var/lib/kubelet/seccomp/profiles/`) and reference it:

```yaml
# recommended for production
securityContext:
  seccompProfile:
    type: Localhost
    localhostProfile: profiles/seccomp-flash.json
```

```yaml
# dev / staging only
securityContext:
  seccompProfile:
    type: Unconfined
```

**Pod Security Standards note:** the `restricted` profile mandates `seccompProfile.type: RuntimeDefault` or `Localhost`. The `Localhost` + `seccomp-flash.json` approach satisfies the `restricted` standard once the profile file is deployed to nodes.

## Compatibility

| | Versions |
|---|---|
| Valkey | unstable, 8.1, 9.0 |
| Rust (build) | ≥1.90 (edition 2024) |
| Linux kernel | ≥5.6 (io_uring), ≥5.11 recommended for rootless containers |
| Platforms (shipped binaries) | linux-x86_64, linux-aarch64 |
| Clients | Any RESP-compliant client; cluster-mode required for cluster deployments (tested against valkey-py) |

## Stability and versioning

valkey-flash follows strict [Semantic Versioning](https://semver.org/). Starting at **v1.0.0**:

- All `FLASH.*` command names, argument shapes, and reply formats are stable
- All `flash.*` configuration knobs and their value semantics are stable
- RDB and AOF on-disk formats are stable (encoding_version byte supports forward-compatible additions)
- WAL on-disk format is stable

Breaking changes to any of the above require a major version bump (v2.0.0). Additive changes (new commands, new optional args, new config knobs with safe defaults) land in minor releases.

Stretch v1.x additions planned for follow-on minor releases (not breaking): full-Rust ASAN instrumentation (CI-level), import-side migration byte tracking, chunked streaming for keys larger than `flash.migration-max-key-bytes`.

## Documentation index

| Document | Audience |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Implementers / contributors — design consolidation across 11 spec decisions |
| [docs/cluster.md](docs/cluster.md) | Operators — deployment, sizing, failover, troubleshooting |
| [docs/cluster-migration-runbook.md](docs/cluster-migration-runbook.md) | Operators — step-by-step live resharding |
| [docs/docker-tests.md](docs/docker-tests.md) | Developers — local Docker stacks + tests |
| [docs/ci.md](docs/ci.md) | Developers — CI workflows and local reproduction |
| [CHANGELOG.md](CHANGELOG.md) | All — per-release change log (Keep a Changelog format) |
| [SECURITY.md](SECURITY.md) | Security researchers — vulnerability disclosure via GitHub Security Advisories |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Contributors — workflow and Conventional Commits |
| [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) | Contributors — community standards |

## Contributing

Pull requests welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for the workflow. All commits use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/); the release process is automated from the tag history.

Local test loop:

```sh
./build.sh               # fmt + clippy + unit + integration tests
cargo llvm-cov --html    # coverage report
make docker-test-single  # integration tests against a Docker stack
make docker-test-cluster # 3-primary + 3-replica Compose stack
```

## Security

Report vulnerabilities through [GitHub Security Advisories](https://github.com/mbocevski/valkey-flash/security/advisories/new) — see [SECURITY.md](SECURITY.md) for the full policy.

## License

[BSD-3-Clause](LICENSE)
