# valkey-flash

> **Status: under development** — not production-ready.

valkey-flash is a Valkey module (Rust) that tiers key/value data to NVMe storage, letting Valkey use flash as an extension of RAM. Hot entries stay in RAM; cold entries are evicted to NVMe via an async io_uring I/O path that never blocks the Valkey event loop.

[![CI](https://github.com/valkey-io/valkey-flash/actions/workflows/ci.yml/badge.svg)](https://github.com/valkey-io/valkey-flash/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/valkey-io/valkey-flash/branch/main/graph/badge.svg)](https://codecov.io/gh/valkey-io/valkey-flash)

## Build & test

```sh
# Full pipeline: fmt check, clippy, unit tests, integration tests
SERVER_VERSION=unstable ./build.sh

# Module .so only
cargo build --release
# → target/release/libvalkey_flash.so
```

See [CLAUDE.md](CLAUDE.md) for architecture decisions and conventions.

## Coverage

Generate a local HTML coverage report:

```sh
cargo llvm-cov --html --features enable-system-alloc --ignore-filename-regex 'src/wrapper/'
# → target/llvm-cov/html/index.html
```

For an lcov file (CI upload):

```sh
cargo llvm-cov --lcov --output-path lcov.info --features enable-system-alloc --ignore-filename-regex 'src/wrapper/'
```

## Deployment

- [Deploying in a cluster](docs/cluster.md) — sizing, unique `flash.path` constraint, replica topologies, slot migration, failover, and troubleshooting
- [Running in containers](#running-in-containers) — Docker, Podman, Kubernetes with the io_uring seccomp profile
- [Developer workflow with Docker](docs/docker-tests.md) — local cluster Compose stack and integration test runner

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
  valkey-flash:dev
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
  valkey-flash:dev
```

**Rootless Podman** — same flag, with additional caveats:

- **Kernel <5.11:** the kernel blocks `io_uring` inside user namespaces; upgrade to ≥5.11 for rootless io_uring support.
- **SELinux (enforcing):** add `--security-opt label=disable`, or write a policy allowing io_uring from the container's label.
- **AppArmor:** if the default AppArmor profile is loaded, also pass `--security-opt apparmor=unconfined`.
- **systemd user units with `NoNewPrivileges=yes`:** override with `NoNewPrivileges=no` in the unit's `[Service]` section; `NoNewPrivileges` prevents applying seccomp profiles at container start.

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

> `PodSecurityPolicy` was removed in Kubernetes 1.25. The examples above use the current `securityContext.seccompProfile` field (stable since Kubernetes 1.19).

### Developer workflow

For building the image locally, running integration tests, and the single-node / cluster Compose stacks, see [docs/docker-tests.md](docs/docker-tests.md).

## License

[BSD-3-Clause](LICENSE)
