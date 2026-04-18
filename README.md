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

## Running in containers

### Why io_uring requires a seccomp override

valkey-flash's NVMe I/O path uses `io_uring` (syscalls `io_uring_setup`, `io_uring_enter`, `io_uring_register`). The default seccomp profiles shipped by Docker and Podman block these syscalls. A plain `docker run` will fail at module load with an io_uring setup error unless you override the seccomp policy.

**Kernel requirement:** Linux ≥5.6. Earlier kernels lack the required io_uring APIs entirely.

### Docker

**Development / CI — disable seccomp:**

```sh
docker run --rm \
  --security-opt seccomp=unconfined \
  -e FLASH_PATH=/data/flash \
  -e FLASH_CAPACITY_BYTES=1073741824 \
  -v flash-data:/data \
  valkey-flash:dev
```

**Docker Compose** — add `security_opt` to the service:

```yaml
services:
  valkey-flash:
    image: valkey-flash:dev
    security_opt:
      - seccomp=unconfined
    volumes:
      - flash-data:/data
    environment:
      FLASH_PATH: /data/flash
      FLASH_CAPACITY_BYTES: 1073741824
```

**Production:** avoid `unconfined` — use a tight custom profile that allows only the three io_uring syscalls. A ready-made profile is tracked in issue #89 (coming soon); pass it via `--security-opt seccomp=/path/to/valkey-flash-io_uring.json`.

### Podman

**Rootful Podman** behaves identically to Docker:

```sh
sudo podman run --rm \
  --security-opt seccomp=unconfined \
  -e FLASH_PATH=/data/flash \
  -e FLASH_CAPACITY_BYTES=1073741824 \
  -v flash-data:/data \
  valkey-flash:dev
```

**Rootless Podman** — same flag, with additional caveats:

- **Kernel <5.11:** the kernel blocks `io_uring` inside user namespaces; upgrade to ≥5.11 for rootless io_uring support.
- **SELinux (enforcing):** add `--security-opt label=disable`, or write a policy allowing io_uring from the container's label.
- **AppArmor:** if the default AppArmor profile is loaded, also pass `--security-opt apparmor=unconfined`.
- **systemd user units with `NoNewPrivileges=yes`:** override with `NoNewPrivileges=no` in the unit's `[Service]` section; `NoNewPrivileges` prevents seccomp profile changes at runtime.

```sh
podman run --rm \
  --security-opt seccomp=unconfined \
  -e FLASH_PATH=/data/flash \
  -e FLASH_CAPACITY_BYTES=1073741824 \
  -v flash-data:/data \
  valkey-flash:dev
```

**podman-compose** — identical `security_opt` syntax to Docker Compose above.

### Kubernetes

Set `seccompProfile.type: Unconfined` on the container (dev/staging) or `Localhost` with a custom profile (production):

```yaml
# dev / staging
securityContext:
  seccompProfile:
    type: Unconfined
```

```yaml
# production — requires the profile from issue #89 deployed to each node
securityContext:
  seccompProfile:
    type: Localhost
    localhostProfile: profiles/valkey-flash-io_uring.json
```

**Pod Security Standards note:** the `restricted` profile mandates `seccompProfile.type: RuntimeDefault` or `Localhost`. To run valkey-flash you need the `baseline` (or `privileged`) namespace label, or a per-namespace exemption, until the custom profile from #89 is available.

> `PodSecurityPolicy` was removed in Kubernetes 1.25. The examples above use the current `securityContext.seccompProfile` field (stable since Kubernetes 1.19).

### Developer workflow

For building the image locally, running integration tests, and the single-node / cluster Compose stacks, see [docs/docker-tests.md](docs/docker-tests.md).

## License

[BSD-3-Clause](LICENSE)
