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

## License

[BSD-3-Clause](LICENSE)
