# valkey-flash

A Valkey module that tiers key/value data to NVMe storage, letting Valkey use flash as an extension of RAM.

[![CI](https://github.com/valkey-io/valkey-flash/actions/workflows/ci.yml/badge.svg)](https://github.com/valkey-io/valkey-flash/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/valkey-io/valkey-flash/branch/main/graph/badge.svg)](https://codecov.io/gh/valkey-io/valkey-flash)

## Status

Early development. See the task backlog for planned work.

## Build

```sh
cargo build --release
```

The `.so` / `.dylib` is written to `target/release/libvalkey_flash.so`.

## Coverage

Generate a local HTML coverage report:

```sh
cargo llvm-cov --html --features enable-system-alloc --ignore-filename-regex 'src/wrapper/'
```

The report is written to `target/llvm-cov/html/index.html`. Open it in a browser to explore line-level coverage.

For a raw lcov file (e.g. for CI upload):

```sh
cargo llvm-cov --lcov --output-path lcov.info --features enable-system-alloc --ignore-filename-regex 'src/wrapper/'
```

## Testing

```sh
# Unit tests only
cargo test --features enable-system-alloc

# Full pipeline (fmt, clippy, unit + integration tests)
SERVER_VERSION=unstable ./build.sh
```

Integration tests require a compiled `valkey-server` binary — `build.sh` clones and compiles it into `tests/build/` automatically.

## License

BSD-3-Clause — see [LICENSE](LICENSE).
