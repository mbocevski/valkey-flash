# CI workflows

Overview of the CI workflows and how to reproduce them locally.

## Workflows

| File | Trigger | Purpose |
|---|---|---|
| `ci.yml` | push / PR | Build, unit tests, integration tests, ASAN (server-level), latency smoke, benchmarks, Docker integration, fuzz smoke, coverage |
| `fuzz-nightly.yml` | nightly 02:00 UTC | Extended 60-minute fuzz runs for WAL, RDB, and cache-ops targets |
| `loom.yml` | push / PR / weekly Mon 03:00 UTC | Concurrency tests under the loom model checker |
| `rust-asan-nightly.yml` | nightly 03:37 UTC | Rust-side ASAN — catches memory-safety violations in module code (unsafe blocks, raw-pointer misuse) |

---

## Rust-side ASAN (`rust-asan-nightly.yml`)

The existing `asan-build` job in `ci.yml` builds the Valkey server with GCC ASAN, which catches C-level memory leaks that originate from the module's allocations. It does **not** instrument the Rust module code itself.

`rust-asan-nightly.yml` complements this by building the Rust cdylib with Rust's LLVM-based AddressSanitizer (`-Z sanitizer=address`). This catches:

- Off-by-one errors and heap buffer overflows in `unsafe` blocks
- Use-after-free bugs in raw-pointer code
- Stack buffer overflows in unsafe Rust
- Any memory-safety violation in Rust code that GCC ASan (operating on C allocations only) would miss

Both the Rust module and the Valkey server are built with LLVM/clang ASAN so they share the same ASan runtime — no runtime conflicts.

### Reproducing locally

**Install a pinned nightly toolchain:**

```sh
rustup toolchain install nightly-2026-04-01 \
  --no-self-update --profile minimal \
  --component rust-src
```

**Build the ASAN-instrumented module:**

```sh
RUSTFLAGS="-Z sanitizer=address" \
  cargo +nightly-2026-04-01 build \
  --target x86_64-unknown-linux-gnu \
  --features enable-system-alloc
# → target/x86_64-unknown-linux-gnu/debug/libvalkey_flash.so
```

**Build valkey-server with clang ASAN:**

```sh
git clone https://github.com/valkey-io/valkey.git /tmp/valkey
cd /tmp/valkey
CC=clang CXX=clang++ make -j SANITIZER=address BUILD_TLS=module
```

**Run tests:**

```sh
mkdir -p /tmp/asan-logs
ASAN_OPTIONS="detect_leaks=1:log_path=/tmp/asan-logs/asan:abort_on_error=0" \
  MODULE_PATH="$PWD/target/x86_64-unknown-linux-gnu/debug/libvalkey_flash.so" \
  SERVER_BIN="/tmp/valkey/src/valkey-server" \
  python -m pytest tests/ \
    --ignore=tests/test_docker_smoke.py \
    --ignore=tests/test_flash_acl.py \
    --ignore=tests/test_flash_aof.py \
    -m "not skip_for_asan" \
    -v --tb=short

# Check for violations
grep -l "SUMMARY: AddressSanitizer" /tmp/asan-logs/asan.* 2>/dev/null \
  && cat /tmp/asan-logs/asan.* \
  || echo "No ASan violations"
```

**Quick single-test ASAN run:**

```sh
ASAN_OPTIONS="detect_leaks=1:log_path=/tmp/asan-logs/asan:abort_on_error=1" \
  MODULE_PATH="..." \
  python -m pytest tests/test_flash_basic.py -v
```

### Skipping tests under ASAN

Mark tests that are incompatible with ASAN runs (e.g., tests that intentionally leak, or assert on exact allocation counts) with `@pytest.mark.skip_for_asan`:

```python
import pytest

@pytest.mark.skip_for_asan
def test_that_intentionally_leaks(client):
    ...
```

The nightly workflow filters these out with `-m "not skip_for_asan"`.

### Updating the pinned nightly

Change `NIGHTLY_TOOLCHAIN` in `.github/workflows/rust-asan-nightly.yml`. The key in the toolchain cache (`rust-toolchain-<date>`) will miss on first run, triggering a fresh download, then warm on subsequent nights.
