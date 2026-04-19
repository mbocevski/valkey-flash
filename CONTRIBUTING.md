# Contributing to valkey-flash

Thank you for your interest in contributing.

## Before you start

- Read [CLAUDE.md](CLAUDE.md) for architecture decisions, conventions, and open design questions.
- Check the task backlog (`backlog:tasks`) for planned work and active specs.
- New commands or data types require a spec task (`backlog:spec`) to be approved before implementation begins.

## Workflow

1. Fork the repository and create a feature branch.
2. Install [uv](https://docs.astral.sh/uv/) if you don't have it:
   ```sh
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```
3. Run `./build.sh` before opening a pull request — it syncs deps, runs ruff, fmt, clippy, unit tests, and integration tests:
   ```sh
   SERVER_VERSION=unstable ./build.sh
   ```
4. All CI gates must pass: ruff, fmt, clippy (`-D warnings`), unit tests, and integration tests.
5. Open a pull request with a clear description of what changed and why.

### Python tooling

Python deps are managed with uv and declared in `pyproject.toml`. Use `uv sync` to install, `uv run pytest` to run tests:

```sh
uv sync                          # install/update deps from uv.lock
uv run pytest tests/             # run integration tests
uv run ruff check .              # lint
uv run ruff format --check .     # format check
uv run ruff format .             # format (auto-fix)
```

## Commit style

This project uses [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).

Format: `type(scope): description`

Examples:
```
feat(storage): add LRU eviction to cache layer
fix(wal): handle ops >4KB in truncate path
test(replication): add WAIT timeout coverage
ci: pin cargo-llvm-cov version
docs: update README coverage section
```

Types: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`

## Code conventions

- Rust edition 2021, `RUSTFLAGS="-D warnings"` in CI.
- `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings` are blocking gates.
- Line coverage target ≥ 85% (enforced in CI once sufficient code exists).
- Every public command needs integration tests: happy path, error paths, RDB + AOF round-trip, replication, keyspace notifications.

## Docker-based testing

Full end-to-end tests run the module inside Docker against real single-node and cluster topologies. See [docs/docker-tests.md](docs/docker-tests.md) for setup, connection, pytest invocation, and debugging.

Quick start:

```sh
make docker-test        # build image + run both smoke tests
make docker-clean       # tear down all stacks and remove the image
```

## Fuzzing

Fuzz targets live in `fuzz/fuzz_targets/`. Running them requires nightly Rust and `cargo-fuzz`:

```sh
cargo install --version =0.13.1 --locked cargo-fuzz

# 30-second smoke (all targets)
cargo +nightly fuzz run fuzz_wal_record_parser -- -max_total_time=30
cargo +nightly fuzz run fuzz_rdb_deserializer  -- -max_total_time=30
cargo +nightly fuzz run fuzz_cache_ops         -- -max_total_time=30
```

Crashes land in `fuzz/artifacts/<target>/`. Corpus seeds are in `fuzz/corpus/<target>/`.

## Code of Conduct

See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).
