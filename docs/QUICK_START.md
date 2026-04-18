# Quick start

A 5-minute walkthrough: install, run, and use valkey-flash in single-node and cluster modes.

## Prerequisites

- Linux with kernel ≥ 5.6 (run `uname -r` to check) — required for `io_uring`
- Either Docker ≥ 24.0 with Compose v2, or a Valkey server with ability to `--loadmodule`
- `valkey-cli` for testing

## Path A — Docker single-node

Fastest way to try the module.

### 1. Pull and run

```sh
docker run --rm -d --name flash \
  -p 6379:6379 \
  --security-opt seccomp=docker/seccomp-flash.json \
  -e FLASH_PATH=/data/flash \
  -e FLASH_CAPACITY_BYTES=1073741824 \
  -e FLASH_CACHE_SIZE_BYTES=268435456 \
  -v flash-data:/data \
  ghcr.io/valkey-io/valkey-flash:1.0.0
```

If you don't have the `seccomp-flash.json` file handy, swap the `--security-opt` to `seccomp=unconfined` for quick-start.

Wait for the module to finish recovery:

```sh
until [ "$(valkey-cli FLASH.DEBUG.STATE)" = "ready" ]; do sleep 0.2; done
```

### 2. Try the tiered commands

```sh
valkey-cli FLASH.SET hello world
# → OK

valkey-cli FLASH.GET hello
# → "world"

valkey-cli FLASH.HSET session:42 user_id 1001 last_seen 1713456789
# → (integer) 2

valkey-cli FLASH.HGETALL session:42
# → 1) "user_id"  2) "1001"  3) "last_seen"  4) "1713456789"

```

> `FLASH.LPUSH` / `FLASH.ZADD` and the rest of the list + sorted-set APIs land in a follow-on v1.x release. v1.0.0 ships strings and hashes.

### 3. Observe the tiering in action

```sh
valkey-cli INFO flash | grep flash_
# → flash_cache_hits, flash_cache_misses, flash_cache_size_bytes,
#   flash_storage_used_bytes, flash_storage_free_bytes,
#   flash_tiered_keys, flash_module_state, ...
```

As you add more keys than the cache can hold, `flash_tiered_keys` will grow and `flash_storage_used_bytes` will climb. Subsequent `FLASH.GET` calls on cold keys transparently fetch from NVMe and re-promote to cache.

### 4. Clean up

```sh
docker stop flash
docker volume rm flash-data
```

## Path B — Local `valkey-server` with the module

If you're building from source or already have Valkey running.

### 1. Build the module

```sh
git clone https://github.com/valkey-io/valkey-flash
cd valkey-flash
cargo build --release
# → target/release/libvalkey_flash.so
```

### 2. Start Valkey with the module

```sh
valkey-server \
  --loadmodule ./target/release/libvalkey_flash.so \
    flash.path /var/lib/valkey/flash.bin \
    flash.capacity-bytes 10737418240 \
    flash.cache-size-bytes 1073741824
```

### 3. Use it

Same commands as Path A step 2.

## Path C — Docker Compose cluster (3 primary + 3 replica)

Exercise cluster mode, slot distribution, and replica propagation.

### 1. Bring up the cluster

```sh
docker compose -f docker/compose.cluster.yml up -d
# Wait ~15 seconds for CLUSTER INIT to complete
```

### 2. Connect with a cluster-aware client

```sh
valkey-cli -c -p 7001
```

```
127.0.0.1:7001> FLASH.SET user:1 "alice"
-> Redirected to slot [14757] located at 127.0.0.1:7003
OK
127.0.0.1:7003> FLASH.GET user:1
"alice"
```

The `-c` flag enables follow-redirects. Without it you get `MOVED` errors.

### 3. Verify replica propagation

```sh
# Find which replica serves the shard holding user:1
valkey-cli -p 7001 CLUSTER NODES

# Connect directly to that replica (replace 7004 with the actual port).
# READONLY is connection-scoped, so issue it and the FLASH.GET on the same session:
printf 'READONLY\nFLASH.GET user:1\n' | valkey-cli -p 7004
# → OK
#   "alice"
```

### 4. Clean up

```sh
docker compose -f docker/compose.cluster.yml down -v
```

## Next steps

- **[Cluster deployment](cluster.md)** — production sizing, replica tier topology, failover
- **[Migration runbook](cluster-migration-runbook.md)** — live resharding
- **[Architecture](ARCHITECTURE.md)** — design consolidation (component map, durability matrix, tiering internals)
- **[Command reference](../README.md#commands)** — full surface shipped in v1.0.0
- **[Configuration reference](../README.md#configuration)** — all knobs

## Troubleshooting

**"Failed to initialize io_uring"** — Your kernel is < 5.6, or the container's seccomp profile blocks `io_uring_setup`. Check `uname -r` on the host and re-read [Running in containers](../README.md#running-in-containers).

**"ERR FLASH-MIGRATE target ... does not have flash-module loaded"** — During slot migration, the target node isn't running the module. `MODULE LOAD` it or add to `--loadmodule` on the target's `valkey-server` args.

**"WRONGTYPE Operation against a key holding the wrong kind of value"** — You used a `FLASH.*` command on a native key, or vice versa. FLASH types are a different module-type-id from native; they can't be mixed on the same key.

**Empty `flash_storage_used_bytes` after many `FLASH.SET`s** — Your cache is big enough to hold all keys in RAM. Lower `flash.cache-size-bytes` or write more keys to force demotion.
