# valkey-flash cluster deployment guide

> **v1 scope**: this guide covers the valkey-flash cluster feature set as shipped in v1.
> Items marked **v1.1** are planned but not yet implemented.

## Contents

- [Overview](#overview)
- [Sizing](#sizing)
- [Critical constraint: unique flash.path per node](#critical-constraint-unique-flashpath-per-node)
- [Replica topologies](#replica-topologies)
- [Slot migration](#slot-migration)
- [Failover](#failover)
- [Capacity planning for migration bandwidth](#capacity-planning-for-migration-bandwidth)
- [Known limitations](#known-limitations)
- [Example: Docker Compose cluster](#example-docker-compose-cluster)
- [Troubleshooting](#troubleshooting)
- [Full configuration reference](#full-configuration-reference)

---

## Overview

valkey-flash works in Valkey cluster mode without any special topology changes.
Each primary node that loads the module gets an independent NVMe tier; FLASH.*
keys hash and route to slots exactly like any other key type, so cluster-aware
clients (valkey-py cluster client, redis-py cluster, ioredis cluster, etc.) work
out of the box.

**What works in v1**

- FLASH.SET / FLASH.GET / FLASH.DEL and the full FLASH.HASH family on any node
- Cluster MOVED/ASK redirect handling — all FLASH.* commands carry correct
  `key_spec` so Valkey computes the right slot and emits the right redirect
- Slot migration of both FlashString and FlashHash keys (hot and cold tier)
- Per-node INFO flash with cluster-mode field (`flash_cluster_mode:yes/no`)
- Migration metrics in INFO flash (8 `flash_migration_*` fields)
- Optional flash tier on replicas (`flash.replica-tier-enabled=true`)
- `FLASH.MIGRATE.PROBE` for pre-migration capacity checks

**v1.1 planned**

- Chunked streaming for keys > `flash.migration-max-key-bytes`
- Import-side byte tracking (`flash_migration_bytes_received` is always 0 in v1)
- Capacity-gate enforcement before slot migration begins

---

## Sizing

### NVMe capacity per node (`flash.capacity-bytes`)

Set `flash.capacity-bytes` to the expected cold working-set size **per primary
shard**, not the cluster total. A 3-primary cluster holding 300 GiB of cold data
needs 100 GiB per primary (assuming uniform key distribution).

Add headroom for compaction overhead and write amplification. A 20–30% buffer is
typical:

```
flash.capacity-bytes = ceil(cold_working_set_per_shard * 1.25)
```

Minimum accepted value: 1 MiB. There is no enforced maximum; the file is
pre-allocated at module load so set it to what you want to allocate, not more.

### In-memory cache (`flash.cache-size-bytes`)

The cache holds the hot working set in RAM (W-TinyLFU policy). The default is
256 MiB. Size it to the hot working set per shard. A good starting point:

```
flash.cache-size-bytes = available_ram_per_node * 0.4
```

Leave at least 40% of available RAM for Valkey's own keyspace metadata and
buffers. `flash.cache-size-bytes` is **mutable at runtime** via `CONFIG SET`
and takes effect immediately (entries are migrated on the next eviction cycle).

### NVMe device mapping

Assign each primary node a **dedicated** backing file on its local NVMe device:

```
# primary-0
flash.path /mnt/nvme0/flash-0.bin

# primary-1
flash.path /mnt/nvme1/flash-1.bin
```

If multiple nodes share a host, use per-node subdirectories on the same device.
Two nodes sharing the same device divide its IOPS between them — ensure the
device can sustain the combined read/write throughput from both NVMe tiers plus
the pre-warm traffic during resharding.

---

## Critical constraint: unique flash.path per node

**Every node that opens the flash NVMe tier must have a unique `flash.path`.**

valkey-flash uses a single-writer file layout with no cross-process locking. If
two nodes point at the same file path simultaneously, they will silently
overwrite each other's block allocations, corrupting both tiers. There is no
error or warning — the corruption is detected later (if at all) during RDB
load or WAL replay.

This applies to:
- All primary nodes
- Replica nodes with `flash.replica-tier-enabled=true`

Replica nodes with `flash.replica-tier-enabled=false` (the default) do not open
the NVMe backend and are exempt from this constraint.

**Docker Compose**: the bundled `docker/compose.cluster.yml` gives each container
a separate named volume, so each node gets a unique file path automatically. If
you override `FLASH_PATH`, verify that each container maps to a different host
path or volume.

**Kubernetes**: use a per-pod `PersistentVolumeClaim` (not a shared PVC) and set
`flash.path` to a path within that claim. A `StatefulSet` with
`volumeClaimTemplates` is the standard approach.

---

## Replica topologies

### Default: RAM-only replicas (`flash.replica-tier-enabled=false`)

In this mode (the default), replica nodes hold the hot working set in RAM and do
not open a local NVMe backend. When a Cold-tier key is requested on a replica, it
returns whatever is in the hot RAM cache; if the key is absent from cache, the
replica cannot serve it directly (the client should read from the primary).

**Pros**: simpler ops, no per-replica NVMe allocation, no unique-path constraint
on replicas.

**Cons**: replicas consume more RAM (no tiering), and promotion to primary
requires a cold-read bootstrap from NVMe after promotion completes — the first
reads to cold keys on the newly promoted node hit NVMe until they warm up.

### Opt-in: symmetric tiering (`flash.replica-tier-enabled=true`)

When enabled, each replica opens its own local NVMe backend (independent of the
primary) at module load. The replica makes its own cache-admission and eviction
decisions, so its hot tier may differ from the primary's.

**Instant promotion**: if the NVMe tier is already initialized on the replica,
promotion to primary is instant — no lazy initialization on first write, no
`REPLICAOF NO ONE` restart required.

**Unique path required**: with `replica-tier-enabled=true`, every replica node
must have its own unique `flash.path` (see the constraint section above).

To enable:

```
# In valkey.conf (immutable after load — not settable via CONFIG SET)
loadmodule /path/to/libvalkey_flash.so flash.replica-tier-enabled yes
```

Or via environment variable in Docker Compose:

```yaml
environment:
  - FLASH_REPLICA_TIER_ENABLED=yes
```

---

## Slot migration

### How it works in v1

valkey-flash participates in the standard Valkey `CLUSTER SETSLOT` /
`MIGRATE` / `CLUSTER SETSLOT NODE` resharding protocol. No special migration
commands are needed from the operator — use any standard resharding tool
(valkey-cli --cluster reshard, the Valkey cluster manager, etc.).

When a slot export begins, valkey-flash runs a **two-phase pre-warm**:

1. **Scan**: the keyspace is scanned for Cold-tier Flash keys whose slot is in
   the migrating range. Keys larger than `flash.migration-max-key-bytes` are
   skipped during pre-warm (they are still migrated correctly via the normal
   RDB `rdb_save` path, which reads from NVMe on demand).

2. **Promote**: each collected Cold key is read from NVMe into RAM (throttled by
   `flash.migration-bandwidth-mbps`). The promoted keys are then transferred by
   Valkey's normal MIGRATE mechanism as Hot objects.

The pre-warm scan respects a wall-clock deadline (`flash.migration-chunk-timeout-sec`).
Keys not reached before the deadline are migrated via the slower NVMe-read path
during the actual MIGRATE transfer.

### Per-key atomicity

Each key is migrated atomically. There is no partial-key migration in v1.
If a key exceeds `flash.migration-max-key-bytes` and cannot be pre-warmed,
its Cold-tier data is read from NVMe during the MIGRATE call itself, adding
latency proportional to the key size.

### FLASH.MIGRATE.PROBE

Use `FLASH.MIGRATE.PROBE` to check whether a target node has the flash module
loaded and how much free capacity it has before starting a reshard:

```
# Check local node
FLASH.MIGRATE.PROBE

# Check a remote node
FLASH.MIGRATE.PROBE 192.168.1.10 6380
```

The response includes `state`, `capacity_bytes`, `free_bytes`, and `path`.
Results are cached per host:port for `flash.migration-probe-cache-sec` seconds
(default 60 s) to avoid hammering the target during repeated checks.

### Tuning migration

```
# Slow down NVMe reads during pre-warm to protect production latency
CONFIG SET flash.migration-bandwidth-mbps 50

# No throttle (useful for maintenance windows)
CONFIG SET flash.migration-bandwidth-mbps 0

# Extend the pre-warm window for large keyspaces
CONFIG SET flash.migration-chunk-timeout-sec 120
```

Changes to `flash.migration-bandwidth-mbps` take effect on the next key
promotion cycle — there is no need to restart the migration.

---

## Failover

Valkey's cluster failure detector operates on the primary node as a whole.
When a primary fails:

1. Cluster detects the failure within `cluster-node-timeout` milliseconds
   (default 15 s in most deployments).
2. One of the primary's replicas is elected and promoted via the standard
   Raft-like election.
3. On the promoted node:
   - If `flash.replica-tier-enabled=false`: the NVMe backend opens fresh on
     first write. Cold keys are not immediately available until warmed by traffic
     or an operator-triggered `FLASH.DEBUG.DEMOTE` → read cycle. Writes work
     immediately.
   - If `flash.replica-tier-enabled=true`: the NVMe tier was already open on the
     replica; promotion is instant and both reads and writes work immediately with
     no bootstrap period.

**No data loss in either case**: the WAL on the failed primary ensures that writes
acknowledged before the crash survive (subject to the configured `flash.sync`
policy — `everysec` may lose up to one second of writes, `always` provides
fsync-per-write durability).

---

## Capacity planning for migration bandwidth

Each resharding operation requires:

- **Network bandwidth**: normal MIGRATE traffic (depends on your key sizes and
  key count per slot)
- **NVMe read bandwidth**: pre-warm phase reads Cold keys from NVMe on the source
  node. At 100 Mbps (the default cap), a 10 GiB cold slot takes ~860 seconds to
  pre-warm fully. Increase `flash.migration-bandwidth-mbps` for faster reshards.

**Concurrent reshards** multiply the NVMe read load. If your NVMe device sustains
2 GB/s reads and you are pre-warming at 200 Mbps each, you can run up to ~80
concurrent slot migrations without saturating the device — but in practice, keep
concurrent migrations to ≤4 to avoid head-of-line blocking on the keyspace scan.

**Rule of thumb**: plan for `flash.migration-bandwidth-mbps × concurrent_migrations`
to stay below 50% of your NVMe device's sequential read bandwidth. The other 50%
should be left for normal hot-tier promotions and cache misses.

---

## Known limitations

| Limitation | Detail |
|---|---|
| Target node missing flash module | `FLASH.MIGRATE.PROBE` returns error; MIGRATE aborts with clear error (see troubleshooting) |
| Key > `flash.migration-max-key-bytes` | Key is skipped during pre-warm; migrated via slower NVMe-read path during MIGRATE call itself |
| Cluster-aware client required | Standard single-node clients receive MOVED/ASK redirects; use a cluster-mode client |
| Import-side byte tracking | `flash_migration_bytes_received` is always 0 in v1; export-side `flash_migration_bytes_sent` is accurate |
| Chunked streaming | Keys > `flash.migration-max-key-bytes` are not streamed in chunks; planned for v1.1 |
| Capacity gating | No pre-MIGRATE capacity check on target; planned for v1.1 as an enhancement to `FLASH.MIGRATE.PROBE` |
| `flash.path` sharing | No file locking; sharing a path between nodes silently corrupts both — see [critical constraint](#critical-constraint-unique-flashpath-per-node) |

---

## Example: Docker Compose cluster

The repo ships a three-primary, three-replica cluster Compose file at
[`docker/compose.cluster.yml`](../docker/compose.cluster.yml). Each node gets
a separate named volume (and therefore a separate `flash.path`) automatically.

```sh
# Build the image
make docker-build

# Start the cluster
docker compose -f docker/compose.cluster.yml -p vf-cluster up -d

# Verify
docker exec vf-cluster-primary-0-1 valkey-cli cluster info | grep cluster_state
docker exec vf-cluster-primary-0-1 valkey-cli info flash | grep flash_cluster_mode

# Run cluster integration tests (requires USE_DOCKER=1)
USE_DOCKER=1 python -m pytest tests/test_docker_smoke.py -v -m docker_cluster

# Tear down
make docker-clean
```

For full Docker / Podman / Kubernetes setup, see
[docs/docker-tests.md](docker-tests.md) and the
[Running in containers](../README.md#running-in-containers) section of the README.

---

## Troubleshooting

### `ERR FLASH-MIGRATE target <addr> does not have flash-module loaded`

The target node responded to `FLASH.MIGRATE.PROBE` but the flash module is not
loaded (the node returned `ERR unknown command`). Load the module on the target
before starting the reshard:

```
# On the target node
MODULE LOAD /path/to/libvalkey_flash.so flash.path /data/flash.bin flash.capacity-bytes 10737418240
```

Or add it to the target's `valkey.conf` and restart.

### `ERR FLASH-MIGRATE target <addr> did not respond within timeout`

The 5-second TCP connection or read timeout was hit. Check:
- Network connectivity between source and target (`ping`, `telnet <host> <port>`)
- Target is running and accepting connections (`PING`)
- Firewall rules allow inter-node traffic on the Valkey port

### `ERR FLASH-MIGRATE target <addr> invalid address`

The host or port string passed to `FLASH.MIGRATE.PROBE` could not be parsed.
Verify the address format: `FLASH.MIGRATE.PROBE <host> <port>` (two separate
arguments, port as an integer).

### Migration is slow / pre-warm is timing out

`flash_migration_keys_rejected` in `INFO flash` increments when keys are skipped
due to timeout or oversize. If this is high:

- Increase `flash.migration-chunk-timeout-sec` to give the pre-warm scan more time
- Reduce `flash.migration-bandwidth-mbps` to leave more NVMe headroom for the scan
  itself (counterintuitive but helps if NVMe is saturated)
- Check if oversized keys are present: if `flash.migration-max-key-bytes` is being
  hit, consider raising it (at the cost of higher per-migration MIGRATE call latency)

### `flash_migration_errors` is non-zero

Each `EXPORT_ABORTED` or `IMPORT_ABORTED` event increments this counter. Check
the Valkey log (`LOG flash:`) for the associated `slot migration export aborted`
message and the surrounding context (network partition, OOM, etc.).

### Node logs `flash: cluster: EXPORT_STARTED scan timed out`

The keyspace scan for Cold keys did not complete within
`flash.migration-chunk-timeout-sec`. Cold keys not reached during the scan are
still migrated correctly — they are read from NVMe on demand during the MIGRATE
transfer. To eliminate the timeout, increase the timeout or reduce the cold
keyspace size per slot.

### Replica not serving Flash keys after promotion

If the promoted replica was running with `flash.replica-tier-enabled=false`, its
NVMe backend opens on promotion (`REPLICAOF NO ONE`), not deferred to the first
write. Cold keys from the old primary are not immediately in the new primary's hot
cache. Warm the cache by reading keys or waiting for traffic. For instant post-promotion availability of Cold keys, enable
`flash.replica-tier-enabled=true` before the next failover.

---

## Full configuration reference

| Knob | Default | Mutable | Description |
|---|---|---|---|
| `flash.path` | `/tmp/valkey-flash.bin` | No | Backing file path — **must be unique per node** |
| `flash.capacity-bytes` | 1 GiB | No | NVMe backing file size |
| `flash.cache-size-bytes` | 256 MiB | **Yes** | In-memory W-TinyLFU cache size |
| `flash.sync` | `everysec` | **Yes** | WAL flush policy: `always`, `everysec`, `no` |
| `flash.io-threads` | 0 (auto) | No | Async I/O thread count; 0 = `num_cpus` |
| `flash.io-uring-entries` | 256 | No | io_uring submission queue depth |
| `flash.compaction-interval-sec` | 60 | **Yes** | Background compaction frequency |
| `flash.cluster-mode-enabled` | `auto` | No | `auto` \| `yes` \| `no`; `auto` reads `ContextFlags::CLUSTER` |
| `flash.replica-tier-enabled` | `false` | No | Open NVMe tier on replica nodes |
| `flash.migration-max-key-bytes` | 64 MiB | No | Skip Cold keys above this size during pre-warm |
| `flash.migration-bandwidth-mbps` | 100 | **Yes** | Pre-warm NVMe read cap; 0 = unlimited |
| `flash.migration-chunk-timeout-sec` | 30 | **Yes** | Wall-clock budget for pre-warm scan |
| `flash.migration-probe-cache-sec` | 60 | No | TTL for `FLASH.MIGRATE.PROBE` result cache; 0 = no cache |
