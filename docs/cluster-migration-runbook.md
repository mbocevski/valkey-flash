# valkey-flash cluster migration runbook

Step-by-step guide for resharding a live valkey-flash cluster. For deployment
context (sizing, unique `flash.path`, replica topologies) see
[docs/cluster.md](cluster.md).

## Contents

- [Pre-flight checks](#pre-flight-checks)
- [Bandwidth tuning](#bandwidth-tuning)
- [Starting a migration](#starting-a-migration)
- [Monitoring progress](#monitoring-progress)
- [Handling in-flight issues](#handling-in-flight-issues)
- [Post-migration verification](#post-migration-verification)
- [Known error patterns](#known-error-patterns)
- [End-to-end walkthrough](#end-to-end-walkthrough)

---

## Pre-flight checks

Run these on **both source and target nodes** before starting any reshard. A
missed check is much cheaper to fix now than mid-migration.

### 1. Flash module loaded on both nodes

```sh
# On source
valkey-cli -h <source-host> -p <source-port> MODULE LIST

# On target
valkey-cli -h <target-host> -p <target-port> MODULE LIST
```

Both should list a module with name `flash`. If the target is missing it, load
the module before proceeding (see [Known error patterns](#known-error-patterns)).

### 2. Flash module ready on both nodes

```sh
valkey-cli -h <source-host> -p <source-port> INFO flash | grep flash_module_state
valkey-cli -h <target-host> -p <target-port> INFO flash | grep flash_module_state
```

Expected: `flash_module_state:ready`. A value of `error` means the module failed
to initialize its NVMe backend — check the server log before continuing.

### 3. Target has enough NVMe capacity

```sh
# Get total capacity and current usage on target
valkey-cli -h <target-host> -p <target-port> INFO flash | grep -E "flash_storage_(capacity|used)_bytes"

# Or probe directly from any cluster node
valkey-cli -h <any-node> -p <any-port> FLASH.MIGRATE.PROBE <target-host> <target-port>
```

The PROBE response includes `capacity_bytes` and `free_bytes`. Ensure
`free_bytes` exceeds the estimated size of the slots you are migrating with
comfortable headroom (20%+ recommended).

### 4. flash.path is unique on every node

```sh
# Collect paths from all primaries (and replicas if replica-tier-enabled=true)
for node in <node1> <node2> <node3>; do
  echo -n "$node: "
  valkey-cli -h $node -p 6379 CONFIG GET flash.path
done
```

Each node must have a distinct path. Two nodes sharing a path will silently
corrupt each other's NVMe tier. See [cluster.md: Critical constraint](cluster.md#critical-constraint-unique-flashpath-per-node).

### 5. Cluster is healthy and source owns the target slots

```sh
valkey-cli -h <any-node> -p <any-port> CLUSTER INFO | grep cluster_state
# Expected: cluster_state:ok

valkey-cli -h <any-node> -p <any-port> CLUSTER NODES
# Verify source node ID owns the slots you plan to move
# Verify target node is in 'master' state with no ongoing migrations
```

Do not start a reshard if `cluster_state` is `fail` or if any node shows
`(in migration)` in its slot assignment.

---

## Bandwidth tuning

Before starting, cap the pre-warm NVMe read throughput so the migration does not
saturate your network and starve client traffic:

```sh
# Recommended: 10% of inter-node link bandwidth
# Example: 1 Gbps link → 100 Mbps (the default); 10 Gbps link → 1000 Mbps
CONFIG SET flash.migration-bandwidth-mbps 100
```

The default (100 Mbps) is conservative for a 1 Gbps link. Raise it during
maintenance windows when client traffic is low; lower it if you observe
elevated latency on client-facing commands during the migration.

`0` disables throttling entirely — use only when migrating in an offline
maintenance window with no client traffic.

This setting takes effect **on the next key promotion cycle**, so you can
adjust it live mid-migration if needed.

---

## Starting a migration

valkey-flash integrates transparently with Valkey's standard resharding protocol.
No special migration commands are needed — use the normal tooling:

```sh
# Reshard 100 slots from source to target
valkey-cli --cluster reshard <any-node>:<any-port> \
  --cluster-from <source-node-id> \
  --cluster-to   <target-node-id> \
  --cluster-slots 100 \
  --cluster-yes
```

When slot export begins on the source, valkey-flash automatically:
1. Scans the keyspace for Cold-tier FLASH keys in the migrating slots
2. Pre-warms them (NVMe → RAM, rate-limited by `flash.migration-bandwidth-mbps`)
3. Transfers all keys (Hot and pre-warmed Cold) via Valkey's normal MIGRATE mechanism

Keys too large for pre-warm (> `flash.migration-max-key-bytes`, default 64 MiB)
are still migrated correctly — their Cold-tier data is read from NVMe on demand
during the MIGRATE call itself, adding latency proportional to key size.

---

## Monitoring progress

### INFO flash fields

Poll these on the **source node** during an active migration:

```sh
watch -n 2 'valkey-cli -h <source-host> -p <source-port> INFO flash | grep flash_migration'
```

| Field | What it means |
|---|---|
| `flash_migration_slots_in_progress` | Slots currently being exported or imported |
| `flash_migration_bytes_sent` | Cumulative bytes pre-warmed (NVMe→RAM) on this node |
| `flash_migration_keys_migrated` | Cumulative keys successfully pre-warmed |
| `flash_migration_keys_rejected` | Keys skipped (oversized or pre-warm timed out) |
| `flash_migration_last_duration_ms` | Wall-clock time of the most recent completed export |
| `flash_migration_errors` | Cumulative EXPORT_ABORTED + IMPORT_ABORTED events |
| `flash_migration_bandwidth_mbps` | Current configured bandwidth cap |

`flash_migration_bytes_received` is always 0 in v1 (import-side tracking not yet implemented).

### Poll all nodes at once

```sh
#!/bin/sh
# poll-migration.sh — run from any machine with valkey-cli in PATH
NODES="primary-0:6379 primary-1:6379 primary-2:6379"
while true; do
  echo "=== $(date -u +%H:%M:%S) ==="
  for node in $NODES; do
    host="${node%:*}"
    port="${node#*:}"
    echo "--- $node ---"
    valkey-cli -h "$host" -p "$port" INFO flash 2>/dev/null \
      | grep "flash_migration\|flash_module_state"
  done
  sleep 5
done
```

A healthy migration shows `flash_migration_slots_in_progress` decrementing over
time and `flash_migration_errors` staying at 0.

---

## Handling in-flight issues

### Migration appears stalled

Check `flash_migration_keys_rejected` — if it is climbing without
`flash_migration_keys_migrated` increasing, the pre-warm is hitting its timeout:

```sh
# Extend the pre-warm window (mutable; takes effect on next export)
CONFIG SET flash.migration-chunk-timeout-sec 120

# If NVMe is saturated, paradoxically lower the bandwidth cap to reduce
# queue depth and let the scan proceed faster
CONFIG SET flash.migration-bandwidth-mbps 50
```

Also check `flash_migration_errors`. If non-zero, look in the Valkey server log
for `flash: cluster: slot migration export aborted` with surrounding context
(network partition, OOM, disk full).

### Abort a stuck migration

If a slot migration is stuck and needs to be cancelled:

```sh
# On the source node — revert the slot to STABLE ownership on source
valkey-cli -h <source-host> -p <source-port> \
  CLUSTER SETSLOT <slot> STABLE

# On the target node — clear the IMPORTING state
valkey-cli -h <target-host> -p <target-port> \
  CLUSTER SETSLOT <slot> STABLE
```

After stabilizing, verify cluster state:

```sh
valkey-cli -h <any-node> -p <any-port> CLUSTER INFO | grep cluster_state
valkey-cli -h <any-node> -p <any-port> CLUSTER NODES | grep -v noaddr
```

### Target runs out of NVMe space mid-reshard

If the target's flash.capacity is exhausted during migration:

1. **Stop the reshard immediately** — abort as above with `CLUSTER SETSLOT STABLE`
2. **Do not restart** the migration to the same target until capacity is freed
3. Check `INFO flash | grep flash_storage` on the target to confirm how much is used
4. Options:
   - Delete cold keys on target to reclaim space, then trigger compaction:
     `FLASH.COMPACTION.TRIGGER` and wait for `FLASH.COMPACTION.STATS` to show reclaimed bytes
   - Redirect the migration to a different target with sufficient free space
   - Increase `flash.capacity-bytes` — but note this is **immutable after module load**,
     so it requires a restart of the target node with a larger value

---

## Post-migration verification

After `valkey-cli --cluster reshard` completes:

### 1. Confirm slot ownership transferred

```sh
# Source should show 0 keys in the migrated slots
valkey-cli -h <source-host> -p <source-port> CLUSTER COUNTKEYSINSLOT <slot>
# Expected: 0

# Target should show > 0 (or 0 if the slot was empty — that is fine)
valkey-cli -h <target-host> -p <target-port> CLUSTER COUNTKEYSINSLOT <slot>
```

### 2. CLUSTER NODES shows new ownership

```sh
valkey-cli -h <any-node> -p <any-port> CLUSTER NODES | grep <target-node-id>
# Target node should list the migrated slot ranges
```

### 3. Sample FLASH key reads from target

Pick a few known FLASH keys that mapped to the migrated slots and read them
directly from the target node (bypassing redirect) to confirm data arrived:

```sh
# Read directly without redirect (-c disables cluster follow mode)
valkey-cli -h <target-host> -p <target-port> FLASH.GET <key-in-migrated-slot>
```

### 4. No migration errors

```sh
valkey-cli -h <source-host> -p <source-port> INFO flash | grep flash_migration_errors
# Expected: flash_migration_errors:0 (or same value as before the migration started)
```

---

## Known error patterns

### `ERR FLASH-MIGRATE target <addr> does not have flash-module loaded`

Returned by `FLASH.MIGRATE.PROBE` when the target responds to TCP connection
but `FLASH.MIGRATE.PROBE` returns `ERR unknown command` — the module is not
loaded on the target.

**Fix**: load the module on the target node before starting the reshard:

```sh
valkey-cli -h <target-host> -p <target-port> \
  MODULE LOAD /path/to/libvalkey_flash.so \
  flash.path /data/flash.bin \
  flash.capacity-bytes 10737418240
```

Or add it to the target's `valkey.conf` and restart.

### `ERR FLASH-MIGRATE target <addr> did not respond within timeout`

Returned by `FLASH.MIGRATE.PROBE` when the TCP connection or response takes
longer than 5 seconds.

**Fix**:
- Verify network connectivity: `ping <target>`, `telnet <target-host> <target-port>`
- Verify the target is running and healthy: `valkey-cli -h <target> PING`
- Check firewall rules allow inter-node traffic on the Valkey port

### Oversized keys skipped (not a client error)

When a Cold-tier key exceeds `flash.migration-max-key-bytes` (default 64 MiB),
it is silently skipped during pre-warm and logged to the server log:

```
flash: cluster: migration skipping oversized key (N bytes > max 67108864 bytes)
```

This increments `flash_migration_keys_rejected` in `INFO flash`. The key is
**still migrated correctly** via the normal MIGRATE path — it just takes longer
because the Cold-tier data is read from NVMe on demand during the MIGRATE call.

If this is causing unacceptable latency spikes:
```sh
# Raise the pre-warm size limit (immutable — requires module reload)
# Add to valkey.conf: flash.migration-max-key-bytes 134217728  (128 MiB)
```

Or manually promote oversized Cold keys to Hot before starting the migration:
```sh
FLASH.GET <key>   # read triggers hot promotion (Cold → Hot)
```

### `ERR FLASH-MIGRATE target <addr> invalid address`

The host or port argument to `FLASH.MIGRATE.PROBE` could not be parsed.

**Fix**: verify the invocation — host and port are separate arguments:
```sh
FLASH.MIGRATE.PROBE 192.168.1.10 6380   # correct
```

---

## End-to-end walkthrough

This example reshards slots 0–1000 from `primary-0` (port 6379) to `primary-2`
(port 6381) in a three-node cluster.

```sh
# ── Step 1: Pre-flight ────────────────────────────────────────────────────────

# Confirm flash loaded and ready on both nodes
valkey-cli -p 6379 INFO flash | grep flash_module_state
valkey-cli -p 6381 INFO flash | grep flash_module_state

# Probe target capacity
valkey-cli -p 6379 FLASH.MIGRATE.PROBE 127.0.0.1 6381
# Look for free_bytes > estimated slot size

# Confirm cluster is healthy
valkey-cli -p 6379 CLUSTER INFO | grep cluster_state

# Note source and target node IDs
SOURCE=$(valkey-cli -p 6379 CLUSTER MYID)
TARGET=$(valkey-cli -p 6381 CLUSTER MYID)
echo "source=$SOURCE  target=$TARGET"

# ── Step 2: Tune bandwidth ────────────────────────────────────────────────────

# Cap pre-warm at 100 Mbps (default — adjust to 10% of your link bandwidth)
valkey-cli -p 6379 CONFIG SET flash.migration-bandwidth-mbps 100

# ── Step 3: Start migration ───────────────────────────────────────────────────

valkey-cli --cluster reshard 127.0.0.1:6379 \
  --cluster-from "$SOURCE" \
  --cluster-to   "$TARGET" \
  --cluster-slots 1001 \
  --cluster-yes

# ── Step 4: Monitor in a second terminal ─────────────────────────────────────

watch -n 3 'valkey-cli -p 6379 INFO flash | grep flash_migration'
# flash_migration_slots_in_progress:1 → 0 when done
# flash_migration_keys_migrated:N — rising
# flash_migration_errors:0 — should stay 0

# ── Step 5: Post-migration verification ──────────────────────────────────────

# Source should have 0 keys in slot 0
valkey-cli -p 6379 CLUSTER COUNTKEYSINSLOT 0   # → 0

# Target should own slots 0–1000
valkey-cli -p 6381 CLUSTER NODES | grep "$TARGET" | grep -o '[0-9]*-[0-9]*'

# Sample a known FLASH key
valkey-cli -p 6381 FLASH.GET mykey              # direct read, no redirect

# Confirm no errors accumulated
valkey-cli -p 6379 INFO flash | grep flash_migration_errors  # → 0
```

---

## Quick-reference: useful commands

```sh
# Live migration status on source
valkey-cli INFO flash | grep flash_migration

# Probe target before migrating
valkey-cli FLASH.MIGRATE.PROBE <host> <port>

# Tune bandwidth live
CONFIG SET flash.migration-bandwidth-mbps <n>

# Extend pre-warm timeout
CONFIG SET flash.migration-chunk-timeout-sec <n>

# Abort stuck migration
CLUSTER SETSLOT <slot> STABLE   # run on both source and target

# Trigger NVMe compaction to reclaim space
FLASH.COMPACTION.TRIGGER

# Check compaction progress
FLASH.COMPACTION.STATS
```
