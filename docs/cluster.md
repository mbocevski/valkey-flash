# valkey-flash cluster deployment

> **Status**: placeholder — full deployment guide lands in task #87.

## Per-key atomicity and migration size limits

valkey-flash v1 migrates each key as a single atomic unit.  Before a slot
export begins the module pre-warms Cold (NVMe-resident) keys in the migrating
slots back into RAM so Valkey can transfer them in one pass.

**Size limit**: keys whose serialised value exceeds `flash.migration-max-key-bytes`
(default 64 MiB) are skipped during pre-warm.  They are not excluded from
migration itself — Valkey will still attempt to transfer them — but their
Cold-tier data will not be promoted first, so the migrating node must perform a
synchronous NVMe read during the actual transfer, adding latency proportional to
the key size.

**v1.1 note**: a chunked streaming protocol (`FLASH.MIGRATE.CHUNK`) is planned
for v1.1.  Once available, keys larger than `flash.migration-max-key-bytes` will
be streamed in chunks rather than rejected from pre-warm.  Until then, keep
individual flash values under 64 MiB for latency-sensitive migrations.

## Bandwidth throttling

The pre-warm phase is rate-limited by `flash.migration-bandwidth-mbps`
(default 100 Mbps).  Set it to `0` to disable throttling entirely:

```
CONFIG SET flash.migration-bandwidth-mbps 0   # unlimited
CONFIG SET flash.migration-bandwidth-mbps 50  # 50 Mbps
```

The limit applies to NVMe-to-RAM promotion throughput, not to network transfer
between cluster nodes.

## Configuration reference

| Knob | Default | Mutable | Notes |
|---|---|---|---|
| `flash.migration-max-key-bytes` | 67108864 (64 MiB) | No | Keys above this size are skipped during pre-warm |
| `flash.migration-bandwidth-mbps` | 100 | Yes | 0 = unlimited |
| `flash.migration-chunk-timeout-sec` | 30 | Yes | Per-migration timeout for the pre-warm scan |
| `flash.migration-probe-cache-sec` | 60 | No | TTL for `FLASH.MIGRATE.PROBE` result cache |
