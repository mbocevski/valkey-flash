//! Server-event handlers for the persistence lifecycle.
//!
//! Valkey fires `REDISMODULE_EVENT_PERSISTENCE` with different subevents for
//! each stage of RDB/AOF save. We use the start subevents (`RdbStart`,
//! `SyncRdbStart`, `AofStart`, `SyncAofStart`) to **drain the pending
//! async-demotion commit queue on the parent process, BEFORE the fork**.
//!
//! Why this handler exists (historical):
//!
//! v1.1 originally ran the drain from inside `aux_save` with
//! `when == AUX_BEFORE_RDB`. That looked right on the surface — it fires
//! "before" the per-key save loop — but Valkey invokes `aux_save` inside
//! the forked BGSAVE child, not on the parent. The drain path calls
//! `commit_one()` → `ctx.open_key_writable(&key)`, which mutates the
//! keyspace. Calling keyspace-mutating module APIs inside a forked child
//! is undefined behaviour; Valkey 9.0 SIGSEGVs reliably once the child
//! touches the inherited hash-table.
//!
//! Symptom: BGSAVE child crashed with signal 11 inside `libvalkey_flash.so`
//! whenever a non-trivial load (50k+ keys) triggered the `save` threshold.
//! Fixed in v1.1.1 by moving the drain to this handler, which runs on the
//! parent process before fork.

use valkey_module::Context;
use valkey_module::server_events::PersistenceSubevent;
use valkey_module_macros::persistence_event_handler;

#[persistence_event_handler]
pub fn persistence_event_handler(ctx: &Context, event: PersistenceSubevent) {
    match event {
        // Drain any pending async-demotion commits before the RDB snapshot
        // captures a view of the keyspace and the NVMe allocator. Phase 2
        // of the demotion pipeline writes blocks to NVMe; phase 3 flips
        // the key to `Tier::Cold` and points it at those blocks. If the
        // snapshot runs between phase 2 completion and phase 3 commit,
        // the RDB would record a key as still `Hot` while its allocated
        // blocks have no `Tier::Cold` owner — on crash-recovery those
        // blocks would leak until a compaction pass noticed. Draining
        // the commit queue here closes that window.
        //
        // `RdbStart` and `SyncRdbStart` fire on the parent process just
        // before the fork / synchronous save begins, which is exactly
        // the window we need. `AofStart` and `SyncAofStart` are included
        // for the AOF-rewrite path, which has the same invariant.
        PersistenceSubevent::RdbStart
        | PersistenceSubevent::SyncRdbStart
        | PersistenceSubevent::AofStart
        | PersistenceSubevent::SyncAofStart => {
            crate::demotion::drain_for_persistence(ctx);
        }
        PersistenceSubevent::Ended | PersistenceSubevent::Failed => {
            // Nothing to do. `aux_save` / `aux_load` on the save-path
            // handle the actual metadata; this event is informational.
        }
    }
}
