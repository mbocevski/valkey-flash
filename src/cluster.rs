use std::sync::atomic::{AtomicBool, Ordering};
use valkey_module::{logging, raw, Context};

/// `true` when this instance is running in Valkey cluster mode.
///
/// Set at module load from the `flash.cluster-mode-enabled` config knob (default
/// `"auto"`: detected from `ContextFlags::CLUSTER`). Writers use `Release`;
/// readers use `Acquire` so the flag is visible across threads.
pub static IS_CLUSTER: AtomicBool = AtomicBool::new(false);

#[inline]
pub fn is_cluster() -> bool {
    IS_CLUSTER.load(Ordering::Acquire)
}

/// Subscribe a stub handler for `ValkeyModuleEvent_AtomicSlotMigration` (event 19).
///
/// The stub logs receipt; real slot-migration behavior is wired in task #78.
/// Uses the raw subscription API because the crate's bundled header predates
/// event ID 19 (introduced in Valkey 9.0).
pub(crate) fn subscribe_cluster_events(ctx: &Context) {
    // VALKEYMODULE_EVENT_ATOMIC_SLOT_MIGRATION = 19 (Valkey >= 9.0).
    const EVENT_ATOMIC_SLOT_MIGRATION: u64 = 19;
    let status = raw::subscribe_to_server_event(
        ctx.ctx,
        raw::RedisModuleEvent {
            id: EVENT_ATOMIC_SLOT_MIGRATION,
            dataver: 1,
        },
        Some(on_slot_migration),
    );
    if status != raw::Status::Ok {
        logging::log_warning("flash: cluster: failed to subscribe to AtomicSlotMigration events");
    }
}

extern "C" fn on_slot_migration(
    _ctx: *mut raw::RedisModuleCtx,
    _eid: raw::RedisModuleEvent,
    subevent: u64,
    _data: *mut ::std::os::raw::c_void,
) {
    // Stub — real slot-migration logic is wired in task #78.
    logging::log_notice(format!(
        "flash: cluster: slot migration event (subevent={subevent}) — handler stub (#78)"
    ));
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn reset() {
        IS_CLUSTER.store(false, Ordering::SeqCst);
    }

    #[test]
    fn is_cluster_starts_false() {
        reset();
        assert!(!is_cluster());
    }

    #[test]
    fn is_cluster_true_after_store() {
        IS_CLUSTER.store(true, Ordering::SeqCst);
        assert!(is_cluster());
        reset();
    }

    #[test]
    fn is_cluster_false_after_clear() {
        IS_CLUSTER.store(true, Ordering::SeqCst);
        IS_CLUSTER.store(false, Ordering::SeqCst);
        assert!(!is_cluster());
    }
}
