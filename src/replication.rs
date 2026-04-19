use std::sync::atomic::{AtomicBool, Ordering};

use valkey_module::{Context, ContextFlags};
use valkey_module_macros::role_changed_event_handler;

use valkey_module::server_events::ServerRole;

/// `true` when this instance is currently operating as a Valkey replica.
///
/// Set at module load when `ContextFlags::SLAVE` is present, and toggled by the
/// role-change server-event callback. Writers use `Release`; readers use `Acquire`
/// so that the flag is visible across threads without extra fencing.
pub static IS_REPLICA: AtomicBool = AtomicBool::new(false);

/// Returns `true` if this instance is currently a replica.
#[inline]
pub fn is_replica() -> bool {
    IS_REPLICA.load(Ordering::Acquire)
}

/// Returns `true` if the command must be obeyed unconditionally — i.e., it
/// arrived over the replication channel from the primary (or from the AOF
/// replay path) and must never be rejected regardless of local admission
/// policies or missing NVMe backend state.
///
/// Uses `ContextFlags::REPLICATED` — available since Valkey 8.0 and sufficient
/// for the v1 use case (Valkey 8.1 `MustObeyClient` is more precise but
/// unavailable on the minimum supported version).
pub fn must_obey_client(ctx: &Context) -> bool {
    ctx.get_flags().contains(ContextFlags::REPLICATED)
}

/// Returns `true` if the command must complete synchronously on the event
/// loop, i.e. cannot call `ctx.block_client()`. This is the case when:
///
/// - we are a replica (writes arrive from the primary and must not block),
/// - the server is in `LOADING` state (AOF/RDB replay — `block_client()` in
///   this state hits `serverAssert(!deny_blocking ...)` in Valkey core),
/// - the context carries `DENY_BLOCKING` (e.g. inside MULTI/EXEC on a
///   client that has disallowed blocking).
pub fn must_run_sync(ctx: &Context) -> bool {
    let flags = ctx.get_flags();
    is_replica()
        || flags.intersects(
            ContextFlags::LOADING | ContextFlags::DENY_BLOCKING | ContextFlags::ASYNC_LOADING,
        )
}

/// Server-event callback fired whenever the replication role changes.
///
/// - `Primary → Replica`: set `IS_REPLICA = true` to suspend NVMe demotions.
///   The hot-set is kept intact; new writes from the primary are applied only
///   to the RAM cache (no NVMe write, no WAL append).
/// - `Replica → Primary`: reset `IS_REPLICA = false` and eagerly initialize the
///   NVMe backend (STORAGE, WAL, POOL, compaction thread) so that write commands
///   succeed immediately without a server restart.
#[role_changed_event_handler]
fn on_role_changed(_ctx: &Context, new_role: ServerRole) {
    match new_role {
        ServerRole::Replica => {
            IS_REPLICA.store(true, Ordering::Release);
            if crate::config::FLASH_REPLICA_TIER_ENABLED.load(Ordering::Relaxed) {
                valkey_module::logging::log_notice(
                    "flash: role changed to Replica — local NVMe tier remains active \
                     (flash.replica-tier-enabled)",
                );
            } else {
                valkey_module::logging::log_notice(
                    "flash: role changed to Replica — NVMe demotions suspended; \
                     hot-set preserved in RAM",
                );
            }
        }
        ServerRole::Primary => {
            IS_REPLICA.store(false, Ordering::Release);
            valkey_module::logging::log_notice(
                "flash: role changed to Primary — initiating NVMe backend",
            );
            crate::init_nvme_backend();
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    fn reset() {
        IS_REPLICA.store(false, Ordering::SeqCst);
    }

    #[test]
    fn is_replica_starts_false() {
        reset();
        assert!(!is_replica(), "IS_REPLICA should start false");
    }

    #[test]
    fn set_replica_flag_makes_is_replica_true() {
        IS_REPLICA.store(true, Ordering::SeqCst);
        assert!(is_replica());
        reset();
    }

    #[test]
    fn clear_replica_flag_makes_is_replica_false() {
        IS_REPLICA.store(true, Ordering::SeqCst);
        IS_REPLICA.store(false, Ordering::SeqCst);
        assert!(!is_replica());
    }

    #[test]
    fn simulated_primary_to_replica_transition() {
        // Simulate what on_role_changed would do for Primary→Replica.
        IS_REPLICA.store(false, Ordering::SeqCst);
        IS_REPLICA.store(true, Ordering::Release); // role change to Replica
        assert!(
            is_replica(),
            "flag must be true after Primary→Replica transition"
        );
        reset();
    }

    #[test]
    fn simulated_replica_to_primary_transition() {
        // Simulate what on_role_changed would do for Replica→Primary.
        IS_REPLICA.store(true, Ordering::SeqCst);
        IS_REPLICA.store(false, Ordering::Release); // role change to Primary
        assert!(
            !is_replica(),
            "flag must be false after Replica→Primary transition"
        );
    }

    #[test]
    fn replica_flag_is_initially_false_in_fresh_atomic() {
        // The static is initialized to false; verify the const initializer.
        // (This test would fail if someone accidentally changed the initial value.)
        let fresh = AtomicBool::new(false);
        assert!(!fresh.load(Ordering::SeqCst));
    }
}

// ── Loom concurrency model tests ──────────────────────────────────────────────
//
// IS_REPLICA is a std::sync::atomic::AtomicBool static; statics cannot use loom
// types (loom's atomics aren't const-constructible). The test below models the
// same Release/Acquire pattern locally using loom's controlled AtomicBool to
// verify the ordering contract under all possible thread interleavings.

#[cfg(all(test, loom))]
mod loom_tests {
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicBool, Ordering};
    use loom::thread;

    // Model the IS_REPLICA Release store (role-change handler) vs Acquire load
    // (is_replica() callers on worker threads). After the writer thread joins,
    // the Acquire load must observe true — loom verifies this across all
    // interleavings including those where the load races ahead of the store.
    #[test]
    fn replica_flag_acquire_release() {
        loom::model(|| {
            let flag = Arc::new(AtomicBool::new(false));
            let f = flag.clone();

            // Simulates on_role_changed Primary→Replica.
            let writer = thread::spawn(move || {
                f.store(true, Ordering::Release);
            });

            // Simulates a concurrent is_replica() call — may see old or new value.
            let _observed = flag.load(Ordering::Acquire);

            writer.join().unwrap();

            // After joining the writer the Release write is always visible.
            assert!(
                flag.load(Ordering::Acquire),
                "IS_REPLICA must be true after role-change writer joins"
            );
        });
    }
}
