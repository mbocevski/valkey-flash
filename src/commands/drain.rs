//! `FLASH.DRAIN [MATCH pattern] [COUNT n] [FORCE]` — scans the keyspace and
//! converts every matching FLASH.* key to its native counterpart via
//! `FLASH.CONVERT`. The entry point for operators preparing to
//! `MODULE UNLOAD flash`: Valkey refuses unload while custom-type keys exist,
//! so DRAIN is run first to clear the flash tier.
//!
//! Semantics:
//!   - Without options: one synchronous sweep, returns summary counts.
//!   - `MATCH <glob>` filters candidate keys by name (same glob syntax as SCAN).
//!   - `COUNT <n>` caps converted keys per invocation (operators script the loop).
//!   - `FORCE` bypasses the headroom guard which otherwise refuses when
//!     projected post-conversion RAM exceeds `maxmemory`.
//!
//! Reply shape: array `[converted, skipped, errors, scanned]` so callers get a
//! structured breakdown without parsing strings.

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use valkey_module::{
    Context, KeysCursor, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue, logging,
};

use crate::commands::convert::{self, CONVERT_TOTAL};
use crate::commands::zset_common::glob_match;

// ── Observable state ──────────────────────────────────────────────────────────

/// Set while a drain is in flight. Visible via `INFO flash` as
/// `flash_drain_in_progress`.
pub static DRAIN_IN_PROGRESS: AtomicBool = AtomicBool::new(false);
/// Outcome counters from the last completed DRAIN. Useful for post-mortem
/// inspection via `INFO flash` after a drain returns.
pub static DRAIN_LAST_CONVERTED: AtomicU64 = AtomicU64::new(0);
pub static DRAIN_LAST_SKIPPED: AtomicU64 = AtomicU64::new(0);
pub static DRAIN_LAST_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static DRAIN_LAST_SCANNED: AtomicU64 = AtomicU64::new(0);

// ── Parsing ───────────────────────────────────────────────────────────────────

#[cfg_attr(test, derive(Debug))]
struct Opts {
    pattern: Option<Vec<u8>>,
    count: Option<u64>,
    force: bool,
}

fn parse_opts(args: &[ValkeyString]) -> Result<Opts, ValkeyError> {
    let bytes: Vec<&[u8]> = args.iter().map(ValkeyString::as_slice).collect();
    parse_opts_bytes(&bytes)
}

/// Byte-slice variant of [`parse_opts`] — split out so unit tests can exercise
/// the parser without a live Valkey context.
fn parse_opts_bytes(args: &[&[u8]]) -> Result<Opts, ValkeyError> {
    let mut pattern: Option<Vec<u8>> = None;
    let mut count: Option<u64> = None;
    let mut force = false;
    let mut i = 1usize;

    while i < args.len() {
        let tok = args[i];
        if tok.eq_ignore_ascii_case(b"MATCH") {
            if i + 1 >= args.len() {
                return Err(ValkeyError::WrongArity);
            }
            pattern = Some(args[i + 1].to_vec());
            i += 2;
        } else if tok.eq_ignore_ascii_case(b"COUNT") {
            if i + 1 >= args.len() {
                return Err(ValkeyError::WrongArity);
            }
            let raw = std::str::from_utf8(args[i + 1]).map_err(|_| {
                ValkeyError::Str("ERR FLASH.DRAIN: COUNT must be a non-negative integer")
            })?;
            let n: u64 = raw.parse().map_err(|_| {
                ValkeyError::Str("ERR FLASH.DRAIN: COUNT must be a non-negative integer")
            })?;
            count = Some(n);
            i += 2;
        } else if tok.eq_ignore_ascii_case(b"FORCE") {
            force = true;
            i += 1;
        } else {
            return Err(ValkeyError::Str("ERR FLASH.DRAIN: unknown option"));
        }
    }

    Ok(Opts {
        pattern,
        count,
        force,
    })
}

// ── Command handler ───────────────────────────────────────────────────────────

pub fn flash_drain_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let opts = parse_opts(&args)?;

    // Defensive: FLASH.DRAIN runs on the main event-loop thread, so nothing else
    // can execute concurrently. The flag is primarily an observability signal.
    if DRAIN_IN_PROGRESS.swap(true, Ordering::AcqRel) {
        return Err(ValkeyError::Str(
            "ERR FLASH.DRAIN: drain already in progress",
        ));
    }

    let result = run_drain(ctx, opts);

    DRAIN_IN_PROGRESS.store(false, Ordering::Release);

    result
}

// ── Drain loop ────────────────────────────────────────────────────────────────

fn run_drain(ctx: &Context, opts: Opts) -> ValkeyResult {
    if !opts.force
        && let Err(e) = check_headroom(ctx)
    {
        return Err(e);
    }

    // Pass 1 — SCAN the keyspace, collect names matching the pattern.
    // We intentionally defer the FLASH-type check to pass 2: the scan callback
    // receives `Option<&ValkeyKey>` but inspecting the type here inside the
    // scan would hold the internal key lock longer than needed and conflict
    // with pass-2 mutations. Filtering in pass 2 also gracefully handles keys
    // that evaporate between passes.
    let candidates = scan_keys(ctx, opts.pattern.as_deref());

    // Pass 2 — convert each candidate. Stop at COUNT.
    let limit = opts.count.unwrap_or(u64::MAX);
    let mut converted: u64 = 0;
    let mut skipped: u64 = 0;
    let mut errors: u64 = 0;
    let mut scanned: u64 = 0;

    for name_bytes in candidates {
        if converted >= limit {
            break;
        }
        scanned += 1;
        let key = ctx.create_string(name_bytes);

        match convert::extract_payload(ctx, &key) {
            Ok(None) => {
                // Key vanished, is native, or is another module's type — all
                // valid "nothing to do" cases.
                skipped += 1;
            }
            Ok(Some(payload)) => match convert::apply_conversion(ctx, &key, payload) {
                Ok(()) => {
                    converted += 1;
                    CONVERT_TOTAL.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    errors += 1;
                    logging::log_warning(
                        format!(
                            "flash: FLASH.DRAIN: convert failed on key {}: {}",
                            String::from_utf8_lossy(key.as_slice()),
                            e
                        )
                        .as_str(),
                    );
                }
            },
            Err(e) => {
                errors += 1;
                logging::log_warning(
                    format!(
                        "flash: FLASH.DRAIN: extract failed on key {}: {}",
                        String::from_utf8_lossy(key.as_slice()),
                        e
                    )
                    .as_str(),
                );
            }
        }
    }

    // Publish summary so operators can correlate DRAIN outcomes via INFO flash
    // after the command returns.
    DRAIN_LAST_CONVERTED.store(converted, Ordering::Relaxed);
    DRAIN_LAST_SKIPPED.store(skipped, Ordering::Relaxed);
    DRAIN_LAST_ERRORS.store(errors, Ordering::Relaxed);
    DRAIN_LAST_SCANNED.store(scanned, Ordering::Relaxed);

    Ok(ValkeyValue::Array(vec![
        ValkeyValue::Integer(converted as i64),
        ValkeyValue::Integer(skipped as i64),
        ValkeyValue::Integer(errors as i64),
        ValkeyValue::Integer(scanned as i64),
    ]))
}

fn scan_keys(ctx: &Context, pattern: Option<&[u8]>) -> Vec<Vec<u8>> {
    let cursor = KeysCursor::new();
    let collected: RefCell<Vec<Vec<u8>>> = RefCell::new(Vec::new());

    let cb = |_ctx: &Context, name: ValkeyString, _k: Option<&valkey_module::key::ValkeyKey>| {
        let bytes = name.as_slice().to_vec();
        if let Some(p) = pattern
            && !glob_match(p, &bytes)
        {
            return;
        }
        collected.borrow_mut().push(bytes);
    };

    while cursor.scan(ctx, &cb) {}
    collected.into_inner()
}

// ── Headroom guard ────────────────────────────────────────────────────────────

/// Refuse the drain if a complete Cold-tier materialisation would push
/// `used_memory` past `maxmemory`.
///
/// The guard is intentionally pessimistic: it assumes every NVMe byte will
/// become a live RAM byte, which overestimates when the operator used
/// `MATCH` to target a subset. That's the safer bias — false refusals cost
/// an operator one `FORCE` flag, a silent OOM costs a cluster.
fn check_headroom(ctx: &Context) -> Result<(), ValkeyError> {
    let Some((used, maxmem)) = read_memory_section(ctx) else {
        logging::log_warning(
            "flash: FLASH.DRAIN: could not read INFO memory; skipping headroom guard",
        );
        return Ok(());
    };
    if maxmem == 0 {
        // maxmemory=0 is the "no limit" sentinel.
        return Ok(());
    }

    let storage_used = crate::STORAGE
        .get()
        .map(|s| {
            const BLOCK: u64 = 4096;
            s.next_block_snapshot() * BLOCK
        })
        .unwrap_or(0);

    let projected = used.saturating_add(storage_used);
    if projected > maxmem {
        return Err(ValkeyError::String(format!(
            "ERR FLASH.DRAIN would exceed maxmemory \
             (used_memory={used} + projected_cold={storage_used} > maxmemory={maxmem}); \
             pass FORCE to override"
        )));
    }
    Ok(())
}

/// Fetch `(used_memory, maxmemory)` from `INFO memory` using the raw
/// unsigned-variant FFI. Returns `None` if the API is unavailable or any
/// field lookup fails.
///
/// The obvious implementation — `ctx.server_info("memory").field("used_memory")`
/// — leaks 24 bytes per call: Valkey core allocates a `RedisModuleString`
/// with refcount 1 inside `VM_ServerInfoGetField`, then the crate's
/// `ValkeyString::new` wrapper calls `RedisModule_RetainString` (bumping to
/// 2) before `Drop` calls `RedisModule_FreeString` (dropping back to 1).
/// The original refcount set by `VM_ServerInfoGetField` is never released.
/// ASAN caught this on the PR (48 bytes leaked per drain probe).
///
/// `RedisModule_ServerInfoGetFieldUnsigned` avoids the allocation entirely
/// by returning `unsigned long long` directly.
fn read_memory_section(ctx: &Context) -> Option<(u64, u64)> {
    use std::ffi::CString;

    let section = CString::new("memory").ok()?;

    // SAFETY: ctx.ctx is valid for the duration of the command invocation
    // (lifetime of &Context). `RedisModule_GetServerInfo` returns an owned
    // `RedisModuleServerInfoData *` that must be released via
    // `RedisModule_FreeServerInfo`; we do so unconditionally at the end.
    let info_data = unsafe {
        #[allow(static_mut_refs)]
        let get = valkey_module::raw::RedisModule_GetServerInfo?;
        get(ctx.ctx, section.as_ptr())
    };
    if info_data.is_null() {
        return None;
    }

    let read_u64 = |name: &str| -> Option<u64> {
        let cname = CString::new(name).ok()?;
        let mut err: std::os::raw::c_int = 0;
        // SAFETY: `info_data` is non-null and owned for the duration of this
        // closure; `cname` outlives the call. `err` is a writable local.
        let v = unsafe {
            #[allow(static_mut_refs)]
            let f = valkey_module::raw::RedisModule_ServerInfoGetFieldUnsigned?;
            f(info_data, cname.as_ptr(), &mut err)
        };
        if err != 0 { None } else { Some(v) }
    };

    let used = read_u64("used_memory");
    let maxmem = read_u64("maxmemory");

    // SAFETY: `info_data` was returned by `RedisModule_GetServerInfo` above
    // and has not been freed yet; this is the matching release call.
    unsafe {
        #[allow(static_mut_refs)]
        if let Some(free) = valkey_module::raw::RedisModule_FreeServerInfo {
            free(ctx.ctx, info_data);
        }
    }

    Some((used?, maxmem?))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drain_counters_are_readable() {
        let _ = DRAIN_IN_PROGRESS.load(Ordering::Relaxed);
        let _ = DRAIN_LAST_CONVERTED.load(Ordering::Relaxed);
        let _ = DRAIN_LAST_SKIPPED.load(Ordering::Relaxed);
        let _ = DRAIN_LAST_ERRORS.load(Ordering::Relaxed);
        let _ = DRAIN_LAST_SCANNED.load(Ordering::Relaxed);
    }

    // ── parse_opts_bytes ─────────────────────────────────────────────────────

    #[test]
    fn parse_bare_command_returns_all_defaults() {
        let opts = parse_opts_bytes(&[b"FLASH.DRAIN"]).unwrap();
        assert_eq!(opts.pattern, None);
        assert_eq!(opts.count, None);
        assert!(!opts.force);
    }

    #[test]
    fn parse_match_captures_pattern_bytes_verbatim() {
        let opts = parse_opts_bytes(&[b"FLASH.DRAIN", b"MATCH", b"user:*"]).unwrap();
        assert_eq!(opts.pattern.as_deref(), Some(b"user:*".as_ref()));
    }

    #[test]
    fn parse_match_is_case_insensitive() {
        let opts = parse_opts_bytes(&[b"FLASH.DRAIN", b"match", b"p"]).unwrap();
        assert_eq!(opts.pattern.as_deref(), Some(b"p".as_ref()));
        let opts = parse_opts_bytes(&[b"FLASH.DRAIN", b"MaTcH", b"p"]).unwrap();
        assert_eq!(opts.pattern.as_deref(), Some(b"p".as_ref()));
    }

    #[test]
    fn parse_count_accepts_u64_max() {
        let opts = parse_opts_bytes(&[b"FLASH.DRAIN", b"COUNT", b"18446744073709551615"]).unwrap();
        assert_eq!(opts.count, Some(u64::MAX));
    }

    #[test]
    fn parse_count_zero_is_accepted() {
        let opts = parse_opts_bytes(&[b"FLASH.DRAIN", b"COUNT", b"0"]).unwrap();
        assert_eq!(opts.count, Some(0));
    }

    #[test]
    fn parse_count_negative_rejected() {
        // u64::parse rejects the leading '-'.
        let err = parse_opts_bytes(&[b"FLASH.DRAIN", b"COUNT", b"-1"]).unwrap_err();
        assert!(matches!(err, ValkeyError::Str(_)));
    }

    #[test]
    fn parse_count_overflow_rejected() {
        let err =
            parse_opts_bytes(&[b"FLASH.DRAIN", b"COUNT", b"99999999999999999999"]).unwrap_err();
        assert!(matches!(err, ValkeyError::Str(_)));
    }

    #[test]
    fn parse_count_non_numeric_rejected() {
        let err = parse_opts_bytes(&[b"FLASH.DRAIN", b"COUNT", b"abc"]).unwrap_err();
        assert!(matches!(err, ValkeyError::Str(_)));
    }

    #[test]
    fn parse_count_with_non_utf8_rejected() {
        // Half a multi-byte codepoint — not valid UTF-8.
        let err = parse_opts_bytes(&[b"FLASH.DRAIN", b"COUNT", &[0xff, 0xfe][..]]).unwrap_err();
        assert!(matches!(err, ValkeyError::Str(_)));
    }

    #[test]
    fn parse_force_flag_toggles_on() {
        let opts = parse_opts_bytes(&[b"FLASH.DRAIN", b"FORCE"]).unwrap();
        assert!(opts.force);
    }

    #[test]
    fn parse_force_is_case_insensitive() {
        let opts = parse_opts_bytes(&[b"FLASH.DRAIN", b"force"]).unwrap();
        assert!(opts.force);
    }

    #[test]
    fn parse_match_without_value_is_wrong_arity() {
        let err = parse_opts_bytes(&[b"FLASH.DRAIN", b"MATCH"]).unwrap_err();
        assert!(matches!(err, ValkeyError::WrongArity));
    }

    #[test]
    fn parse_count_without_value_is_wrong_arity() {
        let err = parse_opts_bytes(&[b"FLASH.DRAIN", b"COUNT"]).unwrap_err();
        assert!(matches!(err, ValkeyError::WrongArity));
    }

    #[test]
    fn parse_unknown_option_rejected() {
        let err = parse_opts_bytes(&[b"FLASH.DRAIN", b"ZOMBIES"]).unwrap_err();
        assert!(matches!(err, ValkeyError::Str(_)));
    }

    #[test]
    fn parse_all_three_options_any_order() {
        let a = parse_opts_bytes(&[b"FLASH.DRAIN", b"MATCH", b"k*", b"COUNT", b"10", b"FORCE"])
            .unwrap();
        let b = parse_opts_bytes(&[b"FLASH.DRAIN", b"COUNT", b"10", b"FORCE", b"MATCH", b"k*"])
            .unwrap();
        let c = parse_opts_bytes(&[b"FLASH.DRAIN", b"FORCE", b"MATCH", b"k*", b"COUNT", b"10"])
            .unwrap();
        for o in [a, b, c] {
            assert_eq!(o.pattern.as_deref(), Some(b"k*".as_ref()));
            assert_eq!(o.count, Some(10));
            assert!(o.force);
        }
    }

    #[test]
    fn parse_repeated_match_last_wins() {
        // Not explicitly contracted, but documenting existing behaviour so a
        // refactor that changes it requires updating the test.
        let opts =
            parse_opts_bytes(&[b"FLASH.DRAIN", b"MATCH", b"first", b"MATCH", b"second"]).unwrap();
        assert_eq!(opts.pattern.as_deref(), Some(b"second".as_ref()));
    }

    #[test]
    fn parse_binary_safe_pattern_preserves_null_bytes() {
        let opts = parse_opts_bytes(&[b"FLASH.DRAIN", b"MATCH", b"pre\x00post*"]).unwrap();
        assert_eq!(opts.pattern.as_deref(), Some(b"pre\x00post*".as_ref()));
    }
}
