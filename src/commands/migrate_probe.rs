use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::config::FLASH_MIGRATION_PROBE_CACHE_SEC;
use crate::recovery::ModuleState;
use crate::{MODULE_STATE, STORAGE};

// ── Probe cache ───────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct ProbeResult {
    pub state: String,
    pub capacity_bytes: u64,
    pub free_bytes: u64,
    pub path: String,
}

static PROBE_CACHE: LazyLock<Mutex<std::collections::HashMap<String, (Instant, ProbeResult)>>> =
    LazyLock::new(|| Mutex::new(std::collections::HashMap::new()));

fn cache_get(addr: &str) -> Option<ProbeResult> {
    let ttl_sec = FLASH_MIGRATION_PROBE_CACHE_SEC.load(std::sync::atomic::Ordering::Relaxed);
    if ttl_sec <= 0 {
        return None;
    }
    let guard = PROBE_CACHE.lock().ok()?;
    let (ts, result) = guard.get(addr)?;
    if ts.elapsed().as_secs() < ttl_sec as u64 {
        Some(result.clone())
    } else {
        None
    }
}

fn cache_put(addr: &str, result: ProbeResult) {
    let ttl_sec = FLASH_MIGRATION_PROBE_CACHE_SEC.load(std::sync::atomic::Ordering::Relaxed);
    if ttl_sec <= 0 {
        return;
    }
    if let Ok(mut guard) = PROBE_CACHE.lock() {
        guard.insert(addr.to_string(), (Instant::now(), result));
    }
}

// ── Local probe info ──────────────────────────────────────────────────────────

fn local_probe() -> ValkeyResult {
    let state = {
        let s = MODULE_STATE
            .lock()
            .map_err(|_| ValkeyError::Str("ERR MODULE_STATE lock poisoned"))?;
        match *s {
            ModuleState::Recovering => "recovering",
            ModuleState::Ready => "ready",
            ModuleState::Error => "error",
        }
        .to_string()
    };

    let capacity_bytes = STORAGE.get().map(|s| s.capacity_bytes()).unwrap_or(0);
    let free_bytes = STORAGE
        .get()
        .map(|s| s.free_block_count() * 4096)
        .unwrap_or(0);
    let path = crate::config::FLASH_PATH
        .lock()
        .map(|g| g.clone())
        .unwrap_or_else(|_| crate::config::FLASH_PATH_DEFAULT.to_string());

    Ok(ValkeyValue::Array(vec![
        ValkeyValue::BulkString("state".to_string()),
        ValkeyValue::BulkString(state),
        ValkeyValue::BulkString("capacity_bytes".to_string()),
        ValkeyValue::BulkString(capacity_bytes.to_string()),
        ValkeyValue::BulkString("free_bytes".to_string()),
        ValkeyValue::BulkString(free_bytes.to_string()),
        ValkeyValue::BulkString("path".to_string()),
        ValkeyValue::BulkString(path),
    ]))
}

// ── Remote probe ──────────────────────────────────────────────────────────────

/// Send `FLASH.MIGRATE.PROBE` to `host:port` and parse the response.
///
/// Returns an error string if:
/// - Connection fails / times out → `ERR FLASH-MIGRATE target <addr> did not respond within timeout`
/// - Target returns UNKNOWN COMMAND → `ERR FLASH-MIGRATE target <addr> does not have flash-module loaded`
pub(crate) fn remote_probe(host: &str, port: u16) -> Result<ProbeResult, String> {
    let addr = format!("{host}:{port}");

    if let Some(cached) = cache_get(&addr) {
        return Ok(cached);
    }

    // `SocketAddr::parse` only accepts numeric addresses. Resolve hostnames via
    // `ToSocketAddrs` so callers can target `host.docker.internal`, internal
    // DNS names, or any other symbolic peer.
    let socket_addr = {
        use std::net::ToSocketAddrs;
        (host, port)
            .to_socket_addrs()
            .map_err(|e| format!("ERR FLASH-MIGRATE target {addr} invalid address: {e}"))?
            .next()
            .ok_or_else(|| format!("ERR FLASH-MIGRATE target {addr} resolved to no addresses"))?
    };
    let stream = TcpStream::connect_timeout(&socket_addr, Duration::from_secs(5))
        .map_err(|_| format!("ERR FLASH-MIGRATE target {addr} did not respond within timeout"))?;

    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| format!("ERR FLASH-MIGRATE target {addr} socket error: {e}"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| format!("ERR FLASH-MIGRATE target {addr} socket error: {e}"))?;

    let mut stream = stream;
    // RESP2 command: FLASH.MIGRATE.PROBE (no key argument) — 19 chars, not 18.
    let cmd = b"*1\r\n$19\r\nFLASH.MIGRATE.PROBE\r\n";
    stream
        .write_all(cmd)
        .map_err(|e| format!("ERR FLASH-MIGRATE target {addr} write error: {e}"))?;

    let mut buf = vec![0u8; 4096];
    let n = stream
        .read(&mut buf)
        .map_err(|_| format!("ERR FLASH-MIGRATE target {addr} did not respond within timeout"))?;
    let response = std::str::from_utf8(&buf[..n])
        .map_err(|e| format!("ERR FLASH-MIGRATE target {addr} non-UTF8 response: {e}"))?;

    // Check for error response (unknown command or module not loaded).
    if response.starts_with('-') {
        let msg = response.trim_start_matches('-').trim();
        if msg.contains("unknown command")
            || msg.contains("ERR unknown")
            || msg.contains("NOSCRIPT")
        {
            return Err(format!(
                "ERR FLASH-MIGRATE target {addr} does not have flash-module loaded"
            ));
        }
        return Err(format!("ERR FLASH-MIGRATE target {addr}: {msg}"));
    }

    // Parse a flat RESP2 array: *8\r\n $key $val $key $val ...
    let pairs = parse_resp2_flat_array(response)
        .map_err(|e| format!("ERR FLASH-MIGRATE target {addr} parse error: {e}"))?;

    let mut map: std::collections::HashMap<&str, &str> = std::collections::HashMap::new();
    let mut iter = pairs.iter();
    while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
        map.insert(k.as_str(), v.as_str());
    }

    let result = ProbeResult {
        state: map.get("state").unwrap_or(&"unknown").to_string(),
        capacity_bytes: map
            .get("capacity_bytes")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0),
        free_bytes: map
            .get("free_bytes")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0),
        path: map.get("path").unwrap_or(&"").to_string(),
    };

    cache_put(&addr, result.clone());
    Ok(result)
}

/// Minimal RESP2 array parser for `*N\r\n$len\r\nbulk\r\n...` responses.
fn parse_resp2_flat_array(resp: &str) -> Result<Vec<String>, String> {
    let mut items = Vec::new();
    let mut lines = resp.split("\r\n");

    let count_line = lines.next().ok_or("empty response")?;
    if !count_line.starts_with('*') {
        return Err(format!("expected array, got: {count_line:?}"));
    }
    let count: usize = count_line[1..]
        .parse()
        .map_err(|_| "bad array count".to_string())?;

    for _ in 0..count {
        let len_line = lines.next().ok_or("truncated")?;
        if !len_line.starts_with('$') {
            return Err(format!("expected bulk string, got: {len_line:?}"));
        }
        let len: usize = len_line[1..]
            .parse()
            .map_err(|_| "bad bulk len".to_string())?;
        let bulk = lines.next().ok_or("truncated bulk")?;
        if bulk.len() != len {
            return Err(format!("len mismatch: expected {len} got {}", bulk.len()));
        }
        items.push(bulk.to_string());
    }
    Ok(items)
}

// ── Command handler ───────────────────────────────────────────────────────────

/// `FLASH.MIGRATE.PROBE [host port]`
///
/// Without arguments: returns info about this node's flash module state.
/// With host + port: probes the target node, caches the result, and returns it.
///
/// Response: flat array of alternating key/value bulk strings:
///   state, capacity_bytes, free_bytes, path
///
/// Errors:
/// - `ERR FLASH-MIGRATE target <addr> does not have flash-module loaded`
/// - `ERR FLASH-MIGRATE target <addr> did not respond within timeout`
pub fn flash_migrate_probe_command(_ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    match args.len() {
        1 => local_probe(),
        3 => {
            let host = args[1].to_string();
            let port_str = args[2].to_string();
            let port: u16 = port_str
                .parse()
                .map_err(|_| ValkeyError::String(format!("ERR invalid port: {port_str}")))?;
            remote_probe(&host, port)
                .map_err(ValkeyError::String)
                .map(|r| {
                    ValkeyValue::Array(vec![
                        ValkeyValue::BulkString("state".to_string()),
                        ValkeyValue::BulkString(r.state),
                        ValkeyValue::BulkString("capacity_bytes".to_string()),
                        ValkeyValue::BulkString(r.capacity_bytes.to_string()),
                        ValkeyValue::BulkString("free_bytes".to_string()),
                        ValkeyValue::BulkString(r.free_bytes.to_string()),
                        ValkeyValue::BulkString("path".to_string()),
                        ValkeyValue::BulkString(r.path),
                    ])
                })
        }
        _ => Err(ValkeyError::WrongArity),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Serializes tests that mutate the shared FLASH_MIGRATION_PROBE_CACHE_SEC atomic
    // and PROBE_CACHE global to prevent parallel-execution races.
    static CACHE_TEST_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn parse_resp2_flat_array_parses_correctly() {
        let resp =
            "*4\r\n$5\r\nstate\r\n$5\r\nready\r\n$14\r\ncapacity_bytes\r\n$10\r\n1073741824\r\n";
        let items = parse_resp2_flat_array(resp).unwrap();
        assert_eq!(
            items,
            vec!["state", "ready", "capacity_bytes", "1073741824"]
        );
    }

    #[test]
    fn parse_resp2_flat_array_empty_array() {
        let resp = "*0\r\n";
        let items = parse_resp2_flat_array(resp).unwrap();
        assert!(items.is_empty());
    }

    #[test]
    fn parse_resp2_flat_array_non_array_returns_error() {
        let result = parse_resp2_flat_array("+OK\r\n");
        assert!(result.is_err());
    }

    #[test]
    fn probe_cache_respects_ttl() {
        let _guard = CACHE_TEST_LOCK.lock().unwrap();
        use std::sync::atomic::Ordering;
        let original_ttl = FLASH_MIGRATION_PROBE_CACHE_SEC.load(Ordering::Relaxed);
        FLASH_MIGRATION_PROBE_CACHE_SEC.store(60, Ordering::Relaxed);

        let result = ProbeResult {
            state: "ready".to_string(),
            capacity_bytes: 1024,
            free_bytes: 512,
            path: "/tmp/test.bin".to_string(),
        };
        cache_put("test-host:1234", result.clone());
        let cached = cache_get("test-host:1234");
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().capacity_bytes, 1024);

        FLASH_MIGRATION_PROBE_CACHE_SEC.store(original_ttl, Ordering::Relaxed);
        PROBE_CACHE.lock().unwrap().remove("test-host:1234");
    }

    #[test]
    fn probe_cache_disabled_when_ttl_zero() {
        let _guard = CACHE_TEST_LOCK.lock().unwrap();
        use std::sync::atomic::Ordering;
        let original_ttl = FLASH_MIGRATION_PROBE_CACHE_SEC.load(Ordering::Relaxed);
        FLASH_MIGRATION_PROBE_CACHE_SEC.store(0, Ordering::Relaxed);
        let result = ProbeResult {
            state: "ready".to_string(),
            capacity_bytes: 1,
            free_bytes: 0,
            path: "/tmp/x".to_string(),
        };
        cache_put("nohost:9999", result);
        assert!(cache_get("nohost:9999").is_none());
        FLASH_MIGRATION_PROBE_CACHE_SEC.store(original_ttl, Ordering::Relaxed);
    }
}
