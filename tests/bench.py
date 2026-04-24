#!/usr/bin/env python3
"""
FLASH.* benchmark suite v1.

Three scenarios:
  1. flash_set_vs_set  — FLASH.SET vs native SET, four value sizes
  2. flash_get_hot     — FLASH.GET, working set fits in cache
  3. flash_get_cold    — FLASH.GET, working set 4× cache size (mostly cache-miss)

Usage:
  python3 tests/bench.py

Environment:
  MODULE_PATH    path to libvalkey_flash.so  (default: inferred from SERVER_VERSION)
  SERVER_BIN     path to valkey-server       (default: inferred from SERVER_VERSION)
  SERVER_VERSION valkey version dir name     (default: unstable)

Output:
  bench-report.json  — machine-readable results (uploaded as CI artifact)
  stdout             — human-readable table

Exit 0: all scenarios pass thresholds.
Exit 1: threshold exceeded or server failed to start.
"""

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager, suppress

import valkey

# ── Benchmark parameters ──────────────────────────────────────────────────────

OPS = 10_000
WARMUP = 200
VALUE_SIZES_BYTES = [64, 1_024, 16_384, 131_072]

HOT_CACHE_BYTES = 32 * 1024 * 1024  # 32 MiB
COLD_CACHE_BYTES = 4 * 1024 * 1024  # 4 MiB — working set will be 4×
COLD_VALUE_BYTES = 4 * 1024  # 4 KiB values for cold scenario

# CI regression thresholds (µs, p99).
# These are intentionally generous for the first CI run; calibrate from artifacts.
THRESHOLDS: dict[str, float] = {
    "flash_set_64b_p99": 5_000,
    "flash_set_1kb_p99": 10_000,
    "flash_set_16kb_p99": 50_000,
    "flash_set_128kb_p99": 200_000,
    "flash_get_hot_p99": 5_000,
    "flash_get_cold_p99": 500_000,
}

REPORT_FILE = "bench-report.json"


# ── Helpers ───────────────────────────────────────────────────────────────────


def find_free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_server(port: int, timeout: float = 15.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def pct(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = max(0, int(len(sorted_vals) * p / 100.0) - 1)
    return sorted_vals[min(idx, len(sorted_vals) - 1)]


def compute_stats(times_us: list[float]) -> dict:
    s = sorted(times_us)
    total_s = sum(times_us) / 1_000_000
    return {
        "ops": len(s),
        "p50_us": round(pct(s, 50), 2),
        "p99_us": round(pct(s, 99), 2),
        "p999_us": round(pct(s, 99.9), 2),
        "throughput_ops_s": round(len(s) / total_s, 1) if total_s > 0 else 0,
    }


@contextmanager
def running_server(
    module_path: str,
    server_bin: str,
    server_version: str,
    flash_path: str,
    extra_args: list[str] | None = None,
):
    """Start valkey-server with the flash module; yield a connected client."""
    port = find_free_port()
    binaries_dir = os.path.dirname(server_bin)

    cmd = [
        server_bin,
        "--port",
        str(port),
        "--daemonize",
        "no",
        "--loadmodule",
        module_path,
        "--flash.path",
        flash_path,
        # Demotion caps low enough that a single bench client can't get
        # starved by the auto-demotion tick sharing the AsyncThreadPool
        # queue. `io_threads * 4` is the pool queue depth; keeping demotion
        # below a quarter of that leaves steady-state headroom for the
        # bench's sequential FLASH.SET / HSET / RPUSH / ZADD writes on
        # 2-core CI hosts where `num_cpus::get() == 2`.
        "--flash.demotion-batch",
        "4",
        "--flash.demotion-max-inflight",
        "16",
        "--save",
        "",
        "--loglevel",
        "warning",
    ]
    if extra_args:
        cmd.extend(extra_args)

    env = os.environ.copy()
    existing_ld = env.get("LD_LIBRARY_PATH", "")
    env["LD_LIBRARY_PATH"] = f"{binaries_dir}:{existing_ld}" if existing_ld else binaries_dir

    proc = subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        if not wait_for_server(port):
            proc.terminate()
            raise RuntimeError(f"valkey-server failed to start on port {port}")
        client = valkey.Valkey(host="127.0.0.1", port=port, socket_timeout=10)
        yield client
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def measure_cmd(client, cmd: str, key: str, value: bytes) -> float:
    t0 = time.perf_counter()
    client.execute_command(cmd, key, value)
    return (time.perf_counter() - t0) * 1_000_000


def measure_get(client, cmd: str, key: str) -> float:
    t0 = time.perf_counter()
    client.execute_command(cmd, key)
    return (time.perf_counter() - t0) * 1_000_000


# ── Scenario 1: FLASH.SET vs SET ─────────────────────────────────────────────


def scenario_flash_set_vs_set(module_path: str, server_bin: str, server_version: str) -> dict:
    results = {}
    with tempfile.NamedTemporaryFile(suffix=".bin", delete=False) as tf:
        flash_path = tf.name
    try:
        # 10k writes × 4 sizes (up to 128 KiB each) + 200 warmups per size can
        # accumulate > 1 GiB of NVMe state before compaction reclaims free blocks,
        # so raise capacity above the module's 1 GiB default.
        extra = ["--flash.capacity-bytes", str(4 * 1024 * 1024 * 1024)]
        with running_server(
            module_path, server_bin, server_version, flash_path, extra_args=extra
        ) as client:
            for size in VALUE_SIZES_BYTES:
                value = b"x" * size
                label = _size_label(size)

                # Warmup
                for i in range(WARMUP):
                    client.execute_command("FLASH.SET", f"w:{i}", value)

                # FLASH.SET
                flash_times = []
                for i in range(OPS):
                    flash_times.append(measure_cmd(client, "FLASH.SET", f"fs:{i}", value))

                # native SET (same server, different key prefix)
                set_times = []
                for i in range(OPS):
                    set_times.append(measure_cmd(client, "SET", f"s:{i}", value))

                results[f"flash_set_{label}"] = compute_stats(flash_times)
                results[f"native_set_{label}"] = compute_stats(set_times)
    finally:
        for p in (flash_path, os.path.splitext(flash_path)[0] + ".wal"):
            with suppress(OSError):
                os.unlink(p)
    return results


# ── Scenario 2: FLASH.GET hot-path ───────────────────────────────────────────


def scenario_flash_get_hot(module_path: str, server_bin: str, server_version: str) -> dict:
    n_keys = 500
    value = b"h" * 1_024  # 1 KiB values

    with tempfile.NamedTemporaryFile(suffix=".bin", delete=False) as tf:
        flash_path = tf.name
    try:
        extra = ["--flash.cache-size-bytes", str(HOT_CACHE_BYTES)]
        with running_server(
            module_path, server_bin, server_version, flash_path, extra_args=extra
        ) as client:
            # Populate working set (fits in 32 MiB cache: 500 × 1 KiB = 500 KiB)
            for i in range(n_keys):
                client.execute_command("FLASH.SET", f"hot:{i}", value)

            # Warmup reads
            for i in range(WARMUP):
                client.execute_command("FLASH.GET", f"hot:{i % n_keys}")

            # Measure — cycle through all keys (hot, in-cache reads)
            times = []
            for i in range(OPS):
                times.append(measure_get(client, "FLASH.GET", f"hot:{i % n_keys}"))
    finally:
        for p in (flash_path, os.path.splitext(flash_path)[0] + ".wal"):
            with suppress(OSError):
                os.unlink(p)
    return {"flash_get_hot": compute_stats(times)}


# ── Scenario 3: FLASH.GET cold-path ──────────────────────────────────────────


def scenario_flash_get_cold(module_path: str, server_bin: str, server_version: str) -> dict:
    # 4× cache size worth of data: 4 MiB cache, 4 KiB values → 4096 keys = 16 MiB
    n_keys = int(COLD_CACHE_BYTES * 4 / COLD_VALUE_BYTES)
    value = b"c" * COLD_VALUE_BYTES

    with tempfile.NamedTemporaryFile(suffix=".bin", delete=False) as tf:
        flash_path = tf.name
    try:
        extra = [
            "--flash.cache-size-bytes",
            str(COLD_CACHE_BYTES),
            "--flash.capacity-bytes",
            str(128 * 1024 * 1024),  # 128 MiB
        ]
        with running_server(
            module_path, server_bin, server_version, flash_path, extra_args=extra
        ) as client:
            # Populate — writes to both cache and NVMe
            for i in range(n_keys):
                client.execute_command("FLASH.SET", f"cold:{i}", value)

            # Access oldest keys first to maximise cache-miss rate
            # (S3-FIFO evicts older entries under pressure)
            times = []
            for i in range(OPS):
                key_idx = i % n_keys  # cycle through entire working set
                times.append(measure_get(client, "FLASH.GET", f"cold:{key_idx}"))
    finally:
        for p in (flash_path, os.path.splitext(flash_path)[0] + ".wal"):
            with suppress(OSError):
                os.unlink(p)
    return {"flash_get_cold": compute_stats(times)}


# ── Reporting ─────────────────────────────────────────────────────────────────


def _size_label(n: int) -> str:
    if n < 1024:
        return f"{n}b"
    return f"{n // 1024}kb"


def print_table(results: dict) -> None:
    col = 20
    print(f"\n{'Scenario':<{col}}  {'p50 µs':>10}  {'p99 µs':>10}  {'p999 µs':>10}  {'ops/s':>10}")
    print("-" * (col + 46))
    for name, s in sorted(results.items()):
        if not isinstance(s, dict) or "p50_us" not in s:
            continue
        print(
            f"{name:<{col}}  {s['p50_us']:>10.1f}  {s['p99_us']:>10.1f}"
            f"  {s['p999_us']:>10.1f}  {s['throughput_ops_s']:>10.0f}"
        )
    print()


def check_thresholds(results: dict) -> list[str]:
    failures = []
    for scenario_key, threshold_us in THRESHOLDS.items():
        # e.g. "flash_set_64b_p99" → scenario="flash_set_64b", metric="p99_us"
        parts = scenario_key.rsplit("_", 1)
        if len(parts) != 2:
            continue
        scenario, metric = parts[0], parts[1] + "_us"
        s = results.get(scenario, {})
        if not isinstance(s, dict):
            continue
        actual = s.get(metric, 0)
        if actual > threshold_us:
            failures.append(
                f"{scenario_key}: {actual:.1f}µs exceeds threshold {threshold_us:.0f}µs"
            )
    return failures


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    server_version = os.environ.get("SERVER_VERSION", "unstable")
    module_path = os.environ.get(
        "MODULE_PATH",
        os.path.join("target", "release", "libvalkey_flash.so"),
    )
    server_bin = os.environ.get(
        "SERVER_BIN",
        os.path.join("tests", "build", "binaries", server_version, "valkey-server"),
    )
    module_path = os.path.realpath(module_path)
    server_bin = os.path.realpath(server_bin)

    print("valkey-flash benchmark v1")
    print(f"  module:  {module_path}")
    print(f"  server:  {server_bin}")
    print(f"  ops/scenario: {OPS:,}  warmup: {WARMUP}")

    results: dict = {}

    print("\n[1/3] FLASH.SET vs native SET (4 value sizes)...")
    results.update(scenario_flash_set_vs_set(module_path, server_bin, server_version))

    print("[2/3] FLASH.GET hot-path...")
    results.update(scenario_flash_get_hot(module_path, server_bin, server_version))

    print("[3/3] FLASH.GET cold-path (4× cache working set)...")
    results.update(scenario_flash_get_cold(module_path, server_bin, server_version))

    results["_meta"] = {
        "valkey_version": server_version,
        "ops_per_scenario": OPS,
        "thresholds_unit": "µs",
        "thresholds": THRESHOLDS,
    }

    with open(REPORT_FILE, "w") as f:
        json.dump(results, f, indent=2)

    print_table(results)
    print(f"Report written to {REPORT_FILE}")

    failures = check_thresholds(results)
    if failures:
        print("FAIL: threshold(s) exceeded:", file=sys.stderr)
        for msg in failures:
            print(f"  {msg}", file=sys.stderr)
        sys.exit(1)

    print("PASS: all thresholds met")


if __name__ == "__main__":
    main()
