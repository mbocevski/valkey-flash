#!/usr/bin/env python3
"""
YCSB-style benchmark for valkey-flash v2.

4 workloads × 4 working-set/cache ratios = 16 measurement combinations.

Workloads (Zipfian key distribution, theta=0.99):
  A: 50% reads / 50% writes  — update-heavy (session store)
  B: 95% reads /  5% writes  — read-heavy (photo tagging)
  C: 100% reads              — read-only (cache)
  D: 95% reads /  5% inserts — read-latest (user status)

Ratio sweep: 0.25×, 1×, 4×, 16× (working-set bytes / cache-size bytes).

Usage:
  python3 tests/bench/ycsb.py [--baseline <prev-report.json>]

Environment:
  MODULE_PATH    path to libvalkey_flash.so
  SERVER_BIN     path to valkey-server
  SERVER_VERSION valkey version dir name (default: unstable)

Output:
  ycsb-report.json   machine-readable results (CI artifact)
  stdout             human-readable summary table
  ::notice::         GitHub annotation per combo (when GITHUB_ACTIONS is set)
  ::warning::        GitHub annotation for p99 regressions > 20%
"""

import argparse
import json
import math
import os
import random
import socket
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager, suppress

import valkey

# ── Parameters ────────────────────────────────────────────────────────────────

BASE_CACHE_BYTES = 16 * 1024 * 1024  # flash.cache-size-bytes for every combo
VALUE_BYTES = 1024  # 1 KiB per key
FLASH_CAPACITY_BYTES = 512 * 1024 * 1024  # 512 MiB flash file per combo

OPS = 5_000  # measurement ops per combo
WARMUP = 200  # warmup ops per combo (not measured)

RATIOS = [0.25, 1.0, 4.0, 16.0]  # working-set / cache size ratios
ZIPF_THETA = 0.99  # YCSB standard skew parameter

REGRESSION_WARN_PCT = 20  # emit ::warning:: on > 20% p99 regression

REPORT_FILE = "ycsb-report.json"
IN_CI = "GITHUB_ACTIONS" in os.environ

# (name, read_frac, write_frac, insert_frac, description)
WORKLOADS = [
    ("A", 0.50, 0.50, 0.00, "50R/50W update-heavy"),
    ("B", 0.95, 0.05, 0.00, "95R/5W  read-heavy"),
    ("C", 1.00, 0.00, 0.00, "100R    read-only"),
    ("D", 0.95, 0.00, 0.05, "95R/5I  read-latest"),
]


# ── Zipfian key generator (matches YCSB-core ZipfianGenerator) ────────────────


class ZipfianGenerator:
    """
    Zipfian distribution over [0, n_items).

    Replicates the algorithm from YCSB-core ZipfianGenerator.java so that
    access frequencies match the standard benchmark definition.
    """

    def __init__(self, n_items: int, theta: float = ZIPF_THETA) -> None:
        self.n = n_items
        self.theta = theta
        self.alpha = 1.0 / (1.0 - theta)
        self.zeta2 = sum(math.pow(i, -theta) for i in range(1, 3))
        self.zetan = sum(math.pow(i, -theta) for i in range(1, n_items + 1))
        self.eta = (1.0 - math.pow(2.0 / n_items, 1.0 - theta)) / (1.0 - self.zeta2 / self.zetan)

    def next(self, rng: random.Random) -> int:
        u = rng.random()
        uz = u * self.zetan
        if uz < 1.0:
            return 0
        if uz < 1.0 + math.pow(0.5, self.theta):
            return 1
        return int(self.n * math.pow(self.eta * u - self.eta + 1.0, self.alpha)) % self.n


# ── Statistics ────────────────────────────────────────────────────────────────


def percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = max(0, int(len(sorted_vals) * p / 100.0) - 1)
    return sorted_vals[min(idx, len(sorted_vals) - 1)]


def compute_stats(times_us: list[float]) -> dict:
    s = sorted(times_us)
    total_s = sum(times_us) / 1_000_000
    return {
        "ops": len(s),
        "p50_us": round(percentile(s, 50), 2),
        "p99_us": round(percentile(s, 99), 2),
        "p999_us": round(percentile(s, 99.9), 2),
        "throughput_ops_s": round(len(s) / total_s, 1) if total_s > 0 else 0.0,
    }


# ── Server lifecycle ──────────────────────────────────────────────────────────


def find_free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_server(port: int, timeout: float = 20.0) -> bool:
    deadline = time.monotonic() + timeout
    # Phase 1: TCP connectivity
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                break
        except OSError:
            time.sleep(0.1)
    else:
        return False
    # Phase 2: flash module initialization (TCP up != module ready).
    # valkey-py 6.x parses INFO into a dict; don't rely on raw-string matching.
    while time.monotonic() < deadline:
        try:
            c = valkey.Valkey(host="127.0.0.1", port=port, socket_timeout=1)
            info = c.info("flash")
            if info.get("flash_module_state") == "ready":
                return True
        except Exception:
            pass
        time.sleep(0.1)
    return False


@contextmanager
def running_server(module_path: str, server_bin: str):
    """Start a fresh valkey-server with the flash module; yield a connected client."""
    port = find_free_port()
    binaries_dir = os.path.dirname(server_bin)

    with tempfile.NamedTemporaryFile(suffix=".bin", delete=False) as tf:
        flash_path = tf.name

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
        "--flash.capacity-bytes",
        str(FLASH_CAPACITY_BYTES),
        "--flash.cache-size-bytes",
        str(BASE_CACHE_BYTES),
        "--save",
        "",
        "--loglevel",
        "warning",
    ]

    env = os.environ.copy()
    existing_ld = env.get("LD_LIBRARY_PATH", "")
    env["LD_LIBRARY_PATH"] = f"{binaries_dir}:{existing_ld}" if existing_ld else binaries_dir

    proc = subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        if not wait_for_server(port):
            proc.terminate()
            raise RuntimeError(f"valkey-server failed to start on port {port}")
        client = valkey.Valkey(host="127.0.0.1", port=port, socket_timeout=30)
        yield client
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        for p in (flash_path, flash_path + ".wal"):
            with suppress(OSError):
                os.unlink(p)


# ── Benchmark core ────────────────────────────────────────────────────────────


def run_combo(
    workload_name: str,
    read_frac: float,
    write_frac: float,
    insert_frac: float,
    ratio: float,
    module_path: str,
    server_bin: str,
) -> dict:
    n_keys = max(100, int(BASE_CACHE_BYTES * ratio / VALUE_BYTES))
    value = b"v" * VALUE_BYTES
    rng = random.Random(42)  # deterministic seed per combo
    zipf = ZipfianGenerator(n_keys)
    insert_counter = n_keys  # new-key counter starts after existing keyspace

    # Theoretical cache-miss estimate: fraction of keyspace that cannot stay hot
    cache_capacity_keys = BASE_CACHE_BYTES / VALUE_BYTES
    miss_ratio_est = round(max(0.0, (n_keys - cache_capacity_keys) / n_keys), 4)

    def pick_op() -> str:
        r = rng.random()
        if r < read_frac:
            return "R"
        if r < read_frac + write_frac:
            return "W"
        return "I"

    with running_server(module_path, server_bin) as client:
        # Populate: write n_keys sequentially so old entries evict to NVMe for
        # high-ratio combos (natural eviction via cache pressure)
        for i in range(n_keys):
            client.execute_command("FLASH.SET", f"ycsb:{i}", value)

        # Warmup (not measured)
        for _ in range(WARMUP):
            op = pick_op()
            if op == "I":
                key = f"ycsb:{insert_counter}"
                insert_counter += 1
                client.execute_command("FLASH.SET", key, value)
            elif op == "R":
                client.execute_command("FLASH.GET", f"ycsb:{zipf.next(rng)}")
            else:
                client.execute_command("FLASH.SET", f"ycsb:{zipf.next(rng)}", value)

        # Measurement
        read_times: list[float] = []
        write_times: list[float] = []
        misses = 0

        for _ in range(OPS):
            op = pick_op()
            t0 = time.perf_counter()
            if op == "I":
                key = f"ycsb:{insert_counter}"
                insert_counter += 1
                client.execute_command("FLASH.SET", key, value)
                write_times.append((time.perf_counter() - t0) * 1_000_000)
            elif op == "R":
                result = client.execute_command("FLASH.GET", f"ycsb:{zipf.next(rng)}")
                read_times.append((time.perf_counter() - t0) * 1_000_000)
                if result is None:
                    misses += 1
            else:
                client.execute_command("FLASH.SET", f"ycsb:{zipf.next(rng)}", value)
                write_times.append((time.perf_counter() - t0) * 1_000_000)

    result = compute_stats(read_times + write_times)
    result.update(
        workload=workload_name,
        ratio=ratio,
        n_keys=n_keys,
        value_bytes=VALUE_BYTES,
        cache_bytes=BASE_CACHE_BYTES,
        miss_ratio_est=miss_ratio_est,
    )
    if read_times:
        result["read_p99_us"] = round(percentile(sorted(read_times), 99), 2)
        result["observed_miss_ratio"] = round(misses / len(read_times), 4)
    if write_times:
        result["write_p99_us"] = round(percentile(sorted(write_times), 99), 2)
    return result


# ── Reporting ─────────────────────────────────────────────────────────────────


def annotate(level: str, msg: str) -> None:
    if IN_CI:
        print(f"::{level}::{msg}")
    elif level == "warning":
        print(f"WARNING: {msg}", file=sys.stderr)


def print_summary(results: dict) -> None:
    col = 30
    print(
        f"\n{'Combo':<{col}}  {'p50 µs':>8}  {'p99 µs':>8}  {'p999 µs':>8}"
        f"  {'ops/s':>8}  {'miss%':>6}"
    )
    print("-" * (col + 52))
    for key in sorted(results.keys()):
        s = results[key]
        if key.startswith("_") or not isinstance(s, dict) or "p99_us" not in s:
            continue
        miss_pct = s.get("miss_ratio_est", 0.0) * 100
        print(
            f"{key:<{col}}  {s['p50_us']:>8.1f}  {s['p99_us']:>8.1f}"
            f"  {s['p999_us']:>8.1f}  {s['throughput_ops_s']:>8.0f}  {miss_pct:>5.0f}%"
        )
    print()


def write_step_summary(results: dict) -> None:
    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_file:
        return
    with open(summary_file, "a") as f:
        f.write("## YCSB Benchmark Results\n\n")
        f.write(
            "| Combo | p50 µs | p99 µs | p999 µs | ops/s | miss% |\n"
            "|-------|--------|--------|---------|-------|-------|\n"
        )
        for key in sorted(results.keys()):
            s = results[key]
            if key.startswith("_") or not isinstance(s, dict) or "p99_us" not in s:
                continue
            miss_pct = s.get("miss_ratio_est", 0.0) * 100
            f.write(
                f"| {key} | {s['p50_us']:.0f} | {s['p99_us']:.0f}"
                f" | {s['p999_us']:.0f} | {s['throughput_ops_s']:.0f}"
                f" | {miss_pct:.0f}% |\n"
            )


def compare_with_baseline(current: dict, baseline: dict) -> list[str]:
    warnings = []
    for key, curr in current.items():
        if key.startswith("_") or not isinstance(curr, dict) or "p99_us" not in curr:
            continue
        prev = baseline.get(key)
        if not isinstance(prev, dict) or "p99_us" not in prev or prev["p99_us"] <= 0:
            continue
        delta_pct = (curr["p99_us"] - prev["p99_us"]) / prev["p99_us"] * 100
        if delta_pct > REGRESSION_WARN_PCT:
            warnings.append(
                f"{key}: p99 {curr['p99_us']:.0f}µs vs baseline {prev['p99_us']:.0f}µs"
                f" ({delta_pct:+.0f}%)"
            )
    return warnings


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument(
        "--baseline",
        metavar="FILE",
        help="Previous ycsb-report.json for p99 regression comparison",
    )
    args = parser.parse_args()

    server_version = os.environ.get("SERVER_VERSION", "unstable")
    module_path = os.path.realpath(
        os.environ.get("MODULE_PATH", os.path.join("target", "release", "libvalkey_flash.so"))
    )
    server_bin = os.path.realpath(
        os.environ.get(
            "SERVER_BIN",
            os.path.join("tests", "build", "binaries", server_version, "valkey-server"),
        )
    )

    total_combos = len(WORKLOADS) * len(RATIOS)
    print(f"valkey-flash YCSB v2  module={module_path}")
    print(f"  {total_combos} combos ({len(WORKLOADS)} workloads × {len(RATIOS)} ratios)")
    print(
        f"  ops/combo={OPS:,}  warmup={WARMUP}  cache={BASE_CACHE_BYTES // 2**20}MiB  value={VALUE_BYTES}B"
    )

    results: dict = {}
    done = 0

    for wl_name, read_frac, write_frac, insert_frac, wl_desc in WORKLOADS:
        for ratio in RATIOS:
            done += 1
            n_keys = max(100, int(BASE_CACHE_BYTES * ratio / VALUE_BYTES))
            combo_key = f"workload_{wl_name}_ratio_{ratio:.2f}x"
            print(
                f"[{done}/{total_combos}] {combo_key}  {wl_desc}  n_keys={n_keys:,}",
                flush=True,
            )

            stats = run_combo(
                wl_name,
                read_frac,
                write_frac,
                insert_frac,
                ratio,
                module_path,
                server_bin,
            )
            results[combo_key] = stats

            annotate(
                "notice",
                f"{combo_key}: p50={stats['p50_us']:.0f}µs"
                f" p99={stats['p99_us']:.0f}µs"
                f" p999={stats['p999_us']:.0f}µs"
                f" ops/s={stats['throughput_ops_s']:.0f}"
                f" miss_est={stats['miss_ratio_est'] * 100:.0f}%",
            )

    results["_meta"] = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "base_cache_bytes": BASE_CACHE_BYTES,
        "value_bytes": VALUE_BYTES,
        "ops_per_combo": OPS,
        "zipf_theta": ZIPF_THETA,
        "ratios": RATIOS,
        "workloads": [w[0] for w in WORKLOADS],
    }

    with open(REPORT_FILE, "w") as f:
        json.dump(results, f, indent=2)

    print_summary(results)
    write_step_summary(results)
    print(f"Report written to {REPORT_FILE}")

    # Regression comparison
    baseline_path = args.baseline
    if baseline_path:
        try:
            with open(baseline_path) as f:
                baseline = json.load(f)
            regressions = compare_with_baseline(results, baseline)
            if regressions:
                print(f"\nRegression warnings vs {baseline_path}:")
                for w in regressions:
                    print(f"  {w}")
                    annotate("warning", f"YCSB regression: {w}")
            else:
                print(f"No regressions vs {baseline_path}")
        except (FileNotFoundError, json.JSONDecodeError) as exc:
            print(f"Baseline comparison skipped: {exc}")


if __name__ == "__main__":
    main()
