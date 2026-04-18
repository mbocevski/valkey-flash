#!/usr/bin/env python3
"""
FLASH.SET / FLASH.GET latency smoke test.

Starts valkey-server with the flash module loaded, runs sequential (non-pipelined)
operations to measure per-command latency, asserts p50/p99 thresholds, and writes
latency-report.json for CI artifact upload.

Exit 0: all thresholds met.
Exit 1: threshold exceeded or server failed to start.
"""

import json
import os
import socket
import subprocess
import sys
import tempfile
import time

import valkey

P50_THRESHOLD_US = 100
P99_THRESHOLD_US = 500

WARMUP_OPS = 200
MEASURE_OPS = 5000


def find_free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_server(port, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def percentile(sorted_vals, pct):
    idx = max(0, int(len(sorted_vals) * pct / 100) - 1)
    return sorted_vals[idx]


def main():
    module_path = os.environ.get("MODULE_PATH")
    server_version = os.environ.get("SERVER_VERSION", "unstable")
    server_bin = os.environ.get(
        "SERVER_BIN",
        os.path.join("tests", "build", "binaries", server_version, "valkey-server"),
    )

    if not module_path:
        print("ERROR: MODULE_PATH not set", file=sys.stderr)
        sys.exit(1)

    module_path = os.path.realpath(module_path)
    server_bin = os.path.realpath(server_bin)

    port = find_free_port()

    with tempfile.NamedTemporaryFile(suffix=".bin", delete=False) as tf:
        flash_path = tf.name

    try:
        cmd = [
            server_bin,
            "--port", str(port),
            "--daemonize", "no",
            "--loadmodule", module_path,
            "--save", "",
            "--loglevel", "warning",
        ]

        env = os.environ.copy()
        binaries_dir = os.path.dirname(server_bin)
        existing_ld = env.get("LD_LIBRARY_PATH", "")
        env["LD_LIBRARY_PATH"] = f"{binaries_dir}:{existing_ld}" if existing_ld else binaries_dir

        proc = subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            if not wait_for_server(port):
                print("ERROR: valkey-server failed to start within 15s", file=sys.stderr)
                proc.terminate()
                sys.exit(1)

            client = valkey.Valkey(host="127.0.0.1", port=port, socket_timeout=5)

            # Warmup
            for i in range(WARMUP_OPS):
                client.execute_command("FLASH.SET", f"warmup:{i}", f"v{i}")

            # Measure FLASH.SET
            set_times = []
            for i in range(MEASURE_OPS):
                t0 = time.perf_counter()
                client.execute_command("FLASH.SET", f"bench:{i}", f"value-{i:08d}")
                set_times.append((time.perf_counter() - t0) * 1_000_000)

            # Measure FLASH.GET (hot path — keys just written, in cache)
            get_times = []
            for i in range(MEASURE_OPS):
                t0 = time.perf_counter()
                client.execute_command("FLASH.GET", f"bench:{i}")
                get_times.append((time.perf_counter() - t0) * 1_000_000)

        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
    finally:
        try:
            os.unlink(flash_path)
        except OSError:
            pass

    set_times.sort()
    get_times.sort()
    mixed = sorted(set_times + get_times)

    def stats(vals):
        return {
            "p50_us": round(percentile(vals, 50), 2),
            "p99_us": round(percentile(vals, 99), 2),
            "ops": len(vals),
        }

    results = {
        "set": stats(set_times),
        "get": stats(get_times),
        "mixed": stats(mixed),
        "thresholds": {"p50_us": P50_THRESHOLD_US, "p99_us": P99_THRESHOLD_US},
        "valkey_version": server_version,
    }

    with open("latency-report.json", "w") as f:
        json.dump(results, f, indent=2)

    print(f"FLASH.SET  p50={results['set']['p50_us']:.1f}µs  p99={results['set']['p99_us']:.1f}µs")
    print(f"FLASH.GET  p50={results['get']['p50_us']:.1f}µs  p99={results['get']['p99_us']:.1f}µs")
    print(f"Mixed      p50={results['mixed']['p50_us']:.1f}µs  p99={results['mixed']['p99_us']:.1f}µs")
    print(f"Thresholds: p50≤{P50_THRESHOLD_US}µs  p99≤{P99_THRESHOLD_US}µs")

    failures = []
    if results["mixed"]["p50_us"] > P50_THRESHOLD_US:
        failures.append(
            f"p50={results['mixed']['p50_us']:.1f}µs exceeds threshold {P50_THRESHOLD_US}µs"
        )
    if results["mixed"]["p99_us"] > P99_THRESHOLD_US:
        failures.append(
            f"p99={results['mixed']['p99_us']:.1f}µs exceeds threshold {P99_THRESHOLD_US}µs"
        )

    if failures:
        print("FAIL: latency thresholds exceeded:", file=sys.stderr)
        for msg in failures:
            print(f"  {msg}", file=sys.stderr)
        sys.exit(1)

    print("PASS: all thresholds met")


if __name__ == "__main__":
    main()
