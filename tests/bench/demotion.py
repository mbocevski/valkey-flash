#!/usr/bin/env python3
"""Auto-demotion performance bench.

Measures the event-loop stall characteristics of `demotion::tick` as a
function of value size and shape.  Each cell runs a sustained-overflow
workload against a deliberately small hot cache so the demotion tick fires
continuously for the measurement window.

Inside the measurement window a background reader client issues GETs on a
known-hot working set; its p99 latency is the external-observer proxy for
event-loop stall time (demotion runs synchronously on the event loop, so
any blocking shows up in unrelated clients' tail latency).

Matrix:
  * sizes:  200 B, 1 KiB, 4 KiB, 16 KiB, 64 KiB
  * shapes: STRING, HASH (two fields), LIST (two elements), ZSET (two members)
  * cache:  4 MiB (fixed) — forces demotion across all cells
  * batch:  auto-sized (or per `flash.demotion-batch`) on the module side

Runtime: ~30 s per cell × 20 cells ≈ 10 min.

Per-cell outputs include demotions/sec, tick phase-1 p50/p99 (sampled
from `flash_demotion_tick_last_us` every 100 ms), AIMD stall events,
client GET p99, NVMe write amplification (bytes / demotion), and final
storage footprint.

Usage:
  python3 tests/bench/demotion.py [--window-sec 30] [--out-dir tests/bench/results]

Environment:
  MODULE_PATH    path to libvalkey_flash.so (required)
  SERVER_BIN     path to valkey-server (default: tests/build/binaries/$SERVER_VERSION/valkey-server)
  SERVER_VERSION valkey version dir name (default: unstable)

Output:
  tests/bench/results/demotion_v1.1.0.md   Markdown report
  tests/bench/results/demotion_v1.1.0.csv  machine-readable, loadable by the
                                           adaptive-batch task's tuner
"""

from __future__ import annotations

import argparse
import csv
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path

import valkey

# ── Configuration ─────────────────────────────────────────────────────────────

CACHE_SIZE_BYTES = 4 * 1024 * 1024
# Flash capacity only needs to exceed the live working set (cache + cold tail +
# per-write allocator slack). The measurement loop uses a delete-then-write
# ring so NVMe blocks cycle through the free-list instead of the bump
# allocator growing forever. 4 GiB gives comfortable headroom.
FLASH_CAPACITY_BYTES = 4 * 1024 * 1024 * 1024

# Preload step seeds the cache at 100% fill before the measurement window.
# Cap on total preload keys to avoid runaway counts at tiny value sizes.
PRELOAD_KEYS_MAX = 10_000

# Five log-spaced sizes — below 200 B the demotion work is serialisation-bound
# rather than NVMe-bound; above 64 KiB the matrix gets large without adding
# much information. Sizes match the spec in the `#143` backlog task.
VALUE_SIZES = [
    ("200B", 200),
    ("1KiB", 1024),
    ("4KiB", 4 * 1024),
    ("16KiB", 16 * 1024),
    ("64KiB", 64 * 1024),
]

# Per-shape writer factory: takes a client + index + value payload and issues
# one write.  Each shape writes a comparable total payload per key so the
# cells are apples-to-apples.
SHAPES = ["STRING", "HASH", "LIST", "ZSET"]

# Background reader load — low enough not to swamp the server, high enough to
# collect meaningful p99 samples.  GETs on a known-hot key are pure native
# reads, no flash involvement — any p99 spike is purely event-loop contention.
READER_RPS = 200
READER_KEY = b"__bench_hot__"
READER_VAL = b"hot"


# ── Server process management ─────────────────────────────────────────────────


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


@contextmanager
def running_server(module_path: str, server_bin: str, flash_path: str, cache_bytes: int):
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
        "--flash.capacity-bytes",
        str(FLASH_CAPACITY_BYTES),
        "--flash.cache-size-bytes",
        str(cache_bytes),
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
        client = valkey.Valkey(host="127.0.0.1", port=port, socket_timeout=10)
        yield client, port
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


# ── Latency sampling ──────────────────────────────────────────────────────────


def percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = max(0, int(len(sorted_vals) * p / 100.0) - 1)
    return sorted_vals[min(idx, len(sorted_vals) - 1)]


class Reader(threading.Thread):
    """Background goroutine-style reader issuing GET on READER_KEY at a fixed rps.

    Collects latency samples in a list that's flushed on stop().  GETs target
    a key pre-seeded in RAM (native SET, not FLASH.SET), so any p99 spike is
    pure event-loop contention, not flash miss latency.
    """

    def __init__(self, port: int, rps: int):
        super().__init__(daemon=True)
        self._port = port
        self._interval = 1.0 / rps
        self._stop_flag = threading.Event()
        self.samples_us: list[float] = []

    def run(self) -> None:
        c = valkey.Valkey(host="127.0.0.1", port=self._port, socket_timeout=10)
        next_tick = time.perf_counter()
        while not self._stop_flag.is_set():
            t0 = time.perf_counter()
            try:
                c.execute_command("GET", READER_KEY)
            except Exception:
                continue
            self.samples_us.append((time.perf_counter() - t0) * 1_000_000)
            next_tick += self._interval
            sleep_for = next_tick - time.perf_counter()
            if sleep_for > 0:
                time.sleep(sleep_for)
            else:
                next_tick = time.perf_counter()

    def stop(self) -> None:
        self._stop_flag.set()
        self.join(timeout=5)


# ── Shape writers ─────────────────────────────────────────────────────────────


def write_payload(client: valkey.Valkey, shape: str, key: str, payload: bytes) -> None:
    """Write one key of the given shape with a total payload close to len(payload).

    Multi-field shapes split the payload roughly in half across two fields so
    the stored encoding has comparable byte weight to the single-field shapes.
    """
    half = len(payload) // 2
    a = payload[:half] or b"a"
    b = payload[half:] or b"b"
    if shape == "STRING":
        client.execute_command("FLASH.SET", key, payload)
    elif shape == "HASH":
        client.execute_command("FLASH.HSET", key, "f1", a, "f2", b)
    elif shape == "LIST":
        client.execute_command("FLASH.RPUSH", key, a, b)
    elif shape == "ZSET":
        # ZADD scores are small ints; member bytes dominate the weight.
        client.execute_command("FLASH.ZADD", key, "1", a, "2", b)
    else:
        raise ValueError(f"unknown shape: {shape}")


# ── Cell measurement ──────────────────────────────────────────────────────────


@dataclass
class CellResult:
    size_label: str
    size_bytes: int
    shape: str
    # demotion throughput
    auto_demotions_delta: int
    demotions_per_sec: float
    tiered_keys_final: int
    # client latency under demotion
    client_get_p50_us: float
    client_get_p99_us: float
    client_get_p999_us: float
    client_get_samples: int
    # phase-1 tick cost (sampled from flash_demotion_tick_last_us every 100 ms)
    tick_phase1_p50_us: float
    tick_phase1_p99_us: float
    tick_stall_events_delta: int
    # NVMe write amplification: storage bytes written per demotion
    nvme_bytes_per_demote: float
    # cache + storage state at the end
    cache_hit_ratio_final: float
    eviction_count_delta: int
    storage_used_bytes_final: int
    storage_free_bytes_final: int


def parse_info(info: dict, key: str, default: int = 0) -> int:
    v = info.get(f"flash_{key}", default)
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def parse_info_float(info: dict, key: str) -> float:
    try:
        return float(info.get(f"flash_{key}", 0))
    except (TypeError, ValueError):
        return 0.0


def measure_cell(
    client: valkey.Valkey,
    port: int,
    size_label: str,
    size_bytes: int,
    shape: str,
    window_sec: float,
) -> CellResult:
    # Pre-seed the reader's hot key in native RAM (not flash).
    client.execute_command("FLASH.DEL", READER_KEY)
    client.execute_command("SET", READER_KEY, READER_VAL)

    # Warm-up: pre-populate ~4× cache worth so the cache is at 100% fill
    # before the measurement window starts.  This keeps the measurement
    # focused on steady-state demotion, not the first-fill transient.
    preload_count = min(
        PRELOAD_KEYS_MAX,
        max(100, (4 * CACHE_SIZE_BYTES) // max(size_bytes, 1)),
    )
    payload = b"x" * size_bytes
    for i in range(preload_count):
        write_payload(client, shape, f"{shape.lower()}_pre_{i}", payload)

    # Let the tick catch up and drain the preload overflow.
    time.sleep(1.0)
    info_before = client.info("flash")
    auto_before = parse_info(info_before, "auto_demotions_total")
    evict_before = parse_info(info_before, "eviction_count")
    stall_before = parse_info(info_before, "demotion_stall_events_total")
    storage_before = parse_info(info_before, "storage_used_bytes")

    # Measurement window: background reader + continuous delete-then-write
    # loop against a bounded key ring. DEL reclaims the prior NVMe blocks
    # via the free-list so the bump allocator doesn't exhaust capacity; the
    # ring is sized large enough that cache pressure stays sustained.
    ring_size = max(200, (8 * CACHE_SIZE_BYTES) // max(size_bytes, 1))
    reader = Reader(port, READER_RPS)
    reader.start()
    # Separate client thread for INFO sampling — keeps the tick-duration
    # sampler from interleaving with the writer loop's synchronous commands.
    tick_samples: list[int] = []
    sampler_stop = threading.Event()

    def _sample_ticks():
        c_sampler = valkey.Valkey(host="127.0.0.1", port=port, socket_timeout=5)
        while not sampler_stop.is_set():
            try:
                info = c_sampler.info("flash")
                v = info.get("flash_demotion_tick_last_us", 0)
                if isinstance(v, (int, float)) and v > 0:
                    tick_samples.append(int(v))
            except Exception:
                pass
            sampler_stop.wait(timeout=0.1)

    sampler_thread = threading.Thread(target=_sample_ticks, daemon=True)
    sampler_thread.start()

    t_start = time.perf_counter()
    i = 0
    while time.perf_counter() - t_start < window_sec:
        key = f"{shape.lower()}_m_{i % ring_size}"
        client.execute_command("FLASH.DEL", key)
        write_payload(client, shape, key, payload)
        i += 1
    elapsed = time.perf_counter() - t_start
    reader.stop()
    sampler_stop.set()
    sampler_thread.join(timeout=3)

    info_after = client.info("flash")
    auto_after = parse_info(info_after, "auto_demotions_total")
    evict_after = parse_info(info_after, "eviction_count")
    stall_after = parse_info(info_after, "demotion_stall_events_total")
    storage_after = parse_info(info_after, "storage_used_bytes")
    tiered_final = parse_info(info_after, "tiered_keys")
    hit_ratio = parse_info_float(info_after, "cache_hit_ratio")
    storage_free = parse_info(info_after, "storage_free_bytes")

    auto_delta = auto_after - auto_before
    storage_delta = max(0, storage_after - storage_before)
    nvme_per_demote = (storage_delta / auto_delta) if auto_delta > 0 else 0.0

    samples = sorted(reader.samples_us)
    tick_sorted = sorted(tick_samples)
    return CellResult(
        size_label=size_label,
        size_bytes=size_bytes,
        shape=shape,
        auto_demotions_delta=auto_delta,
        demotions_per_sec=auto_delta / elapsed if elapsed > 0 else 0.0,
        tiered_keys_final=tiered_final,
        client_get_p50_us=round(percentile(samples, 50), 2),
        client_get_p99_us=round(percentile(samples, 99), 2),
        client_get_p999_us=round(percentile(samples, 99.9), 2),
        client_get_samples=len(samples),
        tick_phase1_p50_us=round(percentile(tick_sorted, 50), 2),
        tick_phase1_p99_us=round(percentile(tick_sorted, 99), 2),
        tick_stall_events_delta=stall_after - stall_before,
        nvme_bytes_per_demote=round(nvme_per_demote, 1),
        cache_hit_ratio_final=hit_ratio,
        eviction_count_delta=evict_after - evict_before,
        storage_used_bytes_final=storage_after,
        storage_free_bytes_final=storage_free,
    )


# ── Reporting ─────────────────────────────────────────────────────────────────


def render_markdown(results: list[CellResult], window_sec: float) -> str:
    lines = [
        "# Auto-demotion stall benchmark",
        "",
        f"- Cache size: {CACHE_SIZE_BYTES // 1024} KiB (fixed)",
        f"- Flash capacity: {FLASH_CAPACITY_BYTES // (1024 * 1024)} MiB (fixed)",
        "- Demotion pipeline: phase-1 submit on the event loop, phase-2 NVMe",
        "  write on `AsyncThreadPool`, phase-3 commit drained at the top of",
        "  the next event-loop tick.",
        f"- Measurement window: {window_sec:.0f} s per cell",
        f"- Background reader load: {READER_RPS} GET/s on a native-RAM key",
        "",
        "Client GET p99 is the external-observer stall proxy: the reader hits a",
        "native-tier key, so any p99 bump reflects event-loop contention from",
        "the demotion tick's phase-1 work (payload clone + pool submit), not",
        "flash I/O. Demotion throughput columns show the rate at which keys",
        "reach NVMe cold tier under sustained cache overflow.",
        "",
    ]
    for shape in SHAPES:
        rows = [r for r in results if r.shape == shape]
        if not rows:
            continue
        lines.append(f"## {shape}")
        lines.append("")
        lines.append(
            "| Value size | Demotions/s | Tick p50 | Tick p99 | Stalls | "
            "Client GET p99 | NVMe B / demote | Storage used |"
        )
        lines.append("|---|---|---|---|---|---|---|---|")
        for r in rows:
            lines.append(
                f"| {r.size_label} | {r.demotions_per_sec:,.0f} | "
                f"{r.tick_phase1_p50_us:.1f} µs | {r.tick_phase1_p99_us:.1f} µs | "
                f"{r.tick_stall_events_delta} | "
                f"{r.client_get_p99_us:.1f} µs | "
                f"{r.nvme_bytes_per_demote:,.0f} B | "
                f"{r.storage_used_bytes_final // (1024 * 1024)} MiB |"
            )
        lines.append("")
    lines.append("")
    lines.append("### Column glossary")
    lines.append("")
    lines.append(
        "- **Tick p50 / p99** — phase-1 wall time sampled from "
        "`flash_demotion_tick_last_us` at 100 ms intervals over the cell's "
        "measurement window. Tracks the event-loop stall budget the adaptive "
        "AIMD controller uses; see `STALL_BUDGET_US` in `src/demotion.rs`."
    )
    lines.append(
        "- **Stalls** — delta of `flash_demotion_stall_events_total` over the "
        "cell. Non-zero means at least one tick exceeded `STALL_BUDGET_US` "
        "and the AIMD controller halved the effective batch cap."
    )
    lines.append(
        "- **Client GET p99** — latency of a background native-RAM GET loop; "
        "an external-observer proxy for event-loop contention from demotion."
    )
    lines.append(
        "- **NVMe B / demote** — write amplification: `storage_used_bytes` "
        "delta divided by the number of committed demotions. For the "
        "default 4 KiB-aligned allocator this should hover near "
        "`max(4096, value_size rounded up)`; substantial drift indicates a "
        "leak or accounting bug."
    )
    lines.append("")
    return "\n".join(lines) + "\n"


def write_csv(results: list[CellResult], path: Path) -> None:
    if not results:
        return
    fields = list(asdict(results[0]).keys())
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in results:
            w.writerow(asdict(r))


# ── CLI ───────────────────────────────────────────────────────────────────────


def _default_server_bin() -> str:
    version = os.environ.get("SERVER_VERSION", "unstable")
    return str(
        Path(__file__).resolve().parent.parent / "build" / "binaries" / version / "valkey-server"
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--window-sec", type=float, default=30.0)
    parser.add_argument("--out-dir", default="tests/bench/results")
    args = parser.parse_args()

    module_path = os.environ.get("MODULE_PATH")
    if not module_path:
        print("MODULE_PATH is required (path to libvalkey_flash.so)", file=sys.stderr)
        return 2
    server_bin = os.environ.get("SERVER_BIN", _default_server_bin())
    if not Path(server_bin).exists():
        print(f"valkey-server not found at {server_bin}", file=sys.stderr)
        return 2

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    results: list[CellResult] = []
    total_cells = len(SHAPES) * len(VALUE_SIZES)
    cell = 0
    for shape in SHAPES:
        for size_label, size_bytes in VALUE_SIZES:
            cell += 1
            print(f"[{cell}/{total_cells}] {shape:<6} {size_label:<6} ...", flush=True)
            with tempfile.TemporaryDirectory(prefix="flash_demo_bench_") as tmp:
                flash_path = os.path.join(tmp, "flash.bin")
                with running_server(module_path, server_bin, flash_path, CACHE_SIZE_BYTES) as (
                    client,
                    port,
                ):
                    r = measure_cell(client, port, size_label, size_bytes, shape, args.window_sec)
                    results.append(r)
                    print(
                        f"    demote={r.demotions_per_sec:,.0f}/s  "
                        f"tiered={r.tiered_keys_final:,}  "
                        f"GET p99={r.client_get_p99_us:.1f}µs"
                    )

    md = render_markdown(results, args.window_sec)
    # Filename matches the spec for the v1.1.0 baseline report so the CSV
    # is loadable by the adaptive-batch task's threshold tuner.
    (out_dir / "demotion_v1.1.0.md").write_text(md)
    write_csv(results, out_dir / "demotion_v1.1.0.csv")
    print(f"\nWrote {out_dir / 'demotion_v1.1.0.md'} and {out_dir / 'demotion_v1.1.0.csv'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
