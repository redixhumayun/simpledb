#!/usr/bin/env python3
"""
Run the regime-based validation matrix: 3 regimes × 2 I/O modes (buffered / direct-io).

Usage:
    python3 run_regime_matrix.py [iterations] [output_dir]

Arguments:
    iterations  - Iterations per benchmark cell (default: 50)
    output_dir  - Directory for JSON and comparison outputs (default: regime_matrix_results)

For each regime in [hot, pressure, thrash]:
  - Run buffered build:   cargo bench --bench io_patterns --no-default-features
                            --features replacement_lru --features page-4k
                            -- <iters> 12 --regime <regime> --json
  - Run direct-io build:  same + --features direct-io
  - Compare pair via compare_benchmarks.py → <output_dir>/compare_<regime>.md
  - Print per-regime and aggregate summary.

Regime definitions (resolved at runtime by the bench binary using /proc/meminfo):
  hot      = 0.25 × RAM
  pressure = 1.0  × RAM
  thrash   = 2.0  × RAM
"""

import json
import subprocess
import sys
from pathlib import Path


REGIMES = ["hot", "pressure", "thrash"]

BASE_FEATURES = [
    "--no-default-features",
    "--features", "replacement_lru",
    "--features", "page-4k",
]

DIRECT_EXTRA = ["--features", "direct-io"]


def detect_filesystem():
    """Return a short description of the filesystem/device for the current directory."""
    try:
        result = subprocess.run(
            ["df", "-T", "."], capture_output=True, text=True, check=True
        )
        # df -T output: Filesystem  Type  1K-blocks  Used  Available  Use%  Mounted on
        lines = result.stdout.strip().splitlines()
        if len(lines) >= 2:
            parts = lines[1].split()
            if len(parts) >= 7:
                device, fstype = parts[0], parts[1]
                return f"{device} ({fstype})"
    except Exception:
        pass
    return "unknown"


def format_ns(ns):
    """Convert nanoseconds to human-readable format."""
    if ns < 1_000:
        return f"{ns:.0f}ns"
    elif ns < 1_000_000:
        return f"{ns / 1_000:.2f}µs"
    elif ns < 1_000_000_000:
        return f"{ns / 1_000_000:.2f}ms"
    else:
        return f"{ns / 1_000_000_000:.2f}s"


def run_bench_cell(regime, iterations, features_extra, label):
    """Run io_patterns bench for one (regime, mode) cell. Returns list of result dicts."""
    cmd = (
        ["cargo", "bench", "--bench", "io_patterns"]
        + BASE_FEATURES
        + features_extra
        + ["--", str(iterations), "12", "--regime", regime, "--json"]
    )
    print(f"  [{label}] running: {' '.join(cmd)}", file=sys.stderr)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"\nERROR: bench failed for {label} / {regime}", file=sys.stderr)
        print(f"Command: {' '.join(cmd)}", file=sys.stderr)
        if e.stderr:
            print(e.stderr, file=sys.stderr)
        raise RuntimeError(f"bench failed: {label}/{regime}") from e

    for line in result.stdout.splitlines():
        line = line.strip()
        if line.startswith("[") and line.endswith("]"):
            try:
                return json.loads(line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Bad JSON from {label}/{regime}: {exc}") from exc

    raise RuntimeError(f"No JSON output from {label}/{regime}")


def run_compare(base_file, pr_file, out_file, label=None):
    """Call compare_benchmarks.py and return the markdown text."""
    cmd = ["python3", "scripts/compare_benchmarks.py", str(base_file), str(pr_file), str(out_file)]
    if label:
        cmd.append(label)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"WARNING: compare_benchmarks.py failed: {result.stderr}", file=sys.stderr)
    return out_file.read_text() if out_file.exists() else ""


def summarise_regime(regime, buffered, direct):
    """Print a compact per-regime delta table to stderr."""
    direct_map = {r["name"]: r["value"] for r in direct}
    print(f"\n  Regime: {regime}", file=sys.stderr)
    print(f"  {'Benchmark':<55} {'Buffered':>12} {'Direct':>12} {'Change':>9}", file=sys.stderr)
    print(f"  {'-'*92}", file=sys.stderr)
    for item in buffered:
        name = item["name"]
        b = item["value"]
        d = direct_map.get(name, b)
        pct = ((d - b) / b * 100) if b else 0
        flag = " 🚀" if pct < -5 else (" ⚠" if pct > 5 else "")
        print(
            f"  {name:<55} {format_ns(b):>12} {format_ns(d):>12} {pct:>+8.1f}%{flag}",
            file=sys.stderr,
        )


def main():
    iterations = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    output_dir = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("regime_matrix_results")
    output_dir.mkdir(parents=True, exist_ok=True)

    filesystem = detect_filesystem()
    print(f"Regime matrix: {iterations} iterations, output → {output_dir}", file=sys.stderr)
    print(f"Regimes: {REGIMES}", file=sys.stderr)
    print(f"Filesystem/device: {filesystem}", file=sys.stderr)

    all_buffered = {}
    all_direct = {}

    for regime in REGIMES:
        print(f"\n=== Regime: {regime} ===", file=sys.stderr)

        # Buffered
        buffered_file = output_dir / f"buffered_{regime}.json"
        buffered = run_bench_cell(regime, iterations, [], "buffered")
        buffered_file.write_text(json.dumps(buffered, indent=2))
        print(f"  Saved: {buffered_file}", file=sys.stderr)

        # Direct-io
        direct_file = output_dir / f"direct_{regime}.json"
        direct = run_bench_cell(regime, iterations, DIRECT_EXTRA, "direct-io")
        direct_file.write_text(json.dumps(direct, indent=2))
        print(f"  Saved: {direct_file}", file=sys.stderr)

        # Compare
        compare_file = output_dir / f"compare_{regime}.md"
        run_compare(buffered_file, direct_file, compare_file, label=regime)
        print(f"  Comparison: {compare_file}", file=sys.stderr)

        summarise_regime(regime, buffered, direct)
        all_buffered[regime] = buffered
        all_direct[regime] = direct

    # Aggregate summary across all regimes
    print("\n\n=== Aggregate Summary ===", file=sys.stderr)
    for regime in REGIMES:
        buffered = all_buffered[regime]
        direct = all_direct[regime]
        direct_map = {r["name"]: r["value"] for r in direct}

        faster = sum(
            1 for r in buffered if ((direct_map.get(r["name"], r["value"]) - r["value"]) / r["value"] * 100) < -5
        )
        slower = sum(
            1 for r in buffered if ((direct_map.get(r["name"], r["value"]) - r["value"]) / r["value"] * 100) > 5
        )
        neutral = len(buffered) - faster - slower
        print(
            f"  {regime:<10}: {faster} faster  {neutral} neutral  {slower} slower  "
            f"(out of {len(buffered)} benchmarks)",
            file=sys.stderr,
        )

    print(f"\nAll outputs in: {output_dir}", file=sys.stderr)


if __name__ == "__main__":
    main()
