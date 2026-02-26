#!/usr/bin/env python3
"""
Auto-discover and run all cargo benchmarks, combining JSON results.

Usage:
    python3 run_all_benchmarks.py [iterations] [num_buffers] [output_file]

Arguments:
    iterations   - Ignored (Criterion controls iteration count)
    num_buffers  - Buffer pool size for benchmarks (default: 12)
    output_file  - Output JSON file path (default: all_benchmarks.json)

Criterion uses --output-format bencher which produces lines like:
    test <name> ... bench: <ns> ns/iter (+/- <dev>)

Output JSON format (github-action-benchmark compatible):
    [
        {"name": "benchmark_name", "unit": "ns", "value": 12345},
        ...
    ]
"""

import glob
import json
import os
import re
import subprocess
import sys
from pathlib import Path

BENCHER_RE = re.compile(
    r"^test (.+?) \.\.\. bench:\s+([\d,]+) ns/iter \(\+/-\s+([\d,]+)\)$"
)


def format_ns(ns):
    """Convert nanoseconds to human-readable format."""
    if ns < 1000:
        return f"{ns:.0f}ns"
    elif ns < 1_000_000:
        return f"{ns/1000:.2f}µs"
    elif ns < 1_000_000_000:
        return f"{ns/1_000_000:.2f}ms"
    else:
        return f"{ns/1_000_000_000:.2f}s"


def parse_bencher_output(stdout: str):
    """
    Parse Criterion --output-format bencher text output.
    Lines look like:
        test Phase1/Core Latency/Pin/Unpin (hit) ... bench: 1157 ns/iter (+/- 42)
    Returns list of {"name": str, "unit": "ns", "value": float} dicts.
    """
    results = []
    for line in stdout.splitlines():
        m = BENCHER_RE.match(line.strip())
        if m:
            name = m.group(1).strip()
            value = float(m.group(2).replace(",", ""))
            results.append({"name": name, "unit": "ns", "value": value})
    return results


def run_benchmark(bench_name, num_buffers):
    """
    Run a single benchmark with Criterion --output-format bencher and return results.

    Returns:
        list: Benchmark results as list of dicts

    Raises:
        RuntimeError: If benchmark fails to run or produce valid output
    """
    print(f"Running: {bench_name}", file=sys.stderr)

    env = os.environ.copy()
    env["SIMPLEDB_BENCH_BUFFERS"] = str(num_buffers)
    env.setdefault("CARGO_TERM_COLOR", "never")

    cmd = [
        'cargo', 'bench', '--bench', bench_name,
        '--', '--output-format', 'bencher', '--noplot'
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            env=env,
        )

        results = parse_bencher_output(result.stdout)

        if not results:
            raise RuntimeError(
                f"No bench results found in output for '{bench_name}'. "
                f"stdout was:\n{result.stdout[:500]}"
            )

        return results

    except subprocess.CalledProcessError as e:
        print(f"\nERROR: Failed to run benchmark '{bench_name}'", file=sys.stderr)
        print(f"Command: {' '.join(cmd)}", file=sys.stderr)
        print(f"Exit code: {e.returncode}", file=sys.stderr)
        if e.stderr:
            print(f"Error output:\n{e.stderr}", file=sys.stderr)
        print(f"\nMake sure '{bench_name}' is defined in Cargo.toml as [[bench]]", file=sys.stderr)
        raise RuntimeError(f"Benchmark {bench_name} failed") from e


def discover_benchmarks():
    """
    Discover all benchmark files in benches/ directory.

    Returns:
        list: Benchmark names (without .rs extension)
    """
    bench_files = glob.glob("benches/*.rs")
    benchmarks = []

    for bench_file in sorted(bench_files):
        filename = Path(bench_file).name
        if filename != "README.md":
            benchmarks.append(Path(bench_file).stem)

    return benchmarks


def main():
    """Main entry point."""
    # Parse arguments (iterations is ignored — Criterion controls count)
    _iterations = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    num_buffers = int(sys.argv[2]) if len(sys.argv) > 2 else 12
    output_file = sys.argv[3] if len(sys.argv) > 3 else "all_benchmarks.json"

    print(f"Running all benchmarks (num_buffers={num_buffers})...", file=sys.stderr)

    # Discover benchmarks
    print("Discovering benchmarks in benches/...", file=sys.stderr)
    benchmarks = discover_benchmarks()

    if not benchmarks:
        print("ERROR: No benchmarks found in benches/", file=sys.stderr)
        sys.exit(1)

    # Run all benchmarks and collect results
    all_results = []
    for bench_name in benchmarks:
        try:
            results = run_benchmark(bench_name, num_buffers)
            all_results.extend(results)
        except RuntimeError:
            sys.exit(1)

    # Write combined results to file
    print("Combining results...", file=sys.stderr)
    with open(output_file, 'w') as f:
        json.dump(all_results, f, indent=2)

    print(f"All benchmark results written to {output_file}", file=sys.stderr)

    # Print summary to stderr
    print("\nSummary:", file=sys.stderr)
    for item in all_results:
        name = item['name']
        value = item['value']
        print(f"  {name}: {format_ns(value)}", file=sys.stderr)

    print(f"\nTotal benchmarks: {len(all_results)}", file=sys.stderr)


if __name__ == "__main__":
    main()
