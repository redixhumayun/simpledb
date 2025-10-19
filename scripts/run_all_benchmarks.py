#!/usr/bin/env python3
"""
Auto-discover and run all cargo benchmarks, combining JSON results.

Usage:
    python3 run_all_benchmarks.py [iterations] [num_buffers] [output_file]

Arguments:
    iterations   - Number of iterations per benchmark (default: 50)
    num_buffers  - Buffer pool size for benchmarks (default: 12)
    output_file  - Output JSON file path (default: all_benchmarks.json)

LIMITATION: All .rs files in benches/ are assumed to be benchmarks defined
in Cargo.toml. Do NOT add helper modules directly in benches/ (e.g., benches/common.rs).
Instead, use benches/common/mod.rs or similar structure.

Output JSON format:
    [
        {"name": "benchmark_name", "unit": "ns", "value": 12345},
        ...
    ]
"""

import glob
import json
import subprocess
import sys
from pathlib import Path


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


def run_benchmark(bench_name, iterations, num_buffers):
    """
    Run a single benchmark and return JSON results.

    Returns:
        list: Benchmark results as list of dicts

    Raises:
        RuntimeError: If benchmark fails to run or produce valid JSON
    """
    print(f"Running: {bench_name}", file=sys.stderr)

    cmd = [
        'cargo', 'bench', '--bench', bench_name, '--',
        str(iterations), str(num_buffers), '--json'
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )

        # Find JSON array in output (benchmarks print to stdout)
        for line in result.stdout.splitlines():
            line = line.strip()
            if line.startswith('[') and line.endswith(']'):
                try:
                    return json.loads(line)
                except json.JSONDecodeError as e:
                    raise RuntimeError(f"Invalid JSON from {bench_name}: {e}")

        raise RuntimeError(
            f"No JSON output found for benchmark '{bench_name}'. "
            f"Make sure it implements --json flag support."
        )

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
    # Parse arguments
    iterations = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    num_buffers = int(sys.argv[2]) if len(sys.argv) > 2 else 12
    output_file = sys.argv[3] if len(sys.argv) > 3 else "all_benchmarks.json"

    print(f"Running all benchmarks with {iterations} iterations...", file=sys.stderr)

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
            results = run_benchmark(bench_name, iterations, num_buffers)
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
