#!/usr/bin/env python3
"""
Compare benchmark results between two runs (e.g., base vs PR).

Usage:
    python3 compare_benchmarks.py base.json pr.json output.md

Reads two JSON files containing benchmark results and generates a markdown
comparison table showing performance changes.

Input JSON format (from run_all_benchmarks.sh):
    [
        {"name": "benchmark_name", "unit": "ns", "value": 12345},
        ...
    ]

Output markdown includes:
    - Side-by-side comparison table
    - Percentage change calculations
    - Visual status indicators (✅ no change, ⚠️ slower, 🚀 faster)
"""

import json
import sys


def load_json(filename):
    """Load benchmark results from JSON file."""
    with open(filename, 'r') as f:
        return json.load(f)


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


def calculate_change(base, pr):
    """Calculate percentage change from base to PR."""
    if base == 0:
        return 0
    return ((pr - base) / base) * 100


def compare_benchmarks(base_file, pr_file):
    """Generate markdown comparison table from two benchmark JSON files."""
    base_data = load_json(base_file)
    pr_data = load_json(pr_file)

    # Create lookup dict for PR results
    pr_lookup = {item['name']: item['value'] for item in pr_data}

    lines = []
    lines.append("| Benchmark | Base | PR | Change | Status |")
    lines.append("|-----------|------|----|---------:|--------|")

    for base_item in base_data:
        name = base_item['name']
        base_val = base_item['value']
        pr_val = pr_lookup.get(name, base_val)

        change = calculate_change(base_val, pr_val)

        # Determine status emoji
        if abs(change) < 5:
            status = "✅"
        elif change > 5:
            status = "⚠️ slower"
        else:
            status = "🚀 faster"

        lines.append(
            f"| {name} | {format_ns(base_val)} | {format_ns(pr_val)} | "
            f"{change:+.2f}% | {status} |"
        )

    return "\n".join(lines)


def main():
    """Main entry point."""
    if len(sys.argv) != 4:
        print("Usage: compare_benchmarks.py <base.json> <pr.json> <output.md>", file=sys.stderr)
        sys.exit(1)

    base_file = sys.argv[1]
    pr_file = sys.argv[2]
    output_file = sys.argv[3]

    # Generate full report
    report = ["## 📊 Benchmark Comparison Report\n"]
    report.append(compare_benchmarks(base_file, pr_file))
    report.append("\n---")
    report.append("\n**Note:** Changes < 5% are considered within normal variance.")
    report.append("\n⚠️ = Performance regression detected")
    report.append("\n🚀 = Performance improvement detected")

    report_text = "\n".join(report)

    # Print to stdout
    print(report_text)

    # Write to file for PR comment
    with open(output_file, 'w') as f:
        f.write(report_text)

    print(f"\nComparison report written to {output_file}", file=sys.stderr)


if __name__ == "__main__":
    main()
