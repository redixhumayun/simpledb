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


def _has_rich_stats(data):
    """Return True if any item in data contains p50/p95/std_dev fields."""
    return any('p50' in item or 'p95' in item or 'std_dev' in item for item in data)


def _fmt(v):
    """Format a nanosecond value, or return '-' if absent."""
    return format_ns(v) if isinstance(v, (int, float)) else '-'


def compare_benchmarks(base_file, pr_file, label=None):
    """Generate markdown comparison table from two benchmark JSON files."""
    base_data = load_json(base_file)
    pr_data = load_json(pr_file)

    # Create lookup dict for PR results (keyed by name)
    pr_lookup = {item['name']: item for item in pr_data}

    rich = _has_rich_stats(base_data) or _has_rich_stats(pr_data)

    lines = []
    if label:
        lines.append(f"**Regime: {label}**\n")
    if rich:
        lines.append("| Benchmark | Base mean | PR mean | Change | Base p50 | PR p50 | Base p95 | PR p95 | Base σ | PR σ | Status |")
        lines.append("|-----------|----------:|--------:|-------:|---------:|-------:|---------:|-------:|-------:|-----:|--------|")
    else:
        lines.append("| Benchmark | Base | PR | Change | Status |")
        lines.append("|-----------|------|----|---------:|--------|")

    for base_item in base_data:
        name = base_item['name']
        base_val = base_item['value']
        pr_item = pr_lookup.get(name)

        if pr_item is None:
            # Benchmark present in base but missing from PR — flag explicitly
            if rich:
                lines.append(f"| {name} | {format_ns(base_val)} | — | N/A | - | - | - | - | - | - | ❌ missing |")
            else:
                lines.append(f"| {name} | {format_ns(base_val)} | — | N/A | ❌ missing |")
            continue

        pr_val = pr_item['value']
        change = calculate_change(base_val, pr_val)

        # Determine status emoji
        if abs(change) < 5:
            status = "✅"
        elif change > 5:
            status = "⚠️ slower"
        else:
            status = "🚀 faster"

        if rich:
            base_p50 = base_item.get('p50', base_item.get('value'))
            base_p95 = base_item.get('p95', '')
            base_sd = base_item.get('std_dev', '')
            pr_p50 = pr_item.get('p50', pr_item.get('value', ''))
            pr_p95 = pr_item.get('p95', '')
            pr_sd = pr_item.get('std_dev', '')

            lines.append(
                f"| {name} | {format_ns(base_val)} | {format_ns(pr_val)} | "
                f"{change:+.2f}% | {_fmt(base_p50)} | {_fmt(pr_p50)} | "
                f"{_fmt(base_p95)} | {_fmt(pr_p95)} | {_fmt(base_sd)} | {_fmt(pr_sd)} | {status} |"
            )
        else:
            lines.append(
                f"| {name} | {format_ns(base_val)} | {format_ns(pr_val)} | "
                f"{change:+.2f}% | {status} |"
            )

    return "\n".join(lines)


def main():
    """Main entry point."""
    if len(sys.argv) < 4:
        print("Usage: compare_benchmarks.py <base.json> <pr.json> <output.md> [label]", file=sys.stderr)
        sys.exit(1)

    base_file = sys.argv[1]
    pr_file = sys.argv[2]
    output_file = sys.argv[3]
    label = sys.argv[4] if len(sys.argv) > 4 else None

    # Generate full report
    report = ["## 📊 Benchmark Comparison Report\n"]
    report.append(compare_benchmarks(base_file, pr_file, label=label))
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
