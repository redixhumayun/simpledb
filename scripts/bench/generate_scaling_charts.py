#!/usr/bin/env -S uv run python
"""Generate scaling charts from benchmark JSON data.

This script reads benchmark data for different replacement policies and
generates line charts showing how throughput scales with thread count for
various access patterns.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_ROOT = REPO_ROOT / "docs" / "benchmarks" / "replacement_policies" / "raw"
CHARTS_DIR = REPO_ROOT / "docs" / "benchmarks" / "charts"

POLICY_ORDER = [
    "replacement_lru",
    "replacement_clock",
    "replacement_sieve",
]

POLICY_DISPLAY = {
    "replacement_lru": "LRU",
    "replacement_clock": "Clock",
    "replacement_sieve": "SIEVE",
}

POLICY_COLORS = {
    "replacement_lru": "#2E86AB",      # Blue
    "replacement_clock": "#A23B72",    # Purple
    "replacement_sieve": "#F18F01",    # Orange
}

THREAD_COUNTS = [1, 2, 4, 8, 16, 32, 64, 128, 256]


@dataclass
class ChartSpec:
    """Specification for a scaling chart."""
    filename: str
    title: str
    bench_patterns: List[str]  # Benchmark name patterns to match for each thread count
    total_ops: int  # Total operations for throughput calculation
    unit: str  # "ops" or "blocks"
    ylabel: str


CHART_SPECS = [
    ChartSpec(
        filename="pin_scaling.png",
        title="PIN Concurrency Scaling",
        bench_patterns=["Concurrent ({} threads, {} ops)"],
        total_ops=10_000,
        unit="ops",
        ylabel="Throughput (M ops/s)",
    ),
    ChartSpec(
        filename="hotset_scaling.png",
        title="Hotset Contention Scaling (K=4)",
        bench_patterns=["Concurrent Hotset ({} threads, K=4, {} ops)"],
        total_ops=10_000,
        unit="ops",
        ylabel="Throughput (M ops/s)",
    ),
    ChartSpec(
        filename="sequential_scaling.png",
        title="Sequential Scan Scaling",
        bench_patterns=[
            "Sequential Scan ({} blocks)",
            "Seq Scan MT x{} ({} blocks)",
        ],
        total_ops=120,  # num_buffers * 10, assuming num_buffers=12
        unit="blocks",
        ylabel="Throughput (M blocks/s)",
    ),
    ChartSpec(
        filename="repeated_scaling.png",
        title="Repeated Access Scaling",
        bench_patterns=[
            "Repeated Access ({} ops)",
            "Repeated Access MT x{} ({} ops)",
        ],
        total_ops=1000,
        unit="ops",
        ylabel="Throughput (M ops/s)",
    ),
    ChartSpec(
        filename="random_k10_scaling.png",
        title="Random Access Scaling (K=10)",
        bench_patterns=[
            "Random (K=10, {} ops)",
            "Random MT x{} (K=10, {} ops)",
        ],
        total_ops=500,
        unit="ops",
        ylabel="Throughput (M ops/s)",
    ),
    ChartSpec(
        filename="random_k50_scaling.png",
        title="Random Access Scaling (K=50)",
        bench_patterns=[
            "Random (K=50, {} ops)",
            "Random MT x{} (K=50, {} ops)",
        ],
        total_ops=500,
        unit="ops",
        ylabel="Throughput (M ops/s)",
    ),
    ChartSpec(
        filename="random_k100_scaling.png",
        title="Random Access Scaling (K=100)",
        bench_patterns=[
            "Random (K=100, {} ops)",
            "Random MT x{} (K=100, {} ops)",
        ],
        total_ops=500,
        unit="ops",
        ylabel="Throughput (M ops/s)",
    ),
    ChartSpec(
        filename="zipfian_scaling.png",
        title="Zipfian Access Scaling (80/20)",
        bench_patterns=[
            "Zipfian (80/20, {} ops)",
            "Zipfian MT x{} (80/20, {} ops)",
        ],
        total_ops=500,
        unit="ops",
        ylabel="Throughput (M ops/s)",
    ),
]


def load_metadata(platform: str) -> Dict:
    """Load metadata JSON for a platform."""
    metadata_path = RAW_ROOT / platform / "metadata.json"
    if not metadata_path.exists():
        raise FileNotFoundError(f"Missing metadata for platform '{platform}' at {metadata_path}")
    return json.loads(metadata_path.read_text())


def load_policy_json(info: Dict) -> Optional[Dict]:
    """Load JSON benchmark results for a policy."""
    json_path = info.get("json_path")
    if not json_path:
        return None
    json_file = (REPO_ROOT / json_path).resolve()
    if not json_file.exists():
        return None
    results = json.loads(json_file.read_text())
    return {entry["name"]: entry["value"] for entry in results}


def compute_throughput(mean_ns: Optional[float], total_ops: int) -> Optional[float]:
    """Convert mean latency (ns) to throughput (ops/s)."""
    if mean_ns is None:
        return None
    seconds = mean_ns / 1_000_000_000.0
    if seconds == 0:
        return None
    return total_ops / seconds


def get_bench_name(spec: ChartSpec, thread_count: int) -> Optional[str]:
    """Get the expected benchmark name for a given thread count."""
    if thread_count == 1:
        # Single-threaded uses first pattern
        pattern = spec.bench_patterns[0]
        # Handle different patterns for single-threaded
        if "Concurrent" in pattern:
            ops_per_thread = spec.total_ops // thread_count
            return pattern.format(thread_count, ops_per_thread)
        elif "Seq Scan MT" in pattern or "Repeated Access MT" in pattern or "Random MT" in pattern or "Zipfian MT" in pattern:
            # For access patterns, use the ST version from first pattern
            return spec.bench_patterns[0].format(spec.total_ops)
        else:
            return spec.bench_patterns[0].format(spec.total_ops)
    else:
        # Multi-threaded uses second pattern if available
        if len(spec.bench_patterns) > 1:
            pattern = spec.bench_patterns[1]
        else:
            pattern = spec.bench_patterns[0]

        # Format the pattern based on what placeholders it expects
        if "Concurrent Hotset" in pattern or "Concurrent" in pattern:
            ops_per_thread = spec.total_ops // thread_count
            return pattern.format(thread_count, ops_per_thread)
        elif "{} blocks)" in pattern:
            return pattern.format(thread_count, spec.total_ops)
        elif "{} ops)" in pattern:
            return pattern.format(thread_count, spec.total_ops)
        else:
            return None


def extract_scaling_data(spec: ChartSpec, platform_data: Dict) -> Dict[str, List[Optional[float]]]:
    """Extract throughput data for all thread counts and policies."""
    scaling_data = {policy: [] for policy in POLICY_ORDER}

    for thread_count in THREAD_COUNTS:
        bench_name = get_bench_name(spec, thread_count)
        if not bench_name:
            # No matching benchmark for this thread count
            for policy in POLICY_ORDER:
                scaling_data[policy].append(None)
            continue

        for policy in POLICY_ORDER:
            policy_means = platform_data.get(policy)
            if not policy_means:
                scaling_data[policy].append(None)
                continue

            mean_ns = policy_means.get(bench_name)
            throughput = compute_throughput(mean_ns, spec.total_ops)
            if throughput:
                # Convert to millions
                throughput_millions = throughput / 1_000_000.0
                scaling_data[policy].append(throughput_millions)
            else:
                scaling_data[policy].append(None)

    return scaling_data


def create_scaling_chart(spec: ChartSpec, platform_data: Dict, output_path: Path, platform: str):
    """Create and save a scaling chart."""
    scaling_data = extract_scaling_data(spec, platform_data)

    # Check if we have any data
    has_data = any(any(v is not None for v in values) for values in scaling_data.values())
    if not has_data:
        print(f"Warning: No data found for {spec.title}, skipping chart generation")
        return

    # Create figure with high resolution for social media
    fig, ax = plt.subplots(figsize=(12, 8), dpi=150)

    # Plot lines for each policy
    for policy in POLICY_ORDER:
        values = scaling_data[policy]
        # Filter out None values for plotting
        plot_threads = [t for t, v in zip(THREAD_COUNTS, values) if v is not None]
        plot_values = [v for v in values if v is not None]

        if plot_values:
            ax.plot(
                plot_threads,
                plot_values,
                marker='o',
                markersize=8,
                linewidth=2.5,
                label=POLICY_DISPLAY[policy],
                color=POLICY_COLORS[policy],
            )

    # Formatting
    ax.set_xscale('log', base=2)
    ax.set_xlabel('Thread Count', fontsize=14, fontweight='bold')
    ax.set_ylabel(spec.ylabel, fontsize=14, fontweight='bold')

    # Add platform to title
    platform_display = platform.title() if platform != "macos" else "macOS"
    title_with_platform = f"{spec.title} ({platform_display})"
    ax.set_title(title_with_platform, fontsize=16, fontweight='bold', pad=20)

    # Set x-axis ticks to thread counts
    ax.set_xticks(THREAD_COUNTS)
    ax.set_xticklabels([str(t) for t in THREAD_COUNTS])

    # Grid
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.set_axisbelow(True)

    # Legend
    ax.legend(loc='best', fontsize=12, framealpha=0.9)

    # Add data source footnote
    data_source = f"Data: docs/benchmarks/replacement_policies/raw/{platform}/metadata.json"
    fig.text(0.99, 0.01, data_source, ha='right', va='bottom', fontsize=8, style='italic', color='gray')

    # Tight layout
    plt.tight_layout()

    # Save
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()

    print(f"Generated: {output_path.name}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--platform", default="macos", help="Platform to generate charts for")
    args = parser.parse_args()

    # Load metadata
    metadata = load_metadata(args.platform)

    # Load policy data
    platform_data = {}
    for policy in POLICY_ORDER:
        info = metadata.get("policies", {}).get(policy)
        if not info:
            print(f"Warning: No info for {policy}")
            continue
        policy_means = load_policy_json(info)
        if policy_means:
            platform_data[policy] = policy_means

    if not platform_data:
        print(f"Error: No policy data found for platform '{args.platform}'")
        return

    # Generate charts
    print(f"Generating scaling charts for {args.platform}...")

    # Create platform-specific directory
    platform_charts_dir = CHARTS_DIR / args.platform
    platform_charts_dir.mkdir(parents=True, exist_ok=True)

    for spec in CHART_SPECS:
        output_path = platform_charts_dir / spec.filename
        create_scaling_chart(spec, platform_data, output_path, args.platform)

    print(f"\nAll charts saved to: {platform_charts_dir}")
    print(f"Platform: {args.platform}")
    print(f"Data source: {RAW_ROOT / args.platform / 'metadata.json'}")


if __name__ == "__main__":
    main()
