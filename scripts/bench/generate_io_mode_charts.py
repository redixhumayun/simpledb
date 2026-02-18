#!/usr/bin/env -S uv run python
"""Generate compact direct-vs-buffered I/O comparison charts.

This script reads regime-matrix JSON artifacts from a results directory and
renders a small chart deck suitable for sharing:

1. Per-regime grouped bar charts:
   - io_mode_hot.png
   - io_mode_pressure.png
   - io_mode_thrash.png

Input directory must contain:
  buffered_hot.json, direct_hot.json,
  buffered_pressure.json, direct_pressure.json,
  buffered_thrash.json, direct_thrash.json
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np

from config import REPO_ROOT

REGIMES = ["hot", "pressure", "thrash"]


@dataclass(frozen=True)
class BenchSpec:
    label: str
    prefix: str


# Compact deck allowlist (11 rows).
BENCHMARKS: List[BenchSpec] = [
    BenchSpec("Sequential Read", "Sequential Read ("),
    BenchSpec("Random Read", "Random Read ("),
    BenchSpec("Sequential Write", "Sequential Write ("),
    BenchSpec("Random Write", "Random Write ("),
    BenchSpec(
        "Durability data-nosync",
        "Random Write durability immediate-fsync data-nosync",
    ),
    BenchSpec(
        "Durability data-fsync",
        "Random Write durability immediate-fsync data-fsync",
    ),
    BenchSpec("One-pass Seq Scan", "One-pass Seq Scan ("),
    BenchSpec("Low-locality Rand Read", "Low-locality Rand Read ("),
    BenchSpec("One-pass Seq Scan+Evict", "One-pass Seq Scan+Evict ("),
    BenchSpec("Low-locality Rand Read+Evict", "Low-locality Rand Read+Evict ("),
    BenchSpec("Multi-stream Scan", "Multi-stream Scan"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "results_dir",
        help="Path to a regime-matrix results dir (e.g., results/regime_capped_YYYYMMDD_HHMMSS)",
    )
    parser.add_argument(
        "--output-dir",
        default="docs/benchmarks/charts/io_mode",
        help="Output directory for chart images",
    )
    return parser.parse_args()


def load_result_map(path: Path) -> Dict[str, float]:
    payload = json.loads(path.read_text())
    return {item["name"]: float(item["value"]) for item in payload}


def find_bench_name(result_map: Dict[str, float], prefix: str) -> Optional[str]:
    matches = [name for name in result_map if name.startswith(prefix)]
    if not matches:
        return None
    if len(matches) > 1:
        raise RuntimeError(f"Ambiguous benchmark prefix '{prefix}' matched {matches}")
    return matches[0]


def resolve_values(
    buffered: Dict[str, float],
    direct: Dict[str, float],
    specs: List[BenchSpec],
) -> List[Tuple[str, float, float]]:
    rows: List[Tuple[str, float, float]] = []
    for spec in specs:
        key = find_bench_name(buffered, spec.prefix)
        if key is None:
            raise RuntimeError(f"Missing benchmark prefix '{spec.prefix}' in buffered results")
        if key not in direct:
            raise RuntimeError(f"Benchmark '{key}' missing in direct results")
        rows.append((spec.label, buffered[key], direct[key]))
    return rows


def ns_to_ms(values: List[float]) -> np.ndarray:
    return np.array(values, dtype=float) / 1_000_000.0


def make_regime_bar_chart(regime: str, rows: List[Tuple[str, float, float]], output_path: Path) -> None:
    labels = [r[0] for r in rows]
    buffered_ms = ns_to_ms([r[1] for r in rows])
    direct_ms = ns_to_ms([r[2] for r in rows])

    x = np.arange(len(labels))
    width = 0.38

    fig, ax = plt.subplots(figsize=(14, 7), dpi=140)
    ax.bar(x - width / 2, buffered_ms, width, label="Buffered", color="#2E86AB")
    ax.bar(x + width / 2, direct_ms, width, label="Direct", color="#F18F01")

    # Log scale keeps large latency spread readable.
    ax.set_yscale("log")
    ax.set_ylabel("Latency (ms, log scale)")
    ax.set_title(f"Direct vs Buffered I/O ({regime})")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=30, ha="right")
    ax.grid(True, axis="y", linestyle="--", alpha=0.3)
    ax.set_axisbelow(True)
    ax.legend()

    fig.tight_layout()
    fig.savefig(output_path)
    plt.close(fig)


def main() -> None:
    args = parse_args()
    results_dir = (REPO_ROOT / args.results_dir).resolve()
    output_dir = (REPO_ROOT / args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    per_regime_rows: Dict[str, List[Tuple[str, float, float]]] = {}

    for regime in REGIMES:
        buffered_path = results_dir / f"buffered_{regime}.json"
        direct_path = results_dir / f"direct_{regime}.json"
        if not buffered_path.exists() or not direct_path.exists():
            raise FileNotFoundError(
                f"Missing regime files for '{regime}' in {results_dir}. "
                f"Expected {buffered_path.name} and {direct_path.name}"
            )

        buffered = load_result_map(buffered_path)
        direct = load_result_map(direct_path)
        rows = resolve_values(buffered, direct, BENCHMARKS)
        per_regime_rows[regime] = rows

        out_path = output_dir / f"io_mode_{regime}.png"
        make_regime_bar_chart(regime, rows, out_path)
        print(f"Wrote {out_path.relative_to(REPO_ROOT)}")

if __name__ == "__main__":
    main()
