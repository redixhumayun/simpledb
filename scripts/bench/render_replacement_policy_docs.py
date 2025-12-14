#!/usr/bin/env -S uv run python
"""Render replacement-policy docs and summary tables from JSON artifacts.

This script consumes the raw outputs captured by
`scripts/bench/run_buffer_pool.py` and regenerates:

1. Platform-specific log bundles
   (`docs/benchmarks/replacement_policies/<platform>_buffer_pool.md`).
2. The replacement-policy comparison tables inside `benches/README.md`.

It relies on the metadata/JSON/text files stored under
`docs/benchmarks/replacement_policies/raw/<platform>/`.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_ROOT = REPO_ROOT / "docs" / "benchmarks" / "replacement_policies" / "raw"

POLICY_ORDER = [
    "replacement_lru",
    "replacement_clock",
    "replacement_sieve",
]


@dataclass
class RowSpec:
    label: str
    bench_name: str
    kind: str  # "latency" or "throughput"
    unit: str  # For throughput: "ops" or "blocks"; ignored for latency
    ops_fn: Optional[Callable[[int], int]] = None
    hit_key: Optional[str] = None
    precision: int = 3


README_ROWS: List[RowSpec] = [
    RowSpec("Pin/Unpin hit", "Pin/Unpin (hit)", "latency", ""),
    RowSpec("Cold pin", "Cold Pin (miss)", "latency", ""),
    RowSpec(
        "Sequential Scan",
        "Sequential Scan (120 blocks)",
        "throughput",
        "blocks",
        lambda num_buffers: num_buffers * 10,
        "Sequential Scan",
    ),
    RowSpec(
        "Seq Scan MT x4",
        "Seq Scan MT x4 (120 blocks)",
        "throughput",
        "blocks",
        lambda num_buffers: num_buffers * 10,
    ),
    RowSpec(
        "Seq Scan MT x16",
        "Seq Scan MT x16 (120 blocks)",
        "throughput",
        "blocks",
        lambda num_buffers: num_buffers * 10,
    ),
    RowSpec(
        "Repeated Access",
        "Repeated Access (1000 ops)",
        "throughput",
        "ops",
        lambda _: 1000,
        "Repeated Access",
    ),
    RowSpec(
        "Repeated Access MT x4",
        "Repeated Access MT x4 (1000 ops)",
        "throughput",
        "ops",
        lambda _: 1000,
    ),
    RowSpec(
        "Repeated Access MT x16",
        "Repeated Access MT x16 (1000 ops)",
        "throughput",
        "ops",
        lambda _: 1000,
    ),
    RowSpec(
        "Random K=10",
        "Random (K=10, 500 ops)",
        "throughput",
        "ops",
        lambda _: 500,
        "Random (K=10)",
    ),
    RowSpec(
        "Random MT x4 K=10",
        "Random MT x4 (K=10, 500 ops)",
        "throughput",
        "ops",
        lambda _: 500,
    ),
    RowSpec(
        "Random MT x16 K=10",
        "Random MT x16 (K=10, 500 ops)",
        "throughput",
        "ops",
        lambda _: 500,
    ),
    RowSpec(
        "Random K=50",
        "Random (K=50, 500 ops)",
        "throughput",
        "ops",
        lambda _: 500,
        "Random (K=50)",
    ),
    RowSpec(
        "Random MT x4 K=50",
        "Random MT x4 (K=50, 500 ops)",
        "throughput",
        "ops",
        lambda _: 500,
    ),
    RowSpec(
        "Random MT x16 K=50",
        "Random MT x16 (K=50, 500 ops)",
        "throughput",
        "ops",
        lambda _: 500,
    ),
    RowSpec(
        "Random K=100",
        "Random (K=100, 500 ops)",
        "throughput",
        "ops",
        lambda _: 500,
        "Random (K=100)",
    ),
    RowSpec(
        "Random MT x4 K=100",
        "Random MT x4 (K=100, 500 ops)",
        "throughput",
        "ops",
        lambda _: 500,
    ),
    RowSpec(
        "Random MT x16 K=100",
        "Random MT x16 (K=100, 500 ops)",
        "throughput",
        "ops",
        lambda _: 500,
    ),
    RowSpec(
        "Zipfian",
        "Zipfian (80/20, 500 ops)",
        "throughput",
        "ops",
        lambda _: 500,
        "Zipfian (80/20)",
    ),
    RowSpec(
        "Zipfian MT x4",
        "Zipfian MT x4 (80/20, 500 ops)",
        "throughput",
        "ops",
        lambda _: 500,
    ),
    RowSpec(
        "Zipfian MT x16",
        "Zipfian MT x16 (80/20, 500 ops)",
        "throughput",
        "ops",
        lambda _: 500,
    ),
    RowSpec(
        "pin:t1",
        "Concurrent (1 threads, 10000 ops)",
        "throughput",
        "ops",
        lambda _: 10000,
    ),
    RowSpec(
        "pin:t2",
        "Concurrent (2 threads, 5000 ops)",
        "throughput",
        "ops",
        lambda _: 10000,
    ),
    RowSpec(
        "pin:t8",
        "Concurrent (8 threads, 1250 ops)",
        "throughput",
        "ops",
        lambda _: 10000,
    ),
    RowSpec(
        "pin:t16",
        "Concurrent (16 threads, 625 ops)",
        "throughput",
        "ops",
        lambda _: 10000,
    ),
    RowSpec(
        "pin:t64",
        "Concurrent (64 threads, 156 ops)",
        "throughput",
        "ops",
        lambda _: 10000,
    ),
    RowSpec(
        "pin:t256",
        "Concurrent (256 threads, 39 ops)",
        "throughput",
        "ops",
        lambda _: 10000,
    ),
    RowSpec(
        "hotset:t1_k4",
        "Concurrent Hotset (1 threads, K=4, 10000 ops)",
        "throughput",
        "ops",
        lambda _: 10000,
    ),
    RowSpec(
        "hotset:t2_k4",
        "Concurrent Hotset (2 threads, K=4, 5000 ops)",
        "throughput",
        "ops",
        lambda _: 10000,
    ),
    RowSpec(
        "hotset:t8_k4",
        "Concurrent Hotset (8 threads, K=4, 1250 ops)",
        "throughput",
        "ops",
        lambda _: 10000,
    ),
    RowSpec(
        "hotset:t16_k4",
        "Concurrent Hotset (16 threads, K=4, 625 ops)",
        "throughput",
        "ops",
        lambda _: 10000,
    ),
    RowSpec(
        "hotset:t64_k4",
        "Concurrent Hotset (64 threads, K=4, 156 ops)",
        "throughput",
        "ops",
        lambda _: 10000,
    ),
    RowSpec(
        "hotset:t256_k4",
        "Concurrent Hotset (256 threads, K=4, 39 ops)",
        "throughput",
        "ops",
        lambda _: 10000,
    ),
]



def load_metadata(platform: str) -> Dict[str, object]:
    metadata_path = RAW_ROOT / platform / "metadata.json"
    if not metadata_path.exists():
        raise FileNotFoundError(f"Missing metadata for platform '{platform}' at {metadata_path}")
    return json.loads(metadata_path.read_text())


def load_policy_data(info: Dict[str, object]) -> Optional[Dict[str, object]]:
    json_path = info.get("json_path")
    if not json_path:
        return None
    json_file = (REPO_ROOT / json_path).resolve()
    if not json_file.exists():
        return None
    results = json.loads(json_file.read_text())
    means = {entry["name"]: entry["value"] for entry in results}

    hit_rates = info.get("hit_rates", {})
    log_path = info.get("log_path")
    log_text = None
    if log_path:
        log_file = (REPO_ROOT / log_path).resolve()
        if log_file.exists():
            log_text = log_file.read_text().rstrip()

    return {
        "display": info.get("display", ""),
        "means": means,
        "hit_rates": hit_rates,
        "log_text": log_text,
    }


def format_latency(ns_value: Optional[float]) -> str:
    if ns_value is None:
        return "—"
    micros = ns_value / 1000.0
    if micros < 1:
        return f"{micros:.3f}\u202fµs"
    if micros < 100:
        return f"{micros:.3f}\u202fµs"
    return f"{micros:.1f}\u202fµs"


def format_throughput(value: Optional[float], unit: str, precision: int) -> str:
    if value is None:
        return "—"
    millions = value / 1_000_000.0
    suffix = "M ops/s" if unit == "ops" else "M blocks/s"
    return f"{millions:.{precision}f}\u202f{suffix}"


def compute_throughput(mean_ns: Optional[float], ops: Optional[int]) -> Optional[float]:
    if mean_ns is None or ops is None:
        return None
    seconds = mean_ns / 1_000_000_000.0
    if seconds == 0:
        return None
    return ops / seconds


def build_table(platform_data: Dict[str, Dict[str, object]], rows: List[RowSpec], num_buffers: int) -> List[List[str]]:
    table = []
    for row in rows:
        row_values = []
        for policy in POLICY_ORDER:
            pdata = platform_data.get(policy)
            if pdata is None:
                row_values.append(None)
                continue
            mean_ns = pdata["means"].get(row.bench_name)
            if row.kind == "latency":
                row_values.append(mean_ns)
            else:
                ops = row.ops_fn(num_buffers) if row.ops_fn else None
                row_values.append(compute_throughput(mean_ns, ops))
        table.append(row_values)
    return table


def highlight_best(values: List[Optional[float]], kind: str) -> List[bool]:
    candidates = [v for v in values if v is not None]
    if not candidates:
        return [False] * len(values)
    target = min(candidates) if kind == "latency" else max(candidates)
    return [v is not None and abs(v - target) < 1e-12 for v in values]


def attach_hit_rate(cell: str, platform_data: Dict[str, Dict[str, object]], policy: str, hit_key: Optional[str]) -> str:
    if cell == "—" or not hit_key:
        return cell
    pdata = platform_data.get(policy)
    if not pdata:
        return cell
    stats = pdata["hit_rates"].get(hit_key)
    if not stats:
        return cell
    rate = stats["hit_rate"]
    return f"{cell} ({rate:.0f}\u202f% hits)"


def make_table_markdown(rows: List[RowSpec], platform_data: Dict[str, Dict[str, object]], num_buffers: int) -> str:
    table_lines = []
    header = ["Benchmark (Phase)"] + [platform_data.get(policy, {}).get("display", policy) for policy in POLICY_ORDER]
    table_lines.append("|" + "|".join(header) + "|")
    table_lines.append("|" + "|".join(["---"] * len(header)) + "|")

    for row, values in zip(rows, build_table(platform_data, rows, num_buffers)):
        best_flags = highlight_best(values, row.kind)
        cells = [row.label]
        for idx, value in enumerate(values):
            if row.kind == "latency":
                cell = format_latency(value)
            else:
                cell = format_throughput(value, row.unit, row.precision)
                policy = POLICY_ORDER[idx]
                cell = attach_hit_rate(cell, platform_data, policy, row.hit_key)
            if best_flags[idx] and cell != "—":
                cell = f"**{cell}**"
            cells.append(cell)
        table_lines.append("|" + "|".join(cells) + "|")

    return "\n".join(table_lines)



def render_platform_doc(platform: str, metadata: Dict[str, object], platform_data: Dict[str, Dict[str, object]]):
    output_path = REPO_ROOT / "docs" / "benchmarks" / "replacement_policies" / f"{platform}_buffer_pool.md"
    lines = [f"# {metadata.get('title', platform.title())}", ""]
    lines.append("Command template: `cargo bench --bench buffer_pool -- <iterations> <num_buffers>`")
    lines.append("")

    for policy in POLICY_ORDER:
        pdata = platform_data.get(policy)
        if not pdata or not pdata.get("log_text"):
            continue
        lines.append(f"## {pdata['display']}")
        lines.append("")
        lines.append("```")
        lines.append(pdata["log_text"])
        lines.append("```")
        lines.append("")

    output_path.write_text("\n".join(lines).rstrip() + "\n")


def load_platform_data(platform: str) -> Dict[str, Dict[str, object]]:
    metadata = load_metadata(platform)
    platform_data: Dict[str, Dict[str, object]] = {}
    for policy in POLICY_ORDER:
        info = metadata.get("policies", {}).get(policy)
        if not info:
            continue
        pdata = load_policy_data(info)
        if pdata:
            platform_data[policy] = pdata
    return metadata, platform_data


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--platforms", nargs="*", default=["macos", "linux"], help="Platforms to process")
    args = parser.parse_args()

    readme_path = REPO_ROOT / "benches" / "README.md"

    for platform in args.platforms:
        metadata, platform_data = load_platform_data(platform)
        num_buffers = metadata.get("num_buffers", 12)
        render_platform_doc(platform, metadata, platform_data)

        table_md = make_table_markdown(README_ROWS, platform_data, num_buffers)
        heading = f"### {metadata.get('title', platform.title())}"
        readme_text = readme_path.read_text()
        heading_idx = readme_text.index(heading)
        section_start = readme_text.index("\n", heading_idx) + 1
        if platform == "macos":
            section_end = readme_text.find("\n### ", section_start)
        else:
            section_end = readme_text.find("\n_Notes_", section_start)
        if section_end == -1:
            raise ValueError(f"Unable to locate section terminator for {platform} in benches/README.md")
        updated_readme = readme_text[:section_start] + table_md + "\n" + readme_text[section_end:]
        readme_path.write_text(updated_readme)


if __name__ == "__main__":
    main()
