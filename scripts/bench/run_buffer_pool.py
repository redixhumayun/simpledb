#!/usr/bin/env -S uv run python
"""Run buffer_pool benchmarks for each replacement-policy variant.

This helper wraps `cargo bench --bench buffer_pool` so that we can:

1. Execute every replacement policy (master + feature-flagged builds)
   for a given platform with consistent `num_buffers`.
2. Capture results in github-action-benchmark compatible JSON
   (parsed from Criterion --output-format bencher text output).
3. Capture the human-readable benchmark log so docs can embed it exactly
   as produced by `cargo bench`.

The script writes all artifacts under:

    docs/benchmarks/replacement_policies/raw/<platform>/

An accompanying metadata.json file keeps track of run parameters and
per-policy artifacts.

Example usage:

    python3 scripts/bench/run_buffer_pool.py \
        --platform macos \
        --title "macOS (M1 Pro, macOS Sequoia)" \
        --environment "macos (aarch64)" \
        --num-buffers 12

This script only orchestrates benchmark execution; rendering markdown
tables/docs is handled by `render_replacement_policy_docs.py`.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from config import (
    DEFAULT_NUM_BUFFERS,
    PAGE_SIZES,
    PIN_HOTSET_POOL_SIZE,
    POLICIES,
    RAW_ROOT,
    REPO_ROOT,
)

BENCHER_RE = re.compile(
    r"^test (.+?) \.\.\. bench:\s+([\d,]+) ns/iter \(\+/-\s+([\d,]+)\)$"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--platform", required=True, help="Platform key (e.g., macos, linux)")
    parser.add_argument(
        "--title",
        help="Display title for docs/README sections",
    )
    parser.add_argument(
        "--environment",
        help="Short environment string (e.g., 'macos (aarch64)')",
    )
    parser.add_argument("--num-buffers", type=int, default=DEFAULT_NUM_BUFFERS, help="Buffer pool size")
    parser.add_argument(
        "--policies",
        nargs="*",
        choices=POLICIES.keys(),
        default=list(POLICIES.keys()),
        help="Subset of policies to run (default: all)",
    )
    parser.add_argument(
        "--page-size",
        choices=PAGE_SIZES.keys(),
        default="page-4k",
        help="Page size feature to use (default: page-4k)",
    )
    parser.add_argument(
        "--skip-text",
        action="store_true",
        help="Skip capturing human-readable text logs",
    )
    return parser.parse_args()


def run_command(cmd: List[str], extra_env: Dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.setdefault("CARGO_TERM_COLOR", "never")
    if extra_env:
        env.update(extra_env)
    return subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=True,
        env=env,
    )


def parse_bencher_output(stdout: str) -> List[Dict[str, object]]:
    """Parse Criterion --output-format bencher text output into JSON records."""
    results = []
    for line in stdout.splitlines():
        m = BENCHER_RE.match(line.strip())
        if m:
            name = m.group(1).strip()
            value = float(m.group(2).replace(",", ""))
            results.append({"name": name, "unit": "ns", "value": value})
    if not results:
        raise RuntimeError("No bench results found in cargo bench output")
    return results


def write_text_log(path: Path, stdout: str, stderr: str) -> None:
    combined = stdout
    if stderr:
        if combined and not combined.endswith("\n"):
            combined += "\n"
        combined += stderr
    path.write_text(combined)


def load_existing_metadata(path: Path) -> Dict[str, object]:
    if path.exists():
        return json.loads(path.read_text())
    return {}


def main() -> None:
    args = parse_args()
    raw_platform_dir = RAW_ROOT / args.platform
    raw_platform_dir.mkdir(parents=True, exist_ok=True)

    metadata_path = raw_platform_dir / "metadata.json"
    metadata = load_existing_metadata(metadata_path)

    # Default title/environment if not provided
    if not args.title:
        if args.platform == "macos":
            args.title = "macOS (M1 Pro, macOS Sequoia)"
        elif args.platform == "linux":
            args.title = "Linux (i7-8650U, Ubuntu 6.8.0-86)"
        else:
            args.title = args.platform.title()

    if not args.environment:
        if args.platform == "macos":
            args.environment = "macos (aarch64)"
        elif args.platform == "linux":
            args.environment = "linux (x86_64)"
        else:
            args.environment = ""

    metadata.update(
        {
            "platform": args.platform,
            "title": args.title,
            "environment": args.environment,
            "num_buffers": args.num_buffers,
            "pin_hotset_pool_size": PIN_HOTSET_POOL_SIZE,
            "page_size": args.page_size,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    metadata.setdefault("policies", {})

    for policy_key in args.policies:
        policy_display = POLICIES[policy_key]
        page_size_display = PAGE_SIZES[args.page_size]
        print(f"Running policy: {policy_key}")

        cargo_args = [
            "--no-default-features",
            "--features", policy_key,
            "--features", args.page_size,
        ]
        base_cmd = ["cargo", "bench", "--bench", "buffer_pool"] + cargo_args
        bench_env = {"SIMPLEDB_BENCH_BUFFERS": str(args.num_buffers)}

        # JSON run (bencher text → parsed to JSON)
        json_cmd = base_cmd + ["--", "--output-format", "bencher", "--noplot"]
        json_result = run_command(json_cmd, extra_env=bench_env)
        json_payload = parse_bencher_output(json_result.stdout)

        json_path = raw_platform_dir / f"{policy_key}.json"
        json_path.write_text(json.dumps(json_payload, indent=2))

        log_rel = None

        if not args.skip_text:
            text_cmd = base_cmd + ["--noplot"]
            text_result = run_command(text_cmd, extra_env=bench_env)
            log_path = raw_platform_dir / f"{policy_key}.txt"
            write_text_log(log_path, text_result.stdout, text_result.stderr)
            log_rel = log_path.relative_to(REPO_ROOT).as_posix()

        full_display = f"{policy_display} ({page_size_display})"
        metadata["policies"][policy_key] = {
            "display": full_display,
            "json_path": json_path.relative_to(REPO_ROOT).as_posix(),
            "log_path": log_rel,
        }

    metadata_path.write_text(json.dumps(metadata, indent=2))
    print(f"Artifacts written to {raw_platform_dir.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
