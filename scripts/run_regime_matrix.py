#!/usr/bin/env python3
"""Run the regime-based validation matrix: 3 regimes × 2 I/O modes."""

import argparse
import json
import os
import shlex
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


REGIMES = ["hot", "pressure", "thrash"]

BASE_FEATURES = [
    "--no-default-features",
    "--features", "replacement_lru",
    "--features", "page-4k",
]

DIRECT_EXTRA = ["--features", "direct-io"]

HEAVY_PROFILE_BLOCKS = {
    # 8 GiB / 16 GiB / 32 GiB with 4KiB pages
    "hot": 2_097_152,
    "pressure": 4_194_304,
    "thrash": 8_388_608,
}


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("iterations", nargs="?", type=int, default=50)
    parser.add_argument("output_dir", nargs="?", default="regime_matrix_results")
    parser.add_argument(
        "--profile",
        choices=["capped", "heavy"],
        default="capped",
        help=(
            "Regime profile. "
            "capped = quick guardrail (regime-derived working sets, ops scale with regime). "
            "heavy = decision-grade signal (large fixed working sets, explicit high ops)."
        ),
    )
    parser.add_argument("--phase1-ops", type=int, default=None)
    parser.add_argument("--mixed-ops", type=int, default=None)
    parser.add_argument("--durability-ops", type=int, default=None)
    return parser.parse_args()


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


def detect_cgroup_context():
    """Detect cgroup v2 context and memory/swap/cpu limits for this process."""
    context = {
        "detected": False,
        "path": None,
        "memory_max": None,
        "memory_swap_max": None,
        "memory_current": None,
        "cpu_max": None,
        "limited": False,
    }
    try:
        cgroup_line = None
        for line in Path("/proc/self/cgroup").read_text().splitlines():
            if line.startswith("0::"):
                cgroup_line = line
                break
        if cgroup_line is None:
            return context

        rel_path = cgroup_line.split("::", 1)[1].strip()
        cgroup_path = Path("/sys/fs/cgroup") / rel_path.lstrip("/")
        if not cgroup_path.exists():
            return context

        def read_val(name):
            p = cgroup_path / name
            return p.read_text().strip() if p.exists() else None

        context["detected"] = True
        context["path"] = rel_path
        context["memory_max"] = read_val("memory.max")
        context["memory_swap_max"] = read_val("memory.swap.max")
        context["memory_current"] = read_val("memory.current")
        context["cpu_max"] = read_val("cpu.max")
        context["limited"] = (
            context["memory_max"] not in (None, "max")
            or context["memory_swap_max"] not in (None, "max")
        )
    except Exception:
        pass
    return context


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


def run_bench_cell(regime, iterations, features_extra, label, args):
    """Run io_patterns bench for one (regime, mode) cell."""
    bench_args = [str(iterations), "12", "--json"]
    if args.profile == "heavy":
        bench_args.extend(
            ["--working-set-blocks", str(HEAVY_PROFILE_BLOCKS[regime])]
        )
    else:
        # capped uses regime-derived working sets.
        bench_args.extend(["--regime", regime])

    # Preserve io_patterns regime auto-scaling by only passing op flags when
    # explicitly set (or intentionally fixed by the heavy profile).
    phase1_ops = args.phase1_ops
    mixed_ops = args.mixed_ops
    durability_ops = args.durability_ops
    if args.profile == "heavy":
        phase1_ops = phase1_ops if phase1_ops is not None else 20_000
        mixed_ops = mixed_ops if mixed_ops is not None else 10_000
        durability_ops = durability_ops if durability_ops is not None else 5_000

    if phase1_ops is not None:
        bench_args.extend(["--phase1-ops", str(phase1_ops)])
    if mixed_ops is not None:
        bench_args.extend(["--mixed-ops", str(mixed_ops)])
    if durability_ops is not None:
        bench_args.extend(["--durability-ops", str(durability_ops)])

    cmd = (
        ["cargo", "bench", "--bench", "io_patterns"]
        + BASE_FEATURES
        + features_extra
        + ["--"]
        + bench_args
    )
    cmd_str = shlex.join(cmd)
    print(f"  [{label}] running: {cmd_str}", file=sys.stderr)
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
                return json.loads(line), cmd_str
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Bad JSON from {label}/{regime}: {exc}") from exc

    raise RuntimeError(f"No JSON output from {label}/{regime}")


def run_compare(base_file, pr_file, out_file, label=None):
    """Call compare_benchmarks.py and return (markdown text, exact command)."""
    cmd = ["python3", "scripts/compare_benchmarks.py", str(base_file), str(pr_file), str(out_file)]
    if label:
        cmd.append(label)
    cmd_str = shlex.join(cmd)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"WARNING: compare_benchmarks.py failed: {result.stderr}", file=sys.stderr)
    return (out_file.read_text() if out_file.exists() else ""), cmd_str


def build_run_metadata(args, iterations, output_dir, filesystem):
    """Build run metadata shared by manifest + compare markdown headers."""
    if args.profile == "heavy":
        phase1_ops = args.phase1_ops if args.phase1_ops is not None else 20_000
        mixed_ops = args.mixed_ops if args.mixed_ops is not None else 10_000
        durability_ops = args.durability_ops if args.durability_ops is not None else 5_000
    else:
        phase1_ops = "auto" if args.phase1_ops is None else args.phase1_ops
        mixed_ops = "auto" if args.mixed_ops is None else args.mixed_ops
        durability_ops = "auto" if args.durability_ops is None else args.durability_ops

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "script_invocation": shlex.join([sys.executable, *sys.argv]),
        "cwd": os.getcwd(),
        "iterations": iterations,
        "profile": args.profile,
        "regimes": REGIMES,
        "ops": {
            "phase1": phase1_ops,
            "mixed": mixed_ops,
            "durability": durability_ops,
        },
        "filesystem_device": filesystem,
        "cgroup": detect_cgroup_context(),
        "output_dir": str(output_dir),
        "base_features": BASE_FEATURES,
        "direct_extra_features": DIRECT_EXTRA,
        "commands": {},
    }


def render_compare_metadata_md(metadata, regime):
    """Render a compact metadata section for compare markdown files."""
    ops = metadata["ops"]
    cgroup = metadata.get("cgroup", {})
    cmd_set = metadata.get("commands", {}).get(regime, {})
    lines = [
        "### Run Metadata",
        "",
        f"- Generated (UTC): `{metadata['generated_at_utc']}`",
        f"- Regime: `{regime}`",
        f"- Iterations: `{metadata['iterations']}`",
        f"- Profile: `{metadata['profile']}`",
        f"- Ops: `phase1={ops['phase1']}, mixed={ops['mixed']}, durability={ops['durability']}`",
        f"- Filesystem/device: `{metadata['filesystem_device']}`",
        f"- Output dir: `{metadata['output_dir']}`",
        f"- Script invocation: `{metadata.get('script_invocation', '')}`",
        f"- Base features: `{' '.join(metadata['base_features'])}`",
        f"- Direct extra features: `{' '.join(metadata['direct_extra_features'])}`",
    ]
    if cgroup.get("detected"):
        lines.extend(
            [
                f"- Cgroup path: `{cgroup.get('path')}`",
                (
                    f"- Cgroup memory: `memory.max={cgroup.get('memory_max')}, "
                    f"memory.swap.max={cgroup.get('memory_swap_max')}, "
                    f"memory.current={cgroup.get('memory_current')}`"
                ),
                f"- Cgroup CPU: `cpu.max={cgroup.get('cpu_max')}`",
                f"- Cgroup limited: `{cgroup.get('limited')}`",
            ]
        )
    if cmd_set:
        lines.extend(
            [
                f"- Buffered command: `{cmd_set.get('buffered', '')}`",
                f"- Direct command: `{cmd_set.get('direct', '')}`",
                f"- Compare command: `{cmd_set.get('compare', '')}`",
            ]
        )
    lines.extend(["", "---", ""])
    return "\n".join(lines)


def prepend_compare_metadata(compare_file, metadata, regime):
    """Prepend run metadata to a generated compare markdown file."""
    if not compare_file.exists():
        return
    original = compare_file.read_text()
    header = render_compare_metadata_md(metadata, regime)
    compare_file.write_text(header + original)


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
    args = parse_args()
    iterations = args.iterations
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    filesystem = detect_filesystem()
    print(f"Regime matrix: {iterations} iterations, output → {output_dir}", file=sys.stderr)
    print(f"Profile: {args.profile}", file=sys.stderr)
    print(f"Regimes: {REGIMES}", file=sys.stderr)
    if args.profile == "heavy":
        print(f"Heavy working-set blocks: {HEAVY_PROFILE_BLOCKS}", file=sys.stderr)
    if args.profile == "heavy":
        phase1_info = args.phase1_ops if args.phase1_ops is not None else 20_000
        mixed_info = args.mixed_ops if args.mixed_ops is not None else 10_000
        durability_info = args.durability_ops if args.durability_ops is not None else 5_000
        print(
            f"Ops: phase1={phase1_info}, mixed={mixed_info}, durability={durability_info} (heavy explicit)",
            file=sys.stderr,
        )
    else:
        phase1_info = "auto" if args.phase1_ops is None else str(args.phase1_ops)
        mixed_info = "auto" if args.mixed_ops is None else str(args.mixed_ops)
        durability_info = "auto" if args.durability_ops is None else str(args.durability_ops)
        print(
            f"Ops: phase1={phase1_info}, mixed={mixed_info}, durability={durability_info}",
            file=sys.stderr,
        )
    print(f"Filesystem/device: {filesystem}", file=sys.stderr)
    metadata = build_run_metadata(args, iterations, output_dir, filesystem)
    (output_dir / "run_manifest.json").write_text(json.dumps(metadata, indent=2))

    all_buffered = {}
    all_direct = {}

    for regime in REGIMES:
        print(f"\n=== Regime: {regime} ===", file=sys.stderr)

        # Buffered
        buffered_file = output_dir / f"buffered_{regime}.json"
        buffered, buffered_cmd = run_bench_cell(regime, iterations, [], "buffered", args)
        buffered_file.write_text(json.dumps(buffered, indent=2))
        print(f"  Saved: {buffered_file}", file=sys.stderr)

        # Direct-io
        direct_file = output_dir / f"direct_{regime}.json"
        direct, direct_cmd = run_bench_cell(regime, iterations, DIRECT_EXTRA, "direct-io", args)
        direct_file.write_text(json.dumps(direct, indent=2))
        print(f"  Saved: {direct_file}", file=sys.stderr)

        # Compare
        compare_file = output_dir / f"compare_{regime}.md"
        _, compare_cmd = run_compare(buffered_file, direct_file, compare_file, label=regime)
        metadata["commands"][regime] = {
            "buffered": buffered_cmd,
            "direct": direct_cmd,
            "compare": compare_cmd,
        }
        (output_dir / "run_manifest.json").write_text(json.dumps(metadata, indent=2))
        prepend_compare_metadata(compare_file, metadata, regime)
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
