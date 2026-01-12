"""Centralized configuration for benchmark scripts.

This module contains constants shared across benchmark scripts to ensure
consistency between Rust benchmarks and Python analysis/rendering tools.

IMPORTANT: Keep these constants in sync with benches/buffer_pool.rs
"""

from pathlib import Path
from typing import Dict, List

# Repository paths
REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_ROOT = REPO_ROOT / "docs" / "benchmarks" / "replacement_policies" / "raw"
CHARTS_DIR = REPO_ROOT / "docs" / "benchmarks" / "charts"

# Replacement policies
POLICY_ORDER: List[str] = [
    "replacement_lru",
    "replacement_clock",
    "replacement_sieve",
]

POLICIES: Dict[str, str] = {
    "replacement_lru": "Replacement LRU",
    "replacement_clock": "Replacement Clock",
    "replacement_sieve": "Replacement SIEVE",
}

POLICY_DISPLAY: Dict[str, str] = {
    "replacement_lru": "LRU",
    "replacement_clock": "Clock",
    "replacement_sieve": "SIEVE",
}

POLICY_COLORS: Dict[str, str] = {
    "replacement_lru": "#2E86AB",      # Blue
    "replacement_clock": "#A23B72",    # Purple
    "replacement_sieve": "#F18F01",    # Orange
}

# Page sizes
PAGE_SIZES: Dict[str, str] = {
    "page-4k": "4KB pages",
    "page-8k": "8KB pages",
    "page-1m": "1MB pages",
}

# Thread counts for scaling benchmarks
THREAD_COUNTS: List[int] = [1, 2, 4, 8, 16, 32, 64, 128, 256]

# Thread count subsets for README table (keeps table readable)
README_MT_THREAD_COUNTS: List[int] = [4, 16]  # For sequential/repeated/random/zipfian MT rows
README_PIN_THREAD_COUNTS: List[int] = [1, 2, 8, 16, 64, 256]  # For pin benchmark rows
README_HOTSET_THREAD_COUNTS: List[int] = [1, 2, 8, 16, 64, 256]  # For hotset benchmark rows

# Total operations for each benchmark type
# These must match the constants in benches/buffer_pool.rs
TOTAL_OPS: Dict[str, int] = {
    "pin": 10_000,
    "hotset": 10_000,
    "repeated": 1_000,
    "random": 500,
    "zipfian": 500,
}

# Buffer pool sizes
PIN_HOTSET_POOL_SIZE: int = 4096
DEFAULT_NUM_BUFFERS: int = 12
DEFAULT_ITERATIONS: int = 100

# Hotset configuration
HOTSET_K: int = 4  # Hot set size for hotset contention benchmarks


def compute_ops_per_thread(total_ops: int, threads: int) -> int:
    """Compute operations per thread (integer division)."""
    return total_ops // threads


def pin_benchmark_name(threads: int) -> str:
    """Generate benchmark name for pin/unpin concurrent test."""
    ops_per_thread = compute_ops_per_thread(TOTAL_OPS["pin"], threads)
    return f"Concurrent ({threads} threads, {ops_per_thread} ops)"


def hotset_benchmark_name(threads: int, k: int = HOTSET_K) -> str:
    """Generate benchmark name for hotset contention test."""
    ops_per_thread = compute_ops_per_thread(TOTAL_OPS["hotset"], threads)
    return f"Concurrent Hotset ({threads} threads, K={k}, {ops_per_thread} ops)"


def sequential_benchmark_name(threads: int, total_blocks: int) -> str:
    """Generate benchmark name for sequential scan."""
    if threads == 1:
        return f"Sequential Scan ({total_blocks} blocks)"
    return f"Seq Scan MT x{threads} ({total_blocks} blocks)"


def repeated_benchmark_name(threads: int) -> str:
    """Generate benchmark name for repeated access."""
    total_ops = TOTAL_OPS["repeated"]
    if threads == 1:
        return f"Repeated Access ({total_ops} ops)"
    return f"Repeated Access MT x{threads} ({total_ops} ops)"


def random_benchmark_name(threads: int, k: int) -> str:
    """Generate benchmark name for random access."""
    total_ops = TOTAL_OPS["random"]
    if threads == 1:
        return f"Random (K={k}, {total_ops} ops)"
    return f"Random MT x{threads} (K={k}, {total_ops} ops)"


def zipfian_benchmark_name(threads: int) -> str:
    """Generate benchmark name for zipfian access."""
    total_ops = TOTAL_OPS["zipfian"]
    if threads == 1:
        return f"Zipfian (80/20, {total_ops} ops)"
    return f"Zipfian MT x{threads} (80/20, {total_ops} ops)"
