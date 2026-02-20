#!/usr/bin/env bash
set -euo pipefail

# Capture a buffered vs direct-io benchmark pair with iostat/vmstat/pidstat logs.
#
# Default target case:
#   - io_patterns
#   - regime=hot
#   - filter="Sequential Read"
#   - iterations=10
#   - ops fixed to 50 for apples-to-apples runs

ITERATIONS=10
NUM_BUFFERS=12
REGIME="hot"
FILTER="Sequential Read"
PHASE1_OPS=50
MIXED_OPS=50
DURABILITY_OPS=50
INTERVAL=1
REPEATS=1
OUTPUT_DIR="results/profile_io_pair_$(date +%Y%m%d_%H%M%S)"

usage() {
  cat <<'EOF'
Usage:
  scripts/bench/profile_io_pair.sh [options]

Options:
  --output-dir <dir>         Output directory for logs/results.
  --iterations <n>           Iterations passed to io_patterns (default: 10).
  --num-buffers <n>          Buffer count arg (default: 12).
  --regime <name>            hot|pressure|thrash (default: hot).
  --filter <pattern>         io_patterns --filter value (default: "Sequential Read").
  --phase1-ops <n>           Fixed phase1 ops (default: 50).
  --mixed-ops <n>            Fixed mixed ops (default: 50).
  --durability-ops <n>       Fixed durability ops (default: 50).
  --interval <sec>           Sampling interval for iostat/vmstat/pidstat (default: 1).
  --repeats <n>              Number of buffered/direct pairs (default: 1).
  -h, --help                 Show help.

Example:
  scripts/bench/profile_io_pair.sh \
    --regime hot \
    --filter "Sequential Read" \
    --iterations 10 \
    --repeats 3
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
    --iterations) ITERATIONS="$2"; shift 2 ;;
    --num-buffers) NUM_BUFFERS="$2"; shift 2 ;;
    --regime) REGIME="$2"; shift 2 ;;
    --filter) FILTER="$2"; shift 2 ;;
    --phase1-ops) PHASE1_OPS="$2"; shift 2 ;;
    --mixed-ops) MIXED_OPS="$2"; shift 2 ;;
    --durability-ops) DURABILITY_OPS="$2"; shift 2 ;;
    --interval) INTERVAL="$2"; shift 2 ;;
    --repeats) REPEATS="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

for tool in cargo iostat vmstat pidstat pgrep; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "Missing required tool: $tool" >&2
    exit 1
  fi
done

mkdir -p "$OUTPUT_DIR"

cat > "$OUTPUT_DIR/run_config.txt" <<EOF
generated_at_utc=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
cwd=$(pwd)
iterations=$ITERATIONS
num_buffers=$NUM_BUFFERS
regime=$REGIME
filter=$FILTER
phase1_ops=$PHASE1_OPS
mixed_ops=$MIXED_OPS
durability_ops=$DURABILITY_OPS
interval=$INTERVAL
repeats=$REPEATS
EOF

stop_sampler() {
  local pid="${1:-}"
  if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" 2>/dev/null || true
  fi
}

wait_for_io_patterns_pid() {
  local timeout_sec="${1:-60}"
  local started_at
  started_at="$(date +%s)"
  local pid=""

  while true; do
    pid="$(pgrep -f "target/release/deps/io_patterns-.*--regime ${REGIME}.*--bench" | tail -n1 || true)"
    if [[ -n "$pid" ]]; then
      echo "$pid"
      return 0
    fi
    if (( $(date +%s) - started_at >= timeout_sec )); then
      return 1
    fi
    sleep 0.2
  done
}

run_case() {
  local mode="$1"    # buffered|direct
  local repeat_idx="$2"
  local case_dir="$OUTPUT_DIR/${mode}_run${repeat_idx}"
  mkdir -p "$case_dir"

  local -a cmd=(
    cargo bench --bench io_patterns
    --no-default-features
    --features replacement_lru
    --features page-4k
  )
  if [[ "$mode" == "direct" ]]; then
    cmd+=(--features direct-io)
  fi
  cmd+=(
    -- "$ITERATIONS" "$NUM_BUFFERS"
    --regime "$REGIME"
    --phase1-ops "$PHASE1_OPS"
    --mixed-ops "$MIXED_OPS"
    --durability-ops "$DURABILITY_OPS"
    --filter "$FILTER"
  )

  printf '%q ' "${cmd[@]}" > "$case_dir/command.sh"
  printf '\n' >> "$case_dir/command.sh"
  echo "mode=$mode" > "$case_dir/meta.txt"
  echo "repeat=$repeat_idx" >> "$case_dir/meta.txt"
  echo "started_at_utc=$(date -u +"%Y-%m-%dT%H:%M:%SZ")" >> "$case_dir/meta.txt"

  iostat -x -y "$INTERVAL" > "$case_dir/iostat.log" 2>&1 &
  local iostat_pid=$!

  vmstat "$INTERVAL" > "$case_dir/vmstat.log" 2>&1 &
  local vmstat_pid=$!

  "${cmd[@]}" > "$case_dir/bench.stdout.log" 2> "$case_dir/bench.stderr.log" &
  local bench_pid=$!
  echo "$bench_pid" > "$case_dir/bench_runner_pid.txt"

  local pidstat_pid=""
  local target_pid=""
  if target_pid="$(wait_for_io_patterns_pid 60)"; then
    echo "$target_pid" > "$case_dir/bench_target_pid.txt"
    pidstat -d -u -r -h "$INTERVAL" -p "$target_pid" > "$case_dir/pidstat.log" 2>&1 &
    pidstat_pid=$!
  else
    echo "warning: io_patterns child PID not found; using bench runner pid=$bench_pid" \
      > "$case_dir/pidstat_warning.txt"
    pidstat -d -u -r -h "$INTERVAL" -p "$bench_pid" > "$case_dir/pidstat.log" 2>&1 &
    pidstat_pid=$!
  fi

  local exit_code=0
  if ! wait "$bench_pid"; then
    exit_code=$?
  fi
  echo "$exit_code" > "$case_dir/exit_code.txt"
  echo "finished_at_utc=$(date -u +"%Y-%m-%dT%H:%M:%SZ")" >> "$case_dir/meta.txt"

  stop_sampler "$pidstat_pid"
  stop_sampler "$iostat_pid"
  stop_sampler "$vmstat_pid"

  if [[ "$exit_code" -ne 0 ]]; then
    echo "Case failed: mode=$mode repeat=$repeat_idx (exit=$exit_code)" >&2
    return "$exit_code"
  fi
}

for ((i=1; i<=REPEATS; i++)); do
  echo "=== Repeat $i/$REPEATS: buffered ==="
  run_case buffered "$i"
  echo "=== Repeat $i/$REPEATS: direct ==="
  run_case direct "$i"
done

echo "Profiling capture complete: $OUTPUT_DIR"
