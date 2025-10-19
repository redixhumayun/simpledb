#!/bin/bash
# Auto-discover and run all cargo benchmarks, combining JSON results

set -e

ITERATIONS=${1:-50}
NUM_BUFFERS=${2:-12}
OUTPUT_FILE=${3:-all_benchmarks.json}

echo "Running all benchmarks with $ITERATIONS iterations..." >&2

# Array to hold all JSON results
ALL_RESULTS=()

# Discover and run all cargo benchmarks from benches/ directory
echo "Discovering benchmarks in benches/..." >&2
for bench_file in benches/*.rs; do
    if [ -f "$bench_file" ] && [ "$(basename "$bench_file")" != "README.md" ]; then
        bench_name=$(basename "$bench_file" .rs)
        echo "Running: $bench_name" >&2

        # Run cargo bench with JSON output
        result=$(cargo bench --bench "$bench_name" -- "$ITERATIONS" "$NUM_BUFFERS" --json 2>/dev/null)
        ALL_RESULTS+=("$result")
    fi
done

# 3. Combine all JSON arrays into one
echo "Combining results..." >&2
combined="["
first=true

for result in "${ALL_RESULTS[@]}"; do
    # Strip outer brackets from each JSON array
    inner=$(echo "$result" | sed 's/^\[//' | sed 's/\]$//')

    if [ "$first" = true ]; then
        combined="$combined$inner"
        first=false
    else
        combined="$combined,$inner"
    fi
done

combined="$combined]"

# 4. Output combined JSON
echo "$combined" > "$OUTPUT_FILE"
echo "All benchmark results written to $OUTPUT_FILE" >&2

# 5. Pretty print summary to stderr
echo "" >&2
echo "Summary:" >&2
echo "$combined" | python3 -c "
import json
import sys
data = json.load(sys.stdin)
for item in data:
    name = item['name']
    value = item['value']
    # Convert nanoseconds to human-readable
    if value < 1000:
        print(f'  {name}: {value:.0f}ns')
    elif value < 1_000_000:
        print(f'  {name}: {value/1000:.2f}µs')
    elif value < 1_000_000_000:
        print(f'  {name}: {value/1_000_000:.2f}ms')
    else:
        print(f'  {name}: {value/1_000_000_000:.2f}s')
" >&2

echo "" >&2
echo "Total benchmarks: $(echo "$combined" | python3 -c "import json, sys; print(len(json.load(sys.stdin)))")" >&2
