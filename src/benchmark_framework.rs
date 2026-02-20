use std::time::{Duration, Instant};
use std::{env, fmt};

pub struct BenchResult {
    pub operation: String,
    pub mean: Duration,
    pub median: Duration,
    pub p95: Duration,
    pub std_dev: Duration,
    pub iterations: usize,
}

impl fmt::Display for BenchResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:60} | {:>10.2?} | {:>10.2?} | {:>10.2?} | {:>10.2?} | {:>8}",
            self.operation, self.mean, self.median, self.p95, self.std_dev, self.iterations
        )
    }
}

impl BenchResult {
    /// Convert benchmark result to JSON format compatible with github-action-benchmark.
    /// `value` is mean (ns) for backward compat; p50/p95/std_dev added as extra fields.
    pub fn to_json(&self) -> String {
        format!(
            r#"{{"name":"{}","unit":"ns","value":{},"p50":{},"p95":{},"std_dev":{}}}"#,
            self.operation,
            self.mean.as_nanos(),
            self.median.as_nanos(),
            self.p95.as_nanos(),
            self.std_dev.as_nanos(),
        )
    }
}

pub fn parse_bench_args() -> (usize, usize, bool, Option<String>) {
    let args: Vec<String> = env::args().collect();
    let mut numeric_args = Vec::new();
    let mut json_output = false;
    let mut filter: Option<String> = None;
    let mut i = 1usize;

    while i < args.len() {
        let arg = &args[i];
        if arg == "--json" {
            json_output = true;
            i += 1;
            continue;
        }
        if arg == "--filter" {
            let Some(value) = args.get(i + 1) else {
                panic!("--filter requires a value");
            };
            if value.starts_with("--") {
                panic!("--filter requires a non-flag value");
            }
            filter = Some(value.clone());
            i += 2;
            continue;
        }
        if arg.starts_with("--") {
            // Unknown flag: skip optional value if present.
            if i + 1 < args.len() && !args[i + 1].starts_with("--") {
                i += 2;
            } else {
                i += 1;
            }
            continue;
        }

        if let Ok(n) = arg.parse::<usize>() {
            numeric_args.push(n);
            i += 1;
            continue;
        }

        panic!(
            "unexpected positional argument '{}'; use --filter <pattern> for benchmark filtering",
            arg
        );
    }

    let iterations = numeric_args.first().copied().unwrap_or(10);
    let num_buffers = numeric_args.get(1).copied().unwrap_or(12);

    (iterations, num_buffers, json_output, filter)
}

pub fn should_run(bench_name: &str, filter: Option<&str>) -> bool {
    match filter {
        None => true,
        Some(f) => bench_name.contains(f),
    }
}

pub fn benchmark<F>(name: &str, iterations: usize, warmup: usize, mut operation: F) -> BenchResult
where
    F: FnMut(),
{
    let mut durations = Vec::with_capacity(iterations);

    // Warm up - run operation multiple times to stabilize caches
    for _ in 0..warmup {
        operation();
    }

    // Collect timing data
    for _ in 0..iterations {
        let start = Instant::now();
        operation();
        durations.push(start.elapsed());
    }

    compute_stats(name, durations)
}

/// Like [`benchmark`] but calls `teardown` after each iteration (outside the timed section).
/// Use this for cache-evict variants where cache must be dropped between measurements.
pub fn benchmark_with_teardown<F, T>(
    name: &str,
    iterations: usize,
    warmup: usize,
    mut operation: F,
    mut teardown: T,
) -> BenchResult
where
    F: FnMut(),
    T: FnMut(),
{
    let mut durations = Vec::with_capacity(iterations);

    for _ in 0..warmup {
        operation();
        teardown();
    }

    for _ in 0..iterations {
        let start = Instant::now();
        operation();
        durations.push(start.elapsed());
        teardown();
    }

    compute_stats(name, durations)
}

fn compute_stats(name: &str, mut durations: Vec<Duration>) -> BenchResult {
    let iterations = durations.len();
    durations.sort();
    let mean = durations.iter().sum::<Duration>() / iterations as u32;
    let median = if iterations % 2 == 1 {
        durations[iterations / 2]
    } else {
        let mid1 = durations[iterations / 2 - 1].as_nanos();
        let mid2 = durations[iterations / 2].as_nanos();
        Duration::from_nanos(((mid1 + mid2) / 2) as u64)
    };
    let p95 = durations[(iterations * 95 / 100).min(iterations - 1)];

    let variance: f64 = if iterations > 1 {
        durations
            .iter()
            .map(|d| (d.as_nanos() as f64 - mean.as_nanos() as f64).powi(2))
            .sum::<f64>()
            / (iterations as f64 - 1.0)
    } else {
        0.0
    };
    let std_dev = Duration::from_nanos(variance.sqrt() as u64);

    BenchResult {
        operation: name.to_string(),
        mean,
        median,
        p95,
        std_dev,
        iterations,
    }
}

pub fn print_header() {
    println!(
        "{:60} | {:>10} | {:>10} | {:>10} | {:>10} | {:>8}",
        "Operation", "Mean", "Median", "p95", "StdDev", "Iters"
    );
    println!("{}", "-".repeat(133));
}

pub struct ThroughputRow {
    pub label: String,
    pub throughput: f64,
    pub unit: String,
    pub mean_duration: Duration,
}

pub fn render_throughput_section(title: &str, rows: &[ThroughputRow]) {
    if rows.is_empty() {
        return;
    }

    println!("{}", title);
    println!(
        "{:<60} | {:>20} | {:>15}",
        "Operation", "Throughput", "Mean Duration"
    );
    println!("{}", "-".repeat(120));

    for row in rows {
        let throughput_str = format!("{:.2} {}", row.throughput, row.unit);
        println!(
            "{:<60} | {:>20} | {:>15.2?}",
            row.label, throughput_str, row.mean_duration
        );
    }
    println!();
}
