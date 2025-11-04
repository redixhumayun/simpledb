use std::time::{Duration, Instant};
use std::{env, fmt};

pub struct BenchResult {
    pub operation: String,
    pub mean: Duration,
    pub median: Duration,
    pub std_dev: Duration,
    pub iterations: usize,
}

impl fmt::Display for BenchResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:60} | {:>10.2?} | {:>10.2?} | {:>10.2?} | {:>8}",
            self.operation, self.mean, self.median, self.std_dev, self.iterations
        )
    }
}

impl BenchResult {
    /// Convert benchmark result to JSON format compatible with github-action-benchmark
    /// Returns JSON object with name, unit, and value (mean time in nanoseconds)
    pub fn to_json(&self) -> String {
        format!(
            r#"{{"name":"{}","unit":"ns","value":{}}}"#,
            self.operation,
            self.mean.as_nanos()
        )
    }
}

pub fn parse_bench_args() -> (usize, usize, bool, Option<String>) {
    let args: Vec<String> = env::args().collect();
    let mut numeric_args = Vec::new();
    let mut json_output = false;
    let mut filter: Option<String> = None;

    // Collect all numeric args, check for flags, and capture filter string
    for arg in args.iter().skip(1) {
        if arg == "--json" {
            json_output = true;
        } else if !arg.starts_with("--") {
            if let Ok(n) = arg.parse::<usize>() {
                numeric_args.push(n);
            } else {
                // Non-numeric, non-flag argument is treated as filter
                filter = Some(arg.clone());
            }
        }
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

    durations.sort();
    let mean = durations.iter().sum::<Duration>() / iterations as u32;
    let median = if iterations % 2 == 1 {
        durations[iterations / 2]
    } else {
        let mid1 = durations[iterations / 2 - 1].as_nanos();
        let mid2 = durations[iterations / 2].as_nanos();
        Duration::from_nanos(((mid1 + mid2) / 2) as u64)
    };

    // Simple std deviation calculation
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
        std_dev,
        iterations,
    }
}

pub fn print_header() {
    println!(
        "{:60} | {:>10} | {:>10} | {:>10} | {:>8}",
        "Operation", "Mean", "Median", "StdDev", "Iters"
    );
    println!("{}", "-".repeat(120));
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
