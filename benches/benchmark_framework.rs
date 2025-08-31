use std::time::{Duration, Instant};
use std::fmt;

pub struct BenchResult {
    pub operation: String,
    pub mean: Duration,
    pub median: Duration,
    pub std_dev: Duration,
    pub iterations: usize,
}

impl fmt::Display for BenchResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:20} | {:>10.2?} | {:>10.2?} | {:>10.2?} | {:>8}",
            self.operation, self.mean, self.median, self.std_dev, self.iterations)
    }
}

pub fn benchmark<F>(name: &str, iterations: usize, mut operation: F) -> BenchResult
where
    F: FnMut(),
{
    let mut durations = Vec::with_capacity(iterations);
    
    // Warm up - run operation once to initialize any caches
    operation();
    
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
        durations.iter()
            .map(|d| (d.as_nanos() as f64 - mean.as_nanos() as f64).powi(2))
            .sum::<f64>() / (iterations as f64 - 1.0)
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
    println!("{:20} | {:>10} | {:>10} | {:>10} | {:>8}",
        "Operation", "Mean", "Median", "StdDev", "Iters");
    println!("{}", "-".repeat(70));
}