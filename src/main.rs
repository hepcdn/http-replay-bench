use std::{
    fs::File, io::{BufRead, BufReader}, iter, path::PathBuf, sync::{Arc, atomic}, thread, time::{Duration, Instant},
};

use ::futures::{StreamExt, stream};
use clap::{Parser, ValueEnum};
use serde::Serialize;
use serde_with::{serde_as, DurationSecondsWithFrac};

use crate::driver::{ClientDriver, DriverConfig};

mod driver;
mod trace;

/// A load generator for benchmarking distributed storage via http protocol.
///
/// This program focuses on generating client load that recreates typical
/// application load as given by prerecorded traces, where the typical access
/// pattern is to make repeated range requests against large (multi-gigabyte)
/// URLs. The program is massively multithreaded to be able to simulate tens of
/// thousands of concurrent clients, and it is designed to be run on a single
/// machine with a large number of CPU cores and network bandwidth.
#[derive(Clone, Parser, Debug, Serialize)]
#[command(version, about, long_about = None)]
struct Args {
    /// Endpoint URL prefix to send requests to.
    #[arg(short, long)]
    endpoint: String,

    /// File containing a list of relative paths to request, one per line.
    ///
    /// Each path will be appended to the endpoint URL prefix.
    #[arg(short, long)]
    path_file: PathBuf,

    /// Limit the number of paths to read from the path file.
    #[arg(short, long)]
    limit: Option<usize>,

    /// Output file to write the results to.
    #[arg(short, long, default_value = "results.json")]
    output_file: PathBuf,

    /// HTTP version to use for requests.
    #[arg(value_enum, default_value_t = HttpVersion::Http1p1)]
    http_version: HttpVersion,

    /// Number of worker threads to spawn for generating load.
    #[arg(short, long, default_value_t = 16)]
    num_workers: usize,

    /// Number of concurrent requests to make per worker thread.
    #[arg(short, long, default_value_t = 1000)]
    worker_concurrency: usize,

    #[command(subcommand)]
    driver: DriverConfig,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Serialize)]
enum HttpVersion {
    /// Use HTTP/1.1 for requests.
    Http1p1,
    /// Use HTTP/2 for requests.
    Http2,
}

#[derive(Clone, Debug)]
struct URLPool {
    paths: Arc<Vec<String>>,
    index: Arc<atomic::AtomicUsize>,
}

impl URLPool {
    fn load(args: &Args) -> anyhow::Result<Self> {
        let file = File::open(&args.path_file)?;
        let reader = BufReader::new(file);
        let paths: Vec<String> = reader
            .lines()
            .map_while(Result::ok)
            .filter(|line| !line.trim().is_empty())
            .take(args.limit.unwrap_or(usize::MAX))
            .map(|line| args.endpoint.to_owned() + &line)
            .collect();
        Ok(Self {
            paths: Arc::new(paths),
            index: Arc::new(atomic::AtomicUsize::new(0)),
        })
    }

    fn next_path(&self) -> Option<String> {
        let idx = self.index.fetch_add(1, atomic::Ordering::Relaxed);
        if idx < self.paths.len() {
            Some(self.paths[idx].clone())
        } else {
            None
        }
    }
}

/// Split clients into multiple worker threads, each running a tokio runtime to generate load concurrently.
#[derive(Debug)]
struct Worker {
    pool: URLPool,
    driver: ClientDriver,
}

impl Worker {
    fn run(&self, args: &Args) -> anyhow::Result<Vec<driver::ClientStats>> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(async {
            let client = client_spec(args).build()?;
            let mut all_stats = Vec::new();
            let mut stream = stream::iter(iter::from_fn(|| self.pool.next_path()))
                .map(|url| self.driver.run(&client, url))
                .buffer_unordered(args.worker_concurrency);
            while let Some(result) = stream.next().await {
                all_stats.push(result);
            }
            Ok(all_stats)
        })
    }
}

/// Set up the reqwest client configuration based on the provided arguments.
///
/// We delay building the client until we are inside the tokio runtime
fn client_spec(args: &Args) -> reqwest::ClientBuilder {
    let builder = reqwest::ClientBuilder::new()
        .pool_idle_timeout(Some(Duration::from_secs(5)))
        .timeout(Duration::from_secs(5))
        .tcp_nodelay(true);
    match args.http_version {
        HttpVersion::Http1p1 => builder.http1_only(),
        HttpVersion::Http2 => builder.http2_prior_knowledge(),
    }
}

#[serde_as]
#[derive(Clone, Debug, Serialize)]
struct RunStats {
    args: Args,
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    run_duration: Duration,
    client_stats: Vec<driver::ClientStats>,
}

fn run(args: &Args) -> anyhow::Result<()> {
    let urls = URLPool::load(args)?;
    let driver: ClientDriver = ClientDriver::new(args.driver.clone())?;

    // Create before starting, so we can fail fast if the file can't be created.
    let output_file = File::create(&args.output_file)?;

    let run_start = Instant::now();

    let stats = thread::scope(move |s| -> anyhow::Result<_> {
        let threads = (0..args.num_workers)
            .map(|_| Worker {
                pool: urls.clone(),
                driver: driver.clone(),
            })
            .map(|worker| s.spawn(move || worker.run(args)))
            .collect::<Vec<_>>();

        let mut all_stats = Vec::new();
        for t in threads {
            let stats = t.join().expect("Worker thread panicked")?;
            all_stats.extend(stats);
        }
        Ok(all_stats)
    })?;

    let run_duration = run_start.elapsed();

    let run_stats = RunStats {
        args: args.clone(),
        run_duration,
        client_stats: stats,
    };

    serde_json::to_writer_pretty(output_file, &run_stats)?;

    Ok(())
}

fn main() {
    let args = Args::parse();
    if let Err(e) = run(&args) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
