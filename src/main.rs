use std::{
    fs::File,
    iter,
    path::PathBuf,
    thread,
    time::{Duration, Instant},
};

use ::futures::{StreamExt, stream};
use clap::Parser;
use serde::Serialize;
use serde_with::{DurationSecondsWithFrac, serde_as};
use tracing::{Level, event};
use tracing_subscriber::fmt::format::FmtSpan;

use crate::driver::{ClientDriver, DriverConfig};
use crate::transport::TransportConfig;
use crate::url::{URLPool, URLPoolConfig};

mod driver;
mod trace;
mod transport;
mod url;
mod wlcg_token_discovery;

/// A load generator for benchmarking distributed storage via http protocol.
///
/// This program focuses on generating client load that recreates typical
/// application load as given by prerecorded traces, where the typical access
/// pattern is to make repeated range requests against large (multi-gigabyte)
/// URLs. The program is massively multithreaded to be able to simulate tens of
/// thousands of concurrent clients, and it is designed to be run on a single
/// machine with a large number of CPU cores and network bandwidth.
#[derive(Clone, Parser, Debug, Serialize)]
#[command(version)]
struct Args {
    #[command(flatten)]
    url_config: URLPoolConfig,

    /// Output file to write the results to.
    #[arg(short, long, default_value = "results.json")]
    output_file: PathBuf,

    #[command(flatten)]
    transport: TransportConfig,

    /// Number of worker threads to spawn for generating load.
    #[arg(short, long, default_value_t = 16)]
    num_workers: usize,

    /// Number of concurrent requests to make per worker thread.
    #[arg(short, long, default_value_t = 1000)]
    worker_concurrency: usize,

    /// Log level for tracing output.
    #[serde(skip)]
    #[arg(long, default_value_t = Level::INFO)]
    log_level: Level,

    #[command(subcommand)]
    driver: DriverConfig,
}

/// Split clients into multiple worker threads, each running a tokio runtime to generate load concurrently.
#[derive(Debug)]
struct Worker {
    pool: URLPool,
    driver: ClientDriver,
}

impl Worker {
    fn run(&self, args: &Args) -> anyhow::Result<Vec<driver::ClientStats>> {
        let _worker_span = tracing::span!(
            Level::INFO,
            "worker",
            worker_concurrency = args.worker_concurrency
        )
        .entered();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(async {
            let transport = transport::Transport::try_build(&args.transport)?;
            let mut all_stats = Vec::new();
            let mut stream = stream::iter(iter::from_fn(|| self.pool.next_path()))
                .map(|url| self.driver.run(&transport, url))
                .buffer_unordered(args.worker_concurrency);
            while let Some(result) = stream.next().await {
                all_stats.push(result);
            }
            Ok(all_stats)
        })
    }
}

#[serde_as]
#[derive(Clone, Debug, Serialize)]
struct RunStats {
    args: Args,
    /// Total wall time taken for the run.
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    wall_time: Duration,
    client_stats: Vec<driver::ClientStats>,
}

fn run(args: &Args) -> anyhow::Result<()> {
    let urls = URLPool::load(&args.url_config)?;
    let driver: ClientDriver = ClientDriver::new(args.driver.clone())?;

    // Create before starting, so we can fail fast if the file can't be created.
    let output_file = File::create(&args.output_file)?;

    let run_start = Instant::now();
    let run_span = tracing::span!(Level::INFO, "run", num_workers = args.num_workers);
    let _run_enter = run_span.enter();

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
        wall_time: run_duration,
        client_stats: stats,
    };

    serde_json::to_writer_pretty(output_file, &run_stats)?;

    Ok(())
}

fn main() {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_max_level(args.log_level)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .init();
    if let Err(e) = run(&args) {
        event!(Level::ERROR, "Application error: {e}");
        std::process::exit(1);
    }
}
