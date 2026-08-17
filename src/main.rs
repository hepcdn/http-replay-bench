use std::{
    fs::File,
    io::IsTerminal,
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
use tracing_subscriber::{
    Layer, filter::LevelFilter, fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt,
};

use crate::driver::{ClientDriver, DriverConfig};
use crate::hud::Hud;
use crate::transport::TransportConfig;
use crate::url::{URLPool, URLPoolConfig};

mod driver;
mod hud;
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
    #[arg(short, long, default_value_t = 4)]
    num_workers: usize,

    /// Number of concurrent requests to make per worker thread.
    #[arg(short, long, default_value_t = 100)]
    worker_concurrency: usize,

    /// Ramp-up window for gradually starting clients, in seconds.
    ///
    /// This prevents all clients from starting at once, which can overwhelm the system under test.
    #[arg(long, default_value_t = 10)]
    ramp_window: u64,

    /// Log level for tracing output.
    #[serde(skip)]
    #[arg(long, default_value_t = Level::INFO)]
    log_level: Level,

    /// Show a live heads-up display of in-flight clients on stderr.
    #[serde(skip)]
    #[arg(long)]
    hud: bool,

    #[command(subcommand)]
    driver: DriverConfig,
}

/// Split clients into multiple worker threads, each running a tokio runtime to generate load concurrently.
#[derive(Debug)]
struct Worker {
    index: usize,
    pool: URLPool,
    driver: ClientDriver,
}

impl Worker {
    fn run(&self, args: &Args) -> anyhow::Result<Vec<driver::ClientStats>> {
        let _worker_span = tracing::span!(Level::INFO, "worker",).entered();
        let launch_delay = args.ramp_window as f64 / args.worker_concurrency.max(1) as f64;
        let thread_offset = launch_delay * (self.index as f64 / args.num_workers.max(1) as f64);
        let launch_start =
            tokio::time::Instant::now() + tokio::time::Duration::from_secs_f64(thread_offset);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(async {
            let transport = transport::Transport::try_build(&args.transport)?;
            // Shared borrow (that can be moved)
            let transport = &transport;
            let mut all_stats = Vec::new();
            let mut stream = stream::iter(iter::from_fn(|| self.pool.next_path()).enumerate())
                .map(move |(i, url)| {
                    let launch_at = launch_start
                        + tokio::time::Duration::from_secs_f64(
                            launch_delay * i.min(args.worker_concurrency) as f64,
                        );
                    async move {
                        tokio::time::sleep_until(launch_at).await;
                        self.driver.run(transport, url).await
                    }
                })
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
    let run_span = tracing::span!(Level::INFO, "run");
    let _run_enter = run_span.enter();

    let stats = thread::scope(move |s| -> anyhow::Result<_> {
        let threads = (0..args.num_workers)
            .map(|index| Worker {
                index,
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

    let hud = Hud::new(args.hud);

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(hud.writer())
        .with_ansi(std::io::stderr().is_terminal())
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_filter(LevelFilter::from_level(args.log_level));

    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(hud.layer())
        .init();

    let result = run(&args);
    hud.finish();

    if let Err(e) = result {
        event!(Level::ERROR, "Application error: {e}");
        std::process::exit(1);
    }
}
