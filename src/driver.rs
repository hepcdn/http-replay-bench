use std::{
    num::NonZeroUsize,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use clap::{Args, Subcommand};
use rand::{RngExt, SeedableRng, rngs::ChaCha8Rng};
use serde::Serialize;
use serde_with::{DurationSecondsWithFrac, TimestampSecondsWithFrac, serde_as};
use sha2::{Digest, Sha256};
use tracing::{Level, event};

use crate::{trace, transport::Transport};

#[serde_as]
#[derive(Clone, Debug, Serialize)]
pub struct ClientStats {
    url: String,
    #[serde_as(as = "TimestampSecondsWithFrac<f64>")]
    start_time: SystemTime,
    #[serde_as(as = "TimestampSecondsWithFrac<f64>")]
    stop_time: SystemTime,
    /// Number of requests made by the client.
    requests: usize,
    /// Total number of bytes read from responses.
    total_bytes: usize,
    /// Total seconds spent reading responses (excluding sleep time).
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    seconds_reading: Duration,
    /// Total seconds spent sleeping between requests.
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    seconds_sleeping: Duration,
    /// Last error encountered by the client, if any.
    error: Option<String>,
}

impl ClientStats {
    fn new(url: &str) -> Self {
        Self {
            url: url.to_owned(),
            start_time: SystemTime::now(),
            stop_time: SystemTime::now(),
            requests: 0,
            total_bytes: 0,
            seconds_reading: Duration::ZERO,
            seconds_sleeping: Duration::ZERO,
            error: None,
        }
    }
}

#[derive(Clone, Debug, Args, Serialize)]
pub struct ReplayConfig {
    /// Path to the trace file to replay.
    trace_file: PathBuf,

    /// Scale factor for wait time between requests.
    #[arg(long, default_value_t = 1.0)]
    wait_scale: f64,
}

#[derive(Clone, Debug)]
pub struct ReplayDriver {
    config: ReplayConfig,
    /// The trace to replay (heavy object, so wrapped in Arc to avoid cloning).
    trace: Arc<trace::Trace>,
}

impl ReplayDriver {
    pub fn new(config: ReplayConfig) -> anyhow::Result<Self> {
        let trace = trace::Trace::read_json(&config.trace_file)?;
        Ok(Self {
            config,
            trace: Arc::new(trace),
        })
    }

    async fn run(&self, transport: &Transport, url: &str) -> ClientStats {
        let mut stats = ClientStats::new(url);

        let content_length = match transport.head_url(url).await {
            Ok(length) => length,
            Err(e) => {
                stats.error = Some(format!("Failed to get content length: {e}"));
                return stats;
            }
        };
        event!(Level::DEBUG, "Content length for {url}: {content_length}");

        for action in self.trace.actions() {
            match action {
                trace::Action::Request(range) => {
                    let range_header = range.to_header_value(content_length.into());
                    let request_start = Instant::now();
                    let request_bytes = transport.range_request(url, range_header).await;
                    match request_bytes {
                        Ok(bytes) => {
                            stats.total_bytes += bytes;
                        }
                        Err(e) => {
                            stats.error = Some(format!("Request failed: {e}"));
                            return stats;
                        }
                    }
                    stats.seconds_reading += request_start.elapsed();
                    stats.requests += 1;
                }
                trace::Action::Sleep(ms) => {
                    let sleep_duration =
                        Duration::from_millis((*ms as f64 * self.config.wait_scale) as u64);
                    tokio::time::sleep(sleep_duration).await;
                    stats.seconds_sleeping += sleep_duration;
                }
            }
        }

        stats.stop_time = SystemTime::now();
        stats
    }
}

#[derive(Clone, Debug, Args, Serialize)]
pub struct PatternConfig {
    /// Number of requests to generate.
    #[arg(short, long)]
    num_requests: usize,

    /// Size of each request in bytes.
    #[arg(short, long)]
    request_size: NonZeroUsize,

    /// Deterministically seed the sequence of random ranges by the URL
    #[arg(long, default_value_t = false)]
    deterministic: bool,
}

#[derive(Clone, Debug)]
pub struct PatternDriver {
    config: PatternConfig,
}

impl PatternDriver {
    pub fn new(config: PatternConfig) -> Self {
        Self { config }
    }

    async fn run(&self, transport: &Transport, url: &str) -> ClientStats {
        let mut stats = ClientStats::new(url);

        let content_length = match transport.head_url(url).await {
            Ok(length) => length,
            Err(e) => {
                stats.error = Some(format!("Failed to get content length: {e}"));
                return stats;
            }
        };

        let request_length = self.config.request_size.get().min(content_length.get());
        let max_start = content_length.get().saturating_sub(request_length);
        let seed = if self.config.deterministic {
            Sha256::digest(url.as_bytes()).into()
        } else {
            rand::random()
        };
        let mut rng = ChaCha8Rng::from_seed(seed);

        for _ in 0..self.config.num_requests {
            let start = rng.random_range(0..=max_start);
            let end = start + request_length - 1;
            let range_header = format!("bytes={start}-{end}");
            let request_start = Instant::now();
            let request_bytes = transport.range_request(url, range_header).await;
            match request_bytes {
                Ok(bytes) => {
                    stats.total_bytes += bytes;
                }
                Err(e) => {
                    stats.error = Some(format!("Request failed: {e}"));
                    return stats;
                }
            }
            stats.seconds_reading += request_start.elapsed();
            stats.requests += 1;
        }

        stats.stop_time = SystemTime::now();
        stats
    }
}

#[derive(Clone, Debug, Subcommand, Serialize)]
pub enum DriverConfig {
    Replay(ReplayConfig),
    Pattern(PatternConfig),
}

#[derive(Clone, Debug)]
pub enum ClientDriver {
    Replay(ReplayDriver),
    Pattern(PatternDriver),
}

impl ClientDriver {
    pub fn new(config: DriverConfig) -> anyhow::Result<Self> {
        match config {
            DriverConfig::Replay(replay_config) => {
                let driver = ReplayDriver::new(replay_config)?;
                Ok(Self::Replay(driver))
            }
            DriverConfig::Pattern(pattern_config) => {
                let driver = PatternDriver::new(pattern_config);
                Ok(Self::Pattern(driver))
            }
        }
    }

    #[tracing::instrument(name = "driver.run", level = Level::DEBUG, skip_all)]
    pub async fn run(&self, transport: &Transport, url: String) -> ClientStats {
        let stats = match self {
            ClientDriver::Replay(driver) => driver.run(transport, &url).await,
            ClientDriver::Pattern(driver) => driver.run(transport, &url).await,
        };
        event!(Level::DEBUG, "Client finished: {stats:?}");
        stats
    }
}
