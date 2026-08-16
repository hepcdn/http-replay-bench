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

use crate::trace;

#[serde_as]
#[derive(Clone, Debug, Serialize)]
pub struct ClientStats {
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
    /// Total number of errors encountered during requests.
    errors: usize,
}

impl ClientStats {
    fn new() -> Self {
        Self {
            start_time: SystemTime::now(),
            stop_time: SystemTime::now(),
            requests: 0,
            total_bytes: 0,
            seconds_reading: Duration::ZERO,
            seconds_sleeping: Duration::ZERO,
            errors: 0,
        }
    }
}

async fn get_content_length(client: &reqwest::Client, url: &str) -> Option<NonZeroUsize> {
    // HEAD to get the total size of the resource
    let head_response = client.head(url).send().await.ok()?;
    if !head_response.status().is_success() {
        return None;
    }
    head_response
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|val| val.to_str().ok())
        .and_then(|s| s.parse::<NonZeroUsize>().ok())
}

/// Helper struct to hold the `sink_response` stats
#[derive(Clone, Debug)]
struct SinkResponseResult {
    total_bytes: usize,
    errors: usize,
}

impl SinkResponseResult {
    fn new() -> Self {
        Self {
            total_bytes: 0,
            errors: 0,
        }
    }
}

/// Sink the response body to /dev/null, returning the total number of bytes read and error count
async fn sink_response(
    response: Result<reqwest::Response, reqwest::Error>,
    expected_range: String,
) -> SinkResponseResult {
    let mut result = SinkResponseResult::new();

    // Errors in sending or with redirect loop/exhaustion
    let Ok(mut response) = response else {
        result.errors += 1;
        return result;
    };

    // Always expect a 206 Partial Content response for range requests; otherwise, count as an error
    if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
        result.errors += 1;
        return result;
    }
    if let Some(range) = response
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|v| v.to_str().ok())
    {
        // Single range (request, response) example:
        // Range: bytes=0-499
        // Content-Range: bytes 0-499/25000
        let expected = expected_range.replace("bytes=", "bytes ");
        let observed = range.split('/').next().unwrap_or("");
        if expected != observed {
            result.errors += 1;
            return result;
        }
    }
    if let Some(content_type) = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        && content_type.starts_with("multipart/byteranges; boundary")
    {
        // Multipart responses
        // TODO: Validate the multipart response matches the expected ranges
        // The streamed body below will include the multipart headers and
        // boundaries, so we can't just count bytes. For now, we'll just
        // count the total bytes read and not validate the content.
    }

    while let Some(chunk) = response.chunk().await.transpose() {
        if let Ok(bytes) = chunk {
            result.total_bytes += bytes.len();
        } else {
            result.errors += 1;
            break;
        }
    }

    result
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

    async fn run(&self, client: &reqwest::Client, url: &str) -> ClientStats {
        let mut stats = ClientStats::new();

        let Some(content_length) = get_content_length(client, url).await else {
            stats.errors += 1;
            return stats;
        };

        for action in self.trace.actions() {
            match action {
                trace::Action::Request(range) => {
                    let range_header = range.to_header_value(content_length.into());
                    let request_start = Instant::now();
                    let response = client
                        .get(url)
                        .header(reqwest::header::RANGE, range_header.as_str())
                        .send()
                        .await;
                    let sink_result = sink_response(response, range_header).await;
                    stats.total_bytes += sink_result.total_bytes;
                    stats.errors += sink_result.errors;
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

    async fn run(&self, client: &reqwest::Client, url: &str) -> ClientStats {
        let mut stats = ClientStats::new();

        let Some(content_length) = get_content_length(client, url).await else {
            stats.errors += 1;
            return stats;
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
            let response = client
                .get(url)
                .header(reqwest::header::RANGE, range_header.as_str())
                .send()
                .await;
            let sink_result = sink_response(response, range_header).await;
            stats.total_bytes += sink_result.total_bytes;
            stats.errors += sink_result.errors;
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

    pub async fn run(&self, client: &reqwest::Client, url: String) -> ClientStats {
        match self {
            ClientDriver::Replay(driver) => driver.run(client, &url).await,
            ClientDriver::Pattern(driver) => driver.run(client, &url).await,
        }
    }
}
