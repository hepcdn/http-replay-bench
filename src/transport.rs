//! Transport mechanism for sending requests and receiving responses.
//!
//! Provides [`reqwest::Client`] to the driver.
//!
//! TODO: this module can be refactored to support multiple transport mechanisms
//! (e.g., reqwest, hyper, etc.) and to allow for more flexible configuration of
//! the transport layer.
use std::time::Duration;

use clap::{Args, ValueEnum};
use serde::Serialize;

#[derive(Clone, Debug, Args, Serialize)]
pub struct TransportConfig {
    /// HTTP version to use for requests.
    #[arg(long, value_enum, default_value_t = HttpVersion::Http1p1)]
    http_version: HttpVersion,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Serialize)]
pub enum HttpVersion {
    /// Use HTTP/1.1 for requests.
    Http1p1,
    /// Use HTTP/2 for requests.
    Http2,
}

/// Set up the reqwest client configuration based on the provided arguments.
///
/// We delay building the client until we are inside the tokio runtime
pub fn client_spec(config: TransportConfig) -> reqwest::ClientBuilder {
    let builder = reqwest::ClientBuilder::new()
        .pool_idle_timeout(Some(Duration::from_secs(5)))
        .timeout(Duration::from_secs(5))
        .tcp_nodelay(true);
    match config.http_version {
        HttpVersion::Http1p1 => builder.http1_only(),
        HttpVersion::Http2 => builder.http2_prior_knowledge(),
    }
}
