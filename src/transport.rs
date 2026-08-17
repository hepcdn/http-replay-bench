//! Transport mechanism for sending requests and receiving responses.
//!
//! Provides [`reqwest::Client`] to the driver.
//!
//! TODO: this module can be refactored to support multiple transport mechanisms
//! (e.g., reqwest, hyper, etc.) and to allow for more flexible configuration of
//! the transport layer.
use std::{num::NonZeroUsize, time::Duration};

use clap::{Args, ValueEnum};
use serde::Serialize;

#[derive(Clone, Debug, Args, Serialize)]
pub struct TransportConfig {
    /// HTTP version to use for requests.
    #[arg(long, value_enum, default_value_t = HttpVersion::Http1p1)]
    http_version: HttpVersion,

    /// Timeout for requests, in seconds.
    #[arg(long, default_value_t = 5)]
    timeout: u64,

    /// Follow redirects when making requests.
    #[arg(long, default_value_t = true)]
    follow_redirects: bool,
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
pub fn client_spec(config: &TransportConfig) -> reqwest::ClientBuilder {
    let builder = reqwest::ClientBuilder::new()
        .pool_idle_timeout(Some(Duration::from_secs(config.timeout)))
        .timeout(Duration::from_secs(config.timeout))
        .tcp_nodelay(true);
    let builder = if config.follow_redirects {
        builder.redirect(reqwest::redirect::Policy::limited(10))
    } else {
        builder.redirect(reqwest::redirect::Policy::none())
    };
    match config.http_version {
        HttpVersion::Http1p1 => builder.http1_only(),
        HttpVersion::Http2 => builder.http2_prior_knowledge(),
    }
}

/// A transport error
///
/// Translates library errors to something simple for drivers to handle
#[derive(Clone, Debug)]
pub enum TransportError {
    /// Error occurred while sending the HEAD request.
    RequestError(String),
    /// The HEAD request failed with the given status code.
    UnexpectedStatus(u16),
    /// The response could not be parsed as a valid Content-Length header.
    InvalidContentLength,
    /// The response could not be parsed as a valid Content-Range header.
    InvalidContentRange,
    /// The response body could not be read.
    BodyReadError(String),
    // The response body could not be parsed as a valid multipart response.
    // InvalidMultipartResponse,
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportError::RequestError(e) => write!(f, "Request error: {e}"),
            TransportError::UnexpectedStatus(code) => {
                write!(f, "Unexpected status code: {code}")
            }
            TransportError::InvalidContentLength => write!(f, "Invalid Content-Length header"),
            TransportError::InvalidContentRange => write!(f, "Invalid Content-Range header"),
            TransportError::BodyReadError(e) => write!(f, "Body read error: {e}"),
        }
    }
}

/// Get the content length of the resource at the given URL by sending a HEAD request.
pub async fn head_url(client: &reqwest::Client, url: &str) -> Result<NonZeroUsize, TransportError> {
    let head_response = client
        .head(url)
        .send()
        .await
        .map_err(|e| TransportError::RequestError(e.to_string()))?;

    if head_response.status() != reqwest::StatusCode::OK {
        return Err(TransportError::UnexpectedStatus(
            head_response.status().into(),
        ));
    }
    head_response
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|val| val.to_str().ok())
        .and_then(|s| s.parse::<NonZeroUsize>().ok())
        .ok_or(TransportError::InvalidContentLength)
}

/// Make a range request to the given URL and return the total number of bytes received.
///
/// Sinks the response body to avoid buffering it in memory, and counts the total bytes received.
pub async fn range_request(
    client: &reqwest::Client,
    url: &str,
    range_header: String,
) -> Result<usize, TransportError> {
    let mut response = client
        .get(url)
        .header(reqwest::header::RANGE, range_header.as_str())
        .send()
        .await
        .map_err(|e| TransportError::RequestError(e.to_string()))?;

    // Always expect a 206 Partial Content response for range requests; otherwise, count as an error
    if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
        return Err(TransportError::UnexpectedStatus(response.status().into()));
    }

    if let Some(range) = response
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|v| v.to_str().ok())
    {
        // Single range (request, response) example:
        // Range: bytes=0-499
        // Content-Range: bytes 0-499/25000
        let expected = range_header.replace("bytes=", "bytes ");
        let observed = range.split('/').next().unwrap_or("");
        if expected != observed {
            return Err(TransportError::InvalidContentRange);
        }
    } else if let Some(content_type) = response
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

    let mut total_bytes = 0;
    while let Some(chunk) = response.chunk().await.transpose() {
        let data = chunk.map_err(|e| TransportError::BodyReadError(e.to_string()))?;
        total_bytes += data.len();
    }

    Ok(total_bytes)
}
