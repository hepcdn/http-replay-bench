//! A module for discovering WLCG tokens from the environment.
//!
//! TODO: move this to a separate crate for reuse across multiple projects.
use std::{env, io::ErrorKind};

use async_trait::async_trait;
use reqwest::{Request, Response, header::AUTHORIZATION};
use reqwest_middleware::{Middleware, Next};
use thiserror::Error;

/// Token source
///
/// For use in discovery error reporting (only applicable to file sources)
#[derive(Debug)]
pub enum TokenSource {
    EnvBearerTokenFile,
    EnvXdgRuntimeDir,
    TmpDir,
}

impl std::fmt::Display for TokenSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TokenSource::EnvBearerTokenFile => write!(f, "path specified in BEARER_TOKEN_FILE"),
            TokenSource::EnvXdgRuntimeDir => write!(f, "path specified in XDG_RUNTIME_DIR"),
            TokenSource::TmpDir => write!(f, "/tmp/bt_u<uid>"),
        }
    }
}

/// Token discovery error
#[derive(Debug, Error)]
pub enum TokenDiscoveryError {
    #[error("No token found in environment or default locations")]
    NoTokenFound,
    #[error("Failed to read token from {0}: {1}")]
    FileReadError(TokenSource, std::io::Error),
}

/// Read to string from a file, returning None if the file does not exist.
fn read_to_string(token_path: &str) -> Result<Option<String>, std::io::Error> {
    match std::fs::read_to_string(token_path) {
        Ok(token) => Ok(Some(token.trim().to_string())),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Get a WLCG token from the environment
///
/// Procedure:
/// - If BEARER_TOKEN is set, use it.
/// - If BEARER_TOKEN_FILE is set, read the token from the file.
/// - If XDG_RUNTIME_DIR is set then read the token from $XDG_RUNTIME_DIR/bt_u$ID
/// - If /tmp/bt_u$ID exists, read the token from it.
///
/// TODO: cache with Lazy ArcSwap and background thread to refresh based on expiration time.
pub fn get_token() -> Result<String, TokenDiscoveryError> {
    if let Ok(token) = env::var("BEARER_TOKEN") {
        return Ok(token);
    }
    if let Ok(token_path) = env::var("BEARER_TOKEN_FILE") {
        if let Some(token) = read_to_string(&token_path)
            .map_err(|e| TokenDiscoveryError::FileReadError(TokenSource::EnvBearerTokenFile, e))?
        {
            return Ok(token);
        }
    }
    let uid = nix::unistd::Uid::current();
    if let Ok(xdg_runtime_dir) = env::var("XDG_RUNTIME_DIR") {
        let token_path = format!("{}/bt_u{}", xdg_runtime_dir, uid);
        if let Some(token) = read_to_string(&token_path)
            .map_err(|e| TokenDiscoveryError::FileReadError(TokenSource::EnvXdgRuntimeDir, e))?
        {
            return Ok(token);
        }
    }
    let token_path = format!("/tmp/bt_u{}", uid);
    if let Some(token) = read_to_string(&token_path)
        .map_err(|e| TokenDiscoveryError::FileReadError(TokenSource::TmpDir, e))?
    {
        return Ok(token);
    }
    Err(TokenDiscoveryError::NoTokenFound)
}

/// Middleware for use with reqwest to inject the authorization token
///
/// TODO: rather than lock in the auth here, we should refresh for each request
/// in case the token expires, once get_token is refactored to return a cached value.
pub struct WLCGTokenAuthMiddleware {
    token: String,
}

impl WLCGTokenAuthMiddleware {
    pub fn try_new() -> Result<Self, TokenDiscoveryError> {
        Ok(WLCGTokenAuthMiddleware {
            token: get_token()?,
        })
    }
}

#[async_trait]
impl Middleware for WLCGTokenAuthMiddleware {
    async fn handle(
        &self,
        mut req: Request,
        extensions: &mut http::Extensions,
        next: Next<'_>,
    ) -> reqwest_middleware::Result<Response> {
        let mut auth_value =
            reqwest::header::HeaderValue::from_str(format!("Bearer {}", self.token).as_str())
                .expect("token became malformed in runtime");
        auth_value.set_sensitive(true);
        req.headers_mut().insert(AUTHORIZATION, auth_value);

        next.run(req, extensions).await
    }
}
