//! A pool of URLs that can be iterated thread-safely across multiple worker threads.
use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
    sync::{Arc, atomic},
};

use clap::Args;
use rand::{rngs::ChaCha8Rng, seq::SliceRandom};
use serde::Serialize;

#[derive(Clone, Debug, Args, Serialize)]
pub struct URLPoolConfig {
    /// Endpoint URL prefix to send requests to.
    #[arg(short, long)]
    endpoint: String,

    /// File containing a list of relative paths to request, one per line.
    ///
    /// Each path will be appended to the endpoint URL prefix.
    #[arg(short, long)]
    path_file: PathBuf,

    /// Shuffle the order of paths.
    ///
    /// Shuffle is post-limit.
    #[arg(long)]
    shuffle: bool,

    /// Limit the number of paths to read from the path file.
    #[arg(long)]
    limit: Option<usize>,

    /// Wall time to repeat the URL pool, in seconds.
    ///
    /// If set, the URL pool will repeat until the wall time has elapsed, rather
    /// than stopping when all paths have been used.
    #[arg(long)]
    wall_time: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct URLPool {
    paths: Arc<Vec<String>>,
    index: Arc<atomic::AtomicUsize>,
    wall_end: Option<std::time::Instant>,
}

impl URLPool {
    pub fn load(config: &URLPoolConfig) -> anyhow::Result<Self> {
        let file = File::open(&config.path_file)?;
        let reader = BufReader::new(file);
        let paths = reader
            .lines()
            .filter_map(|line| match line {
                Ok(line) if !line.trim().is_empty() => Some(Ok(line)),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .take(config.limit.unwrap_or(usize::MAX))
            .map(|line| line.map(|line| format!("{}{}", config.endpoint, line)))
            .collect::<Result<Vec<_>, _>>()?;

        if paths.is_empty() {
            anyhow::bail!("path file contains no request paths");
        }
        let paths = if config.shuffle {
            let mut rng: ChaCha8Rng = rand::make_rng();
            let mut paths = paths;
            paths.shuffle(&mut rng);
            paths
        } else {
            paths
        };
        Ok(Self {
            paths: Arc::new(paths),
            index: Arc::new(atomic::AtomicUsize::new(0)),
            wall_end: config
                .wall_time
                .map(|seconds| std::time::Instant::now() + std::time::Duration::from_secs(seconds)),
        })
    }

    pub fn next_path(&self) -> Option<String> {
        let idx = self.index.fetch_add(1, atomic::Ordering::Relaxed);
        if let Some(wall_end) = self.wall_end {
            if std::time::Instant::now() < wall_end {
                Some(self.paths[idx % self.paths.len()].clone())
            } else {
                None
            }
        } else if idx < self.paths.len() {
            Some(self.paths[idx].clone())
        } else {
            None
        }
    }
}
