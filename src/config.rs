use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Empty means `~/.harness-durable`, resolved by `Config::load`.
    pub state_dir: PathBuf,
    pub flush_seconds: u64,
    pub max_records: usize,
    pub max_bytes: usize,
    pub rescan_seconds: u64,
    /// `watch` merges small local batches this often.
    pub compact_seconds: u64,
    /// Upper size of a merged batch, in bytes of Lance files.
    pub compact_max_bytes: u64,
    pub sources: Vec<SourceConfig>,
    pub remotes: Vec<RemoteConfig>,
    pub feedback: crate::feedback::FeedbackConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            state_dir: PathBuf::new(),
            flush_seconds: 5,
            max_records: 1000,
            max_bytes: 8 * 1024 * 1024,
            rescan_seconds: 30,
            compact_seconds: 600,
            compact_max_bytes: 128 * 1024 * 1024,
            sources: vec![],
            remotes: vec![],
            feedback: crate::feedback::FeedbackConfig::default(),
        }
    }
}

fn default_state_dir() -> Result<PathBuf> {
    Ok(directories::BaseDirs::new()
        .context("cannot find the home directory; use --state-dir and --config")?
        .home_dir()
        .join(".harness-durable"))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    pub harness: String,
    pub path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RemoteConfig {
    S3 {
        name: String,
        bucket: String,
        #[serde(default)]
        prefix: String,
        endpoint: Option<String>,
        region: Option<String>,
        #[serde(default)]
        allow_http: bool,
    },
    Cloudflare {
        name: String,
        url: String,
        archive: String,
        token_env: String,
    },
}

impl RemoteConfig {
    pub fn name(&self) -> &str {
        match self {
            Self::S3 { name, .. } | Self::Cloudflare { name, .. } => name,
        }
    }
}

impl Config {
    pub fn load(path: Option<&Path>, state_dir: Option<PathBuf>) -> Result<Self> {
        let mut config: Self = match path {
            Some(p) => {
                ensure!(p.exists(), "config does not exist: {}", p.display());
                toml::from_str(&std::fs::read_to_string(p)?)
                    .with_context(|| format!("invalid config {}", p.display()))?
            }
            None => match default_state_dir().map(|d| d.join("config.toml")) {
                Ok(p) if p.exists() => toml::from_str(&std::fs::read_to_string(&p)?)
                    .with_context(|| format!("invalid config {}", p.display()))?,
                _ => Self::default(),
            },
        };
        if let Some(s) = state_dir {
            config.state_dir = s;
        }
        if config.state_dir.as_os_str().is_empty() {
            config.state_dir = default_state_dir()?;
        }
        ensure!(
            config.flush_seconds > 0
                && config.max_records > 0
                && config.max_bytes > 0
                && config.rescan_seconds > 0
                && config.compact_seconds > 0
                && config.compact_max_bytes > 0,
            "batch, scan, and compaction thresholds must be positive"
        );
        let mut names = std::collections::HashSet::new();
        config.feedback.validate()?;
        for source in &config.sources {
            crate::adapters::adapter(&source.harness)?;
        }
        for remote in &config.remotes {
            ensure!(!remote.name().is_empty(), "remote name must not be empty");
            ensure!(names.insert(remote.name()), "duplicate remote name");
        }
        Ok(config)
    }
}
