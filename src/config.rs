use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub state_dir: PathBuf,
    pub flush_seconds: u64,
    pub max_records: usize,
    pub max_bytes: usize,
    pub rescan_seconds: u64,
    pub sources: Vec<SourceConfig>,
    pub remotes: Vec<RemoteConfig>,
}

impl Default for Config {
    fn default() -> Self {
        let home = directories::BaseDirs::new().expect("home directory");
        Self {
            state_dir: home.home_dir().join(".harness-durable"),
            flush_seconds: 5,
            max_records: 1000,
            max_bytes: 8 * 1024 * 1024,
            rescan_seconds: 30,
            sources: vec![],
            remotes: vec![],
        }
    }
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
        let default_path = Self::default().state_dir.join("config.toml");
        let p = path.unwrap_or(&default_path);
        let mut config: Self = if p.exists() {
            toml::from_str(&std::fs::read_to_string(p)?)
                .with_context(|| format!("invalid config {}", p.display()))?
        } else {
            ensure!(path.is_none(), "config does not exist: {}", p.display());
            Self::default()
        };
        if let Some(s) = state_dir {
            config.state_dir = s;
        }
        ensure!(
            config.flush_seconds > 0
                && config.max_records > 0
                && config.max_bytes > 0
                && config.rescan_seconds > 0,
            "batch and scan thresholds must be positive"
        );
        let mut names = std::collections::HashSet::new();
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
