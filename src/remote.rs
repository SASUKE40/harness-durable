use crate::{
    archive::{self, validate_manifest},
    config::RemoteConfig,
    model::{Manifest, Query},
    state::State,
};
use anyhow::{Context, Result, bail, ensure};
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, path::Path as ObjectPath};
use reqwest::{Client, Method};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::io::AsyncWriteExt;

#[derive(Debug)]
struct AwsCredentials(aws_credential_types::provider::SharedCredentialsProvider);
#[async_trait::async_trait]
impl object_store::CredentialProvider for AwsCredentials {
    type Credential = object_store::aws::AwsCredential;
    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        use aws_credential_types::provider::ProvideCredentials;
        let c = self
            .0
            .provide_credentials()
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "AWS credentials",
                source: Box::new(e),
            })?;
        Ok(Arc::new(object_store::aws::AwsCredential {
            key_id: c.access_key_id().into(),
            secret_key: c.secret_access_key().into(),
            token: c.session_token().map(str::to_owned),
        }))
    }
}

pub enum Remote {
    S3 {
        store: Arc<dyn ObjectStore>,
        prefix: String,
    },
    Cloudflare {
        client: Client,
        url: String,
        token: String,
    },
}

impl Remote {
    pub async fn new(config: &RemoteConfig) -> Result<Self> {
        match config {
            RemoteConfig::S3 {
                bucket,
                prefix,
                endpoint,
                region,
                allow_http,
                ..
            } => {
                let sdk = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
                let mut b = object_store::aws::AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .with_allow_http(*allow_http);
                if let Some(r) = region
                    .as_deref()
                    .or_else(|| sdk.region().map(|r| r.as_ref()))
                {
                    b = b.with_region(r);
                }
                if let Some(e) = endpoint {
                    b = b.with_endpoint(e);
                }
                if let Some(provider) = sdk.credentials_provider() {
                    b = b.with_credentials(Arc::new(AwsCredentials(provider)));
                }
                let prefix = prefix.trim_matches('/').to_string();
                ensure!(
                    prefix.is_empty() || archive::safe_relative(&prefix),
                    "invalid S3 prefix"
                );
                Ok(Self::S3 {
                    store: Arc::new(b.build()?),
                    prefix,
                })
            }
            RemoteConfig::Cloudflare {
                url,
                archive,
                token_env,
                ..
            } => {
                ensure!(
                    archive::safe_relative(archive) && !archive.contains('/'),
                    "invalid archive ID"
                );
                let parsed = reqwest::Url::parse(url)?;
                ensure!(
                    parsed.scheme() == "https"
                        || (parsed.scheme() == "http"
                            && matches!(
                                parsed.host_str(),
                                Some("localhost" | "127.0.0.1" | "[::1]")
                            )),
                    "Cloudflare URL requires HTTPS (except localhost)"
                );
                let token = std::env::var(token_env)
                    .with_context(|| format!("missing token environment variable {token_env}"))?;
                ensure!(!token.is_empty(), "empty API token");
                Ok(Self::Cloudflare {
                    client: Client::builder()
                        .timeout(Duration::from_secs(300))
                        .redirect(reqwest::redirect::Policy::none())
                        .build()?,
                    url: format!("{}/v1/archives/{}", url.trim_end_matches('/'), archive),
                    token,
                })
            }
        }
    }
    fn key(prefix: &str, suffix: &str) -> ObjectPath {
        ObjectPath::from(if prefix.is_empty() {
            suffix.to_string()
        } else {
            format!("{prefix}/{suffix}")
        })
    }
    fn batch(m: &Manifest) -> String {
        format!("{}/{}", m.collector_id, m.batch_id)
    }
    async fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<Vec<u8>>,
        file: Option<&Path>,
    ) -> Result<reqwest::Response> {
        let Self::Cloudflare { client, url, token } = self else {
            bail!("HTTP backend required")
        };
        let mut request = client
            .request(method, format!("{url}/{path}"))
            .bearer_auth(token);
        if let Some(body) = body {
            request = request
                .header("Content-Type", "application/json")
                .body(body);
        }
        if let Some(path) = file {
            let f = tokio::fs::File::open(path).await?;
            request = request
                .header("Content-Length", f.metadata().await?.len())
                .body(reqwest::Body::wrap_stream(
                    tokio_util::io::ReaderStream::new(f),
                ));
        }
        Ok(request.send().await?.error_for_status()?)
    }
    pub async fn upload(&self, path: &Path) -> Result<()> {
        let m = archive::manifest(path)?;
        archive::verify(path, &m)?;
        for attempt in 0..5 {
            match self.upload_once(path, &m).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if attempt == 4 || !retryable(&e) {
                        return Err(e);
                    }
                    tokio::time::sleep(Duration::from_millis(250 * (1 << attempt))).await;
                }
            }
        }
        unreachable!()
    }
    async fn upload_once(&self, path: &Path, m: &Manifest) -> Result<()> {
        let batch = Self::batch(m);
        let data = serde_json::to_vec(m)?;
        match self {
            Self::S3 { store, prefix } => {
                // Reserve a batch identity before any upload. Conditional creation
                // prevents a concurrent writer from changing the inventory.
                put_identical(
                    store,
                    &Self::key(prefix, &format!("{batch}/registration.json")),
                    &data,
                )
                .await?;
                let marker = Self::key(prefix, &format!("{batch}/manifest.json"));
                match store.get(&marker).await {
                    Ok(v) => {
                        let old: Manifest = serde_json::from_slice(&v.bytes().await?)?;
                        ensure!(&old == m, "conflicting published batch");
                        return Ok(());
                    }
                    Err(object_store::Error::NotFound { .. }) => {}
                    Err(e) => return Err(e.into()),
                }
                for f in &m.files {
                    let key = Self::key(prefix, &format!("{batch}/{}", f.path));
                    let mut writer = object_store::buffered::BufWriter::new(store.clone(), key);
                    let mut file = tokio::fs::File::open(path.join(&f.path)).await?;
                    tokio::io::copy(&mut file, &mut writer).await?;
                    writer.shutdown().await?;
                }
                put_identical(store, &marker, &data).await?;
            }
            Self::Cloudflare { .. } => {
                self.request(Method::PUT, &format!("batches/{batch}"), Some(data), None)
                    .await?;
                for f in &m.files {
                    self.request(
                        Method::PUT,
                        &format!("batches/{batch}/files/{}", f.path),
                        None,
                        Some(&path.join(&f.path)),
                    )
                    .await?;
                }
                self.request(Method::POST, &format!("batches/{batch}/commit"), None, None)
                    .await?;
            }
        }
        Ok(())
    }
    pub async fn list(&self) -> Result<Vec<Manifest>> {
        let mut result = Vec::new();
        match self {
            Self::S3 { store, prefix } => {
                let prefix = Self::key(prefix, "");
                let mut objects = store.list(Some(&prefix));
                while let Some(o) = objects.try_next().await? {
                    if o.location.as_ref().ends_with("/manifest.json") {
                        let m: Manifest =
                            serde_json::from_slice(&store.get(&o.location).await?.bytes().await?)?;
                        validate_manifest(&m)?;
                        result.push(m);
                    }
                }
            }
            Self::Cloudflare { .. } => {
                let mut after = String::new();
                loop {
                    let page: serde_json::Value = self
                        .request(Method::GET, &format!("batches?after={after}"), None, None)
                        .await?
                        .json()
                        .await?;
                    for item in page["batches"].as_array().context("invalid batch list")? {
                        let m: Manifest = serde_json::from_value(item.clone())?;
                        validate_manifest(&m)?;
                        result.push(m);
                    }
                    if let Some(cursor) = page["next"].as_str() {
                        ensure!(cursor != after, "pagination did not advance");
                        after = cursor.into();
                    } else {
                        break;
                    }
                }
            }
        }
        Ok(result)
    }
    pub async fn download(&self, m: &Manifest, cache: &Path) -> Result<PathBuf> {
        validate_manifest(m)?;
        let batch = Self::batch(m);
        let target = cache.join(&batch);
        if target.exists() {
            let existing = archive::manifest(&target)?;
            ensure!(&existing == m, "cache manifest conflict");
            archive::verify(&target, m)?;
            return Ok(target);
        }
        let staging = cache.join(format!(".tmp-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&staging).await?;
        let result: Result<()> = async {
            for f in &m.files {
                let p = staging.join(&f.path);
                tokio::fs::create_dir_all(p.parent().context("file parent")?).await?;
                let mut out = tokio::fs::File::create(&p).await?;
                match self {
                    Self::S3 { store, prefix } => {
                        let mut stream = store
                            .get(&Self::key(prefix, &format!("{batch}/{}", f.path)))
                            .await?
                            .into_stream();
                        while let Some(chunk) = stream.try_next().await? {
                            out.write_all(&chunk).await?;
                        }
                    }
                    Self::Cloudflare { .. } => {
                        let mut stream = self
                            .request(
                                Method::GET,
                                &format!("batches/{batch}/files/{}", f.path),
                                None,
                                None,
                            )
                            .await?
                            .bytes_stream();
                        while let Some(chunk) = stream.try_next().await? {
                            out.write_all(&chunk).await?;
                        }
                    }
                }
                out.sync_all().await?;
            }
            archive::verify(&staging, m)?;
            tokio::fs::write(staging.join("manifest.json"), serde_json::to_vec_pretty(m)?).await?;
            tokio::fs::create_dir_all(target.parent().context("cache parent")?).await?;
            tokio::fs::rename(&staging, &target).await?;
            Ok(())
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_dir_all(staging).await;
        }
        result?;
        Ok(target)
    }
}

async fn put_identical(store: &Arc<dyn ObjectStore>, key: &ObjectPath, data: &[u8]) -> Result<()> {
    match store
        .put_opts(
            key,
            data.to_vec().into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(
            object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. },
        ) => {
            let existing = store.get(key).await?.bytes().await?;
            let a: Manifest = serde_json::from_slice(&existing)?;
            let b: Manifest = serde_json::from_slice(data)?;
            ensure!(a == b, "conflicting batch ID: {key}");
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}
fn retryable(e: &anyhow::Error) -> bool {
    if let Some(http) = e.downcast_ref::<reqwest::Error>() {
        return http
            .status()
            .is_none_or(|s| s.is_server_error() || s.as_u16() == 429);
    }
    e.downcast_ref::<object_store::Error>().is_some()
        || e.downcast_ref::<std::io::Error>().is_some()
}
pub async fn sync(state: &State, configs: &[RemoteConfig], selected: Option<&str>) -> Result<()> {
    if let Some(name) = selected {
        ensure!(
            configs.iter().any(|r| r.name() == name),
            "unknown remote {name}"
        );
    }
    let mut errors = Vec::new();
    for config in configs
        .iter()
        .filter(|r| selected.is_none_or(|s| s == r.name()))
    {
        let result: Result<()> = async {
            let remote = Remote::new(config).await?;
            for path in state.batch_paths()? {
                let m = archive::manifest(&path)?;
                if !state.uploaded(config.name(), &m.batch_id)? {
                    remote.upload(&path).await?;
                    state.mark_uploaded(config.name(), &m.batch_id)?;
                }
            }
            Ok(())
        }
        .await;
        if let Err(e) = result {
            errors.push(format!("{}: {e:#}", config.name()));
        }
    }
    ensure!(errors.is_empty(), "{}", errors.join("; "));
    Ok(())
}
pub async fn cached_paths(config: &RemoteConfig, root: &Path, q: &Query) -> Result<Vec<PathBuf>> {
    let remote = Remote::new(config).await?;
    let mut paths = Vec::new();
    let cache = root.join("cache").join(crate::model::hash(config.name()));
    for m in remote.list().await? {
        if m.sessions.iter().any(|s| q.matches_session(s)) {
            paths.push(remote.download(&m, &cache).await?);
        }
    }
    Ok(paths)
}

pub enum SyncUpdate {
    Uploaded { remote: String, batch: String },
    Failed(String),
    Finished,
}

/// Network transfers never hold up file capture. Only the collector task writes
/// SQLite; the background uploader reports successful publications over a channel.
pub fn background_sync(
    state: &State,
    configs: &[RemoteConfig],
    tx: tokio::sync::mpsc::UnboundedSender<SyncUpdate>,
) -> Result<tokio::task::JoinHandle<()>> {
    let mut jobs = Vec::new();
    for config in configs {
        let mut paths = Vec::new();
        for path in state.batch_paths()? {
            let m = archive::manifest(&path)?;
            if !state.uploaded(config.name(), &m.batch_id)? {
                paths.push((path, m.batch_id));
            }
        }
        if !paths.is_empty() {
            jobs.push((config.clone(), paths));
        }
    }
    Ok(tokio::spawn(async move {
        for (config, paths) in jobs {
            let result: Result<()> = async {
                let remote = Remote::new(&config).await?;
                for (path, batch) in paths {
                    remote.upload(&path).await?;
                    let _ = tx.send(SyncUpdate::Uploaded {
                        remote: config.name().into(),
                        batch,
                    });
                }
                Ok(())
            }
            .await;
            if let Err(e) = result {
                let _ = tx.send(SyncUpdate::Failed(format!("{}: {e:#}", config.name())));
            }
        }
        let _ = tx.send(SyncUpdate::Finished);
    }))
}
