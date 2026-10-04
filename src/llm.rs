//! Small, configurable inference transport. No provider/model names are guessed.
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    ChatCompletions,
    Anthropic,
    Typesafe,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConfig {
    pub protocol: Protocol,
    /// Full inference endpoint, including /v1/messages or /v1/chat/completions.
    pub endpoint: String,
    pub model: String,
    pub token_env: Option<String>,
    #[serde(default = "default_tokens")]
    pub max_tokens: u32,
}
fn default_tokens() -> u32 {
    4096
}

impl ModelConfig {
    pub fn validate(&self) -> Result<()> {
        let url = reqwest::Url::parse(&self.endpoint).context("invalid model endpoint")?;
        ensure!(
            url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none(),
            "put model credentials in token_env, not endpoint"
        );
        ensure!(
            url.scheme() == "https"
                || (url.scheme() == "http"
                    && matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"))),
            "model endpoints require HTTPS except on loopback"
        );
        ensure!(
            !self.model.trim().is_empty() && self.max_tokens > 0,
            "model and positive max_tokens are required"
        );
        Ok(())
    }
}

#[async_trait::async_trait]
pub trait LanguageModel: Send + Sync {
    async fn complete(&self, system: &str, input: &Value) -> Result<String>;
}

pub struct HttpModel {
    config: ModelConfig,
    token: Option<String>,
    client: reqwest::Client,
}
impl HttpModel {
    pub fn new(config: ModelConfig) -> Result<Self> {
        config.validate()?;
        let token = config
            .token_env
            .as_ref()
            .map(|name| {
                let token = std::env::var(name).with_context(|| {
                    format!("missing model credential environment variable {name}")
                })?;
                ensure!(
                    !token.trim().is_empty(),
                    "empty model credential environment variable {name}"
                );
                Ok(token)
            })
            .transpose()?;
        Ok(Self {
            config,
            token,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(120))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
        })
    }
}

#[async_trait::async_trait]
impl LanguageModel for HttpModel {
    async fn complete(&self, system: &str, input: &Value) -> Result<String> {
        let c = &self.config;
        let input_text = serde_json::to_string(input)?;
        let body = match c.protocol {
            Protocol::ChatCompletions => json!({"model":c.model,"max_tokens":c.max_tokens,
                "messages":[{"role":"system","content":system},{"role":"user","content":input_text}]}),
            Protocol::Anthropic => {
                json!({"model":c.model,"max_tokens":c.max_tokens,"system":system,
                "messages":[{"role":"user","content":input_text}]})
            }
            Protocol::Typesafe => json!({"model":c.model,"state":input,"questions":{"reward":{
                "type":"choice", "instructions":system.split(" Return ONLY").next().unwrap_or(system),
                "criteria":{"0":"The supplied evaluation criterion is not satisfied or is unsupported by evidence.","1":"The supplied evaluation criterion is satisfied by observed evidence; accept valid alternative solutions."}}}}),
        };
        for attempt in 0..4 {
            let mut req = self.client.post(&c.endpoint).json(&body);
            if matches!(c.protocol, Protocol::Anthropic) {
                req = req.header("anthropic-version", "2023-06-01");
                if let Some(token) = &self.token {
                    req = req.header("x-api-key", token);
                }
            } else if let Some(token) = &self.token {
                req = req.bearer_auth(token);
            }
            let response = req.send().await;
            let transient = match &response {
                Ok(r) => r.status().as_u16() == 429 || r.status().is_server_error(),
                Err(e) => e.is_timeout() || e.is_connect(),
            };
            if transient && attempt < 3 {
                let retry_after = response
                    .as_ref()
                    .ok()
                    .and_then(|r| r.headers().get("retry-after"))
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .map(|seconds| Duration::from_secs(seconds.min(30)));
                tokio::time::sleep(
                    retry_after.unwrap_or(Duration::from_millis(250 * (1 << attempt))),
                )
                .await;
                continue;
            }
            let mut response = response.map_err(|_| {
                anyhow::anyhow!("model request failed after {} attempt(s)", attempt + 1)
            })?;
            // Do not echo provider error bodies, which can contain input or credentials.
            ensure!(
                response.status().is_success(),
                "model endpoint returned HTTP {}",
                response.status()
            );
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await.context("reading model response")? {
                ensure!(
                    bytes.len() + chunk.len() <= 4 * 1024 * 1024,
                    "model response exceeds 4 MiB"
                );
                bytes.extend_from_slice(&chunk);
            }
            let v: Value =
                serde_json::from_slice(&bytes).context("invalid provider JSON response")?;
            let output = match c.protocol {
                Protocol::ChatCompletions => {
                    ensure!(
                        v["choices"][0]["finish_reason"] == "stop",
                        "model response incomplete or refused"
                    );
                    v["choices"][0]["message"]["content"]
                        .as_str()
                        .context("missing model text")?
                        .to_string()
                }
                Protocol::Typesafe => {
                    let answer = &v["answers"]["reward"];
                    ensure!(
                        answer["type"] == "choice",
                        "Jev must return a choice answer"
                    );
                    let reward = match answer["choice"].as_str() {
                        Some("0") => 0,
                        Some("1") => 1,
                        _ => bail!("Jev reward must be choice 0 or 1"),
                    };
                    let probability = |key: &str| -> Result<f64> {
                        let p = answer["probabilities"][key]
                            .as_f64()
                            .context("missing Jev probability")?;
                        ensure!((0.0..=1.0).contains(&p), "invalid Jev probability");
                        Ok(p)
                    };
                    ensure!(
                        (probability("0")? + probability("1")? - 1.0).abs() < 0.001,
                        "Jev probabilities must sum to one"
                    );
                    let confidence = answer["confidence"]
                        .as_f64()
                        .context("missing Jev confidence")?;
                    ensure!((0.0..=1.0).contains(&confidence), "invalid Jev confidence");
                    serde_json::to_string(
                        &json!({"reward":reward,"rationale":format!("Jev selected {reward}; confidence {confidence}. Jev supplies a typed decision, not a textual explanation."),"provider_response":v}),
                    )?
                }
                Protocol::Anthropic => {
                    ensure!(
                        v["stop_reason"] == "end_turn",
                        "model response incomplete or refused"
                    );
                    v["content"]
                        .as_array()
                        .context("missing model content")?
                        .iter()
                        .filter(|b| b["type"] == "text")
                        .filter_map(|b| b["text"].as_str())
                        .collect::<Vec<_>>()
                        .join("\n")
                }
            };
            ensure!(!output.trim().is_empty(), "empty model response");
            return Ok(output);
        }
        bail!("model retry budget exhausted")
    }
}
