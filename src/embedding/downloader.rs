use std::fs;
use std::path::{Path, PathBuf};
use anyhow::{Context, Result};
use reqwest::Client;

pub struct ModelDownloader {
    cache_dir: PathBuf,
    client: Client,
}

impl ModelDownloader {
    pub fn new<P: AsRef<Path>>(cache_dir: P) -> Self {
        Self {
            cache_dir: cache_dir.as_ref().to_path_buf(),
            client: Client::new(),
        }
    }

    pub async fn download_if_missing(&self, url: &str, filename: &str) -> Result<PathBuf> {
        let dest = self.cache_dir.join(filename);
        if dest.exists() {
            return Ok(dest);
        }

        if !self.cache_dir.exists() {
            fs::create_dir_all(&self.cache_dir).context("Failed to create cache directory")?;
        }

        println!("Downloading {} to {:?}", url, dest);
        let bytes = self.client.get(url).send().await?.bytes().await?;
        fs::write(&dest, bytes).context("Failed to write to file")?;

        Ok(dest)
    }

    pub async fn get_bge_small(&self) -> Result<(PathBuf, PathBuf)> {
        let model_url = "https://huggingface.co/BAAI/bge-small-en-v1.5/resolve/main/onnx/model.onnx";
        let tokenizer_url = "https://huggingface.co/BAAI/bge-small-en-v1.5/resolve/main/tokenizer.json";

        let model_path = self.download_if_missing(model_url, "bge-small-en-v1.5.onnx").await?;
        let tokenizer_path = self.download_if_missing(tokenizer_url, "tokenizer.json").await?;

        Ok((model_path, tokenizer_path))
    }
}
