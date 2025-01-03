use serde::Deserialize;
use std::path::PathBuf;
use std::fs;
use anyhow::Result;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub verification: VerificationConfig,
    pub download: DownloadConfig,
    pub general: GeneralConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct VerificationConfig {
    pub chunk_size: usize,
    pub max_concurrent_files: usize,
    pub max_concurrent_verifications: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DownloadConfig {
    pub max_concurrent_downloads: usize,
    pub buffer_size: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct GeneralConfig {
    pub thread_sleep_ms: u64,
}

impl Config {
    pub fn load() -> Result<Self> {
        let content = fs::read_to_string("config.toml")?;
        Ok(toml::from_str(&content)?)
    }

    pub fn load_or_default() -> Self {
        Self::load().unwrap_or_else(|_| Self::default())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            verification: VerificationConfig {
                chunk_size: 256 * 1024,
                max_concurrent_files: 1000,
                max_concurrent_verifications: num_cpus::get() * 4,
            },
            download: DownloadConfig {
                max_concurrent_downloads: 10,
                buffer_size: 8 * 1024 * 1024,
            },
            general: GeneralConfig {
                thread_sleep_ms: 0,
            },
        }
    }
}

// Re-export at crate level
pub use self::Config as AppConfig;
