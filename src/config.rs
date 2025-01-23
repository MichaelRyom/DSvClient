use serde::Deserialize;
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
        // Add debug logging for config file loading
        log::debug!("Attempting to load config file...");
        
        // Try executable directory first
        if let Ok(mut exe_path) = std::env::current_exe() {
            exe_path.pop(); // Remove executable name
            exe_path.push("config.toml");
            log::debug!("Checking exe path: {}", exe_path.display());
            
            if let Ok(content) = fs::read_to_string(&exe_path) {
                log::info!("Loaded config from executable path: {}", exe_path.display());
                return Ok(toml::from_str(&content)?);
            }
        }
        
        // Try current working directory
        let cwd_path = std::env::current_dir()?.join("config.toml");
        log::debug!("Checking current directory: {}", cwd_path.display());
        
        match fs::read_to_string(&cwd_path) {
            Ok(content) => {
                log::info!("Loaded config from current directory: {}", cwd_path.display());
                Ok(toml::from_str(&content)?)
            }
            Err(e) => {
                log::warn!("Failed to load config.toml: {}", e);
                Err(anyhow::anyhow!("Failed to load config: {}", e))
            }
        }
    }

    pub fn load_or_default() -> Self {
        match Self::load() {
            Ok(config) => config,
            Err(e) => {
                log::warn!("Using default config: {}", e);
                Self::default()
            }
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            verification: VerificationConfig {
                chunk_size: 256 * 1024, //262144 = 256KB
                max_concurrent_files: 10,
                max_concurrent_verifications: num_cpus::get() * 4,
            },
            download: DownloadConfig {
                max_concurrent_downloads: 10,
                buffer_size: 8 * 1024 * 1024, //8388608 = 8MB
            },
            general: GeneralConfig {
                thread_sleep_ms: 0, // No sleep by default
            },
        }
    }
}

// Re-export at crate level
pub use self::Config as AppConfig;
