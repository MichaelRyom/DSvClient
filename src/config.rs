use serde::Deserialize;
use std::fs;
use anyhow::Result;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub verification: VerificationConfig,
    pub download: DownloadConfig,
    pub general: GeneralConfig,
    pub logging: LoggingConfig,  // Add logging config
    pub exclude: ExcludeConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct VerificationConfig {
    #[serde(default)]
    pub chunk_size: Option<usize>,
    #[serde(default)]
    pub max_concurrent_files: Option<usize>,
    #[serde(default)]
    pub max_concurrent_verifications: Option<usize>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DownloadConfig {
    #[serde(default)]
    pub max_concurrent_downloads: Option<usize>,
    #[serde(default)]
    pub buffer_size: Option<usize>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct GeneralConfig {
    #[serde(default)]
    pub thread_sleep_ms: Option<u64>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LoggingConfig {
    #[serde(default)]
    pub log_path: Option<String>,
    #[serde(default)]
    pub log_file: Option<String>,
    #[serde(default)]
    pub term_level: Option<String>,
    #[serde(default)]
    pub file_level: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ExcludeConfig {
    #[serde(default)]
    pub patterns: Option<Vec<String>>,
}

impl Config {
    pub fn load() -> Result<Self> {
        log::debug!("Attempting to load config file...");
        
        // Start with default configuration
        let mut config = Config::default();
        
        // Try to load and merge config file
        let config_content = if let Ok(mut exe_path) = std::env::current_exe() {
            exe_path.pop();
            exe_path.push("config.toml");
            log::debug!("Checking exe path: {}", exe_path.display());
            
            fs::read_to_string(&exe_path).or_else(|_| {
                let cwd_path = std::env::current_dir()?.join("config.toml");
                log::debug!("Checking current directory: {}", cwd_path.display());
                fs::read_to_string(&cwd_path)
            })
        } else {
            let cwd_path = std::env::current_dir()?.join("config.toml");
            fs::read_to_string(&cwd_path)
        };

        // Merge configuration if file exists
        if let Ok(content) = config_content {
            log::info!("Found config.toml, merging with defaults");
            if let Ok(file_config) = toml::from_str::<Config>(&content) {
                // Merge verification settings
                if let Some(chunk_size) = file_config.verification.chunk_size {
                    config.verification.chunk_size = Some(chunk_size);
                }
                if let Some(max_files) = file_config.verification.max_concurrent_files {
                    config.verification.max_concurrent_files = Some(max_files);
                }
                if let Some(max_verifications) = file_config.verification.max_concurrent_verifications {
                    config.verification.max_concurrent_verifications = Some(max_verifications);
                }

                // Merge download settings
                if let Some(max_downloads) = file_config.download.max_concurrent_downloads {
                    config.download.max_concurrent_downloads = Some(max_downloads);
                }
                if let Some(buffer_size) = file_config.download.buffer_size {
                    config.download.buffer_size = Some(buffer_size);
                }

                // Merge general settings
                if let Some(sleep_ms) = file_config.general.thread_sleep_ms {
                    config.general.thread_sleep_ms = Some(sleep_ms);
                }

                // Merge logging settings
                if let Some(log_path) = file_config.logging.log_path {
                    config.logging.log_path = Some(log_path);
                }
                if let Some(log_file) = file_config.logging.log_file {
                    config.logging.log_file = Some(log_file);
                }
                if let Some(term_level) = file_config.logging.term_level {
                    config.logging.term_level = Some(term_level);
                }
                if let Some(file_level) = file_config.logging.file_level {
                    config.logging.file_level = Some(file_level);
                }

                // Merge exclude settings
                if let Some(patterns) = file_config.exclude.patterns {
                    config.exclude.patterns = Some(patterns);
                }
            }
        } else {
            log::info!("No config.toml found, using default settings");
        }

        Ok(config)
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
            verification: VerificationConfig::default(),
            download: DownloadConfig::default(),
            general: GeneralConfig::default(),
            logging: LoggingConfig::default(),
            exclude: ExcludeConfig::default(),
        }
    }
}

impl Default for VerificationConfig {
    fn default() -> Self {
        Self {
            chunk_size: Some(256 * 1024), // 256KB
            max_concurrent_files: Some(10),
            max_concurrent_verifications: Some(num_cpus::get() * 4),
        }
    }
}

impl Default for DownloadConfig {
    fn default() -> Self {
        Self {
            max_concurrent_downloads: Some(10),
            buffer_size: Some(8 * 1024 * 1024), // 8MB
        }
    }
}

impl Default for GeneralConfig {
    fn default() -> Self {
        Self {
            thread_sleep_ms: Some(0),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            log_path: Some("logs".to_string()),
            log_file: Some("download_errors.log".to_string()),
            term_level: Some("Info".to_string()),
            file_level: Some("Warn".to_string()),
        }
    }
}

impl Default for ExcludeConfig {
    fn default() -> Self {
        Self {
            patterns: Some(Vec::new()),
        }
    }
}

// Use the default values from the Default implementations
impl VerificationConfig {
    pub fn chunk_size(&self) -> usize {
        self.chunk_size.unwrap_or_else(|| VerificationConfig::default().chunk_size.unwrap())
    }
    pub fn max_concurrent_files(&self) -> usize {
        self.max_concurrent_files.unwrap_or_else(|| VerificationConfig::default().max_concurrent_files.unwrap())
    }
    pub fn max_concurrent_verifications(&self) -> usize {
        self.max_concurrent_verifications.unwrap_or_else(|| VerificationConfig::default().max_concurrent_verifications.unwrap())
    }
}

impl DownloadConfig {
    pub fn max_concurrent_downloads(&self) -> usize {
        self.max_concurrent_downloads.unwrap_or_else(|| DownloadConfig::default().max_concurrent_downloads.unwrap())
    }
    pub fn buffer_size(&self) -> usize {
        self.buffer_size.unwrap_or_else(|| DownloadConfig::default().buffer_size.unwrap())
    }
}

impl GeneralConfig {
    pub fn thread_sleep_ms(&self) -> u64 {
        self.thread_sleep_ms.unwrap_or_else(|| GeneralConfig::default().thread_sleep_ms.unwrap())
    }
}

impl LoggingConfig {
    pub fn log_path(&self) -> String {
        let raw_path = self.log_path.clone()
            .unwrap_or_else(|| LoggingConfig::default().log_path.unwrap());
        
        // Convert the raw path to a proper PathBuf and back to normalize separators
        let path = std::path::PathBuf::from(raw_path);
        path.to_string_lossy().to_string()
    }

    pub fn log_file(&self) -> String {
        self.log_file.clone().unwrap_or_else(|| LoggingConfig::default().log_file.unwrap())
    }

    pub fn term_level(&self) -> log::LevelFilter {
        self.term_level
            .as_deref()
            .and_then(|l| l.parse().ok())
            .unwrap_or(log::LevelFilter::Info)
    }

    pub fn file_level(&self) -> log::LevelFilter {
        self.file_level
            .as_deref()
            .and_then(|l| l.parse().ok())
            .unwrap_or(log::LevelFilter::Warn)
    }
}

impl ExcludeConfig {
    pub fn should_exclude(&self, url: &str) -> bool {
        if let Some(patterns) = &self.patterns {
            let normalized_url = url.replace('\\', "/");
            patterns.iter().any(|pattern| {
                let normalized_pattern = pattern.replace('\\', "/");
                normalized_url.contains(&normalized_pattern)
            })
        } else {
            false
        }
    }
}

// Re-export at crate level
pub use self::Config as AppConfig;
