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
    #[serde(default)]
    pub exclude_file: Option<String>,
    #[serde(skip)]
    cached_patterns: Option<Vec<String>>,
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
                if let Some(exclude_file) = file_config.exclude.exclude_file {
                    config.exclude.exclude_file = Some(exclude_file);
                }
            }
        } else {
            log::info!("No config.toml found, using default settings");
        }

        // Initialize exclude patterns after loading config
        config.exclude.initialize();

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
            log_file: Some("DSvClient.log".to_string()),
            term_level: Some("Info".to_string()),
            file_level: Some("Warn".to_string()),
        }
    }
}

impl Default for ExcludeConfig {
    fn default() -> Self {
        Self {
            patterns: Some(Vec::new()),
            exclude_file: None,
            cached_patterns: None,
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
    /// Initialize and cache all patterns from both config and exclude file
    pub fn initialize(&mut self) {
        if self.cached_patterns.is_none() {
            let mut all_patterns = Vec::new();
            
            // Add patterns from config
            if let Some(patterns) = &self.patterns {
                all_patterns.extend(patterns.clone());
            }
            
            // Load and add patterns from exclude file
            if let Some(exclude_file) = &self.exclude_file {
                match fs::read_to_string(exclude_file) {
                    Ok(content) => {
                        let file_patterns: Vec<String> = if exclude_file.ends_with(".json") {
                            // Try to parse as JSON
                            match serde_json::from_str::<serde_json::Value>(&content) {
                                Ok(json_value) => {
                                    if let Some(patterns_array) = json_value.get("exclusion_patterns").and_then(|v| v.as_array()) {
                                        log::info!("Loading JSON exclusion patterns from file: {}", exclude_file);
                                        patterns_array.iter()
                                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                            .collect()
                                    } else if let Some(detailed_errors) = json_value.get("detailed_errors").and_then(|v| v.as_array()) {
                                        log::info!("Loading JSON exclusion patterns from detailed_errors in file: {}", exclude_file);
                                        detailed_errors.iter()
                                            .filter_map(|v| v.get("url").and_then(|u| u.as_str()).map(|s| s.to_string()))
                                            .collect()
                                    } else {
                                        log::warn!("Exclusion file '{}' is JSON but missing 'exclusion_patterns' or 'detailed_errors' array", exclude_file);
                                        Vec::new()
                                    }
                                }
                                Err(_) => {
                                    // Fallback to line-by-line parsing if JSON is invalid
                                    log::warn!("Could not parse exclude file '{}' as JSON, falling back to line-by-line parsing", exclude_file);
                                    content.lines()
                                        .map(|line| line.trim())
                                        .filter(|line| !line.is_empty() && !line.starts_with('#'))
                                        .map(|line| line.to_string())
                                        .collect()
                                }
                            }
                        } else {
                            // Parse as plain text
                            log::info!("Loading plain text exclusion patterns from file: {}", exclude_file);
                            content.lines()
                                .map(|line| line.trim())
                                .filter(|line| !line.is_empty() && !line.starts_with('#'))
                                .map(|line| line.to_string())
                                .collect()
                        };
                        log::info!("Loaded {} patterns from {}", file_patterns.len(), exclude_file);
                        all_patterns.extend(file_patterns);
                    }
                    Err(e) => {
                        log::warn!("Failed to read exclude file '{}': {}", exclude_file, e);
                    }
                }
            }
            
            log::info!("Total exclusion patterns loaded: {}", all_patterns.len());
            self.cached_patterns = Some(all_patterns);
        }
    }

    /// Get all patterns (loads from cache after initialization)
    pub fn get_all_patterns(&self) -> &[String] {
        self.cached_patterns.as_ref().map(|v| v.as_slice()).unwrap_or(&[])
    }

    pub fn should_exclude(&self, url: &str) -> bool {
        let patterns = self.get_all_patterns();
        if patterns.is_empty() {
            log::debug!("No exclusion patterns loaded");
            return false;
        }
        
        let normalized_url = url.replace('\\', "/");
        for pattern in patterns {
            let normalized_pattern = pattern.replace('\\', "/");
            if normalized_url.contains(&normalized_pattern) {
                log::info!("URL excluded by pattern '{}': {}", pattern, url);
                return true;
            }
        }
        
        log::debug!("URL not excluded (checked against {} patterns): {}", patterns.len(), url);
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_exclude_file_functionality() {
        // Create a temporary exclude file for testing
        let test_content = "# Test exclusions\n.iso\n.vmdk\nvmw/vib20/tools-light\n/HPE/\ntest-\n";
        fs::write("test_exclusions.txt", test_content).expect("Failed to create test file");

        // Create config with both patterns and exclude_file
        let mut config = ExcludeConfig {
            patterns: Some(vec![
                "pattern1".to_string(),
                "pattern2".to_string(),
            ]),
            exclude_file: Some("test_exclusions.txt".to_string()),
            cached_patterns: None,
        };

        // Initialize the config to load patterns
        config.initialize();

        // Test get_all_patterns method
        let all_patterns = config.get_all_patterns();
        assert!(all_patterns.contains(&"pattern1".to_string()));
        assert!(all_patterns.contains(&"pattern2".to_string()));
        assert!(all_patterns.contains(&".iso".to_string()));
        assert!(all_patterns.contains(&"vmw/vib20/tools-light".to_string()));
        assert!(all_patterns.contains(&"/HPE/".to_string()));
        assert!(all_patterns.contains(&"test-".to_string()));

        // Test should_exclude method
        assert!(config.should_exclude("https://example.com/file.iso")); // from exclude file
        assert!(config.should_exclude("https://example.com/pattern1/file.xml")); // from config
        assert!(config.should_exclude("https://example.com/vmw/vib20/tools-light/file.vib")); // from exclude file
        assert!(config.should_exclude("https://example.com/HPE/driver.vib")); // from exclude file
        assert!(config.should_exclude("https://example.com/test-file.zip")); // from exclude file
        assert!(!config.should_exclude("https://example.com/normal/file.zip")); // should not be excluded

        // Clean up test file
        fs::remove_file("test_exclusions.txt").ok();
    }

    #[test]
    fn test_exclude_file_missing() {
        // Test with missing exclude file
        let mut config = ExcludeConfig {
            patterns: Some(vec!["pattern1".to_string()]),
            exclude_file: Some("nonexistent.txt".to_string()),
            cached_patterns: None,
        };

        config.initialize();
        let all_patterns = config.get_all_patterns();
        assert_eq!(all_patterns.len(), 1);
        assert!(all_patterns.contains(&"pattern1".to_string()));
    }

    #[test]
    fn test_exclude_file_only() {
        // Create a temporary exclude file for testing
        let test_content = r#"
# Test exclusions
.iso
vmw/vib20
"#;
        fs::write("test_file_only.txt", test_content).expect("Failed to create test file");

        // Test with only exclude_file, no patterns in config
        let mut config = ExcludeConfig {
            patterns: None,
            exclude_file: Some("test_file_only.txt".to_string()),
            cached_patterns: None,
        };

        config.initialize();
        let all_patterns = config.get_all_patterns();
        assert_eq!(all_patterns.len(), 2);
        assert!(all_patterns.contains(&".iso".to_string()));
        assert!(all_patterns.contains(&"vmw/vib20".to_string()));

        // Clean up test file
        fs::remove_file("test_file_only.txt").ok();
    }

    #[test]
    fn test_detailed_errors_json_format() {
        // Create a temporary exclude file for testing
        let test_content = r#"{
  "description": "Auto-generated exclusion patterns from 403 Forbidden errors",
  "detailed_errors": [
    {
      "target_path": "/home/vmware/ESX_HOST/main/esx/vmw/vib20/lsu-lsi-lsi-msgpt3-plugin/VMware_bootbank_lsu-lsi-lsi-msgpt3-plugin_1.0.0-9vmw.670.1.39.11675023.vib",
      "url": "https://dl.broadcom.com/{{ACCESS_KEY}}/PROD/COMP/ESX_HOST/main/esx/vmw/vib20/lsu-lsi-lsi-msgpt3-plugin/VMware_bootbank_lsu-lsi-lsi-msgpt3-plugin_1.0.0-9vmw.670.1.39.11675023.vib"
    },
    {
      "target_path": "/home/vmware/ESX_HOST/main/esx/vmw/vib20/lsu-hp-hpsa-plugin/VMware_bootbank_lsu-hp-hpsa-plugin_2.0.0-16vmw.670.1.28.10302608.vib",
      "url": "https://dl.broadcom.com/{{ACCESS_KEY}}/PROD/COMP/ESX_HOST/main/esx/vmw/vib20/lsu-hp-hpsa-plugin/VMware_bootbank_lsu-hp-hpsa-plugin_2.0.0-16vmw.670.1.28.10302608.vib"
    }
  ]
}"#;
        fs::write("test_detailed_errors.json", test_content).expect("Failed to create test file");

        // Test with only exclude_file, no patterns in config
        let mut config = ExcludeConfig {
            patterns: None,
            exclude_file: Some("test_detailed_errors.json".to_string()),
            cached_patterns: None,
        };

        config.initialize();
        let all_patterns = config.get_all_patterns();
        assert_eq!(all_patterns.len(), 2);
        assert!(all_patterns.contains(&"https://dl.broadcom.com/{{ACCESS_KEY}}/PROD/COMP/ESX_HOST/main/esx/vmw/vib20/lsu-lsi-lsi-msgpt3-plugin/VMware_bootbank_lsu-lsi-lsi-msgpt3-plugin_1.0.0-9vmw.670.1.39.11675023.vib".to_string()));
        assert!(all_patterns.contains(&"https://dl.broadcom.com/{{ACCESS_KEY}}/PROD/COMP/ESX_HOST/main/esx/vmw/vib20/lsu-hp-hpsa-plugin/VMware_bootbank_lsu-hp-hpsa-plugin_2.0.0-16vmw.670.1.28.10302608.vib".to_string()));

        // Clean up test file
        fs::remove_file("test_detailed_errors.json").ok();
    }

}

// Re-export at crate level
pub use self::Config as AppConfig;
