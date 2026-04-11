use crate::config::AppConfig;
use crate::parser::{Vendor, VcsaPackage, XmlParser};
use crate::process::{FileType, ProcessManager, Source};
use crate::verify::VerificationManager;
use anyhow::Result;
use bytes::Bytes;
use futures::future::join_all;
use http_body_util::{BodyExt, Empty};
use hyper_tls::HttpsConnector;
use hyper_util::client::legacy::Client;
use log::{debug, info, warn};
//use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore}; // Change to use tokio's Mutex // Use renamed import
use hyper::body::Body;
use std::time::{Duration, Instant};
use std::pin::Pin;
use std::task::{Context, Poll};
use bytes::BytesMut;
use futures::{Stream, StreamExt};
use url::Url;

//const MAX_CONCURRENT_DOWNLOADS: usize = 100;

type DownloadTracker = Arc<Mutex<HashSet<PathBuf>>>;
// Add a new type for tracking processed files
#[derive(Default)]
struct FileTracker {
    processed_paths: HashSet<String>,
    xml_files: HashSet<PathBuf>,
    zip_files: HashSet<PathBuf>,
    vib_files: HashSet<PathBuf>,
}

// Add timeout backoff tracking
#[derive(Debug, Clone)]
struct TimeoutTracker {
    timeout_count: usize,
    last_timeout: Option<std::time::Instant>,
    current_timeout: std::time::Duration,
    max_timeout: std::time::Duration,
}

impl Default for TimeoutTracker {
    fn default() -> Self {
        Self {
            timeout_count: 0,
            last_timeout: None,
            current_timeout: std::time::Duration::from_secs(30), // Base timeout
            max_timeout: std::time::Duration::from_secs(300),    // Max 5 minutes
        }
    }
}

impl TimeoutTracker {
    fn record_timeout(&mut self) {
        self.timeout_count += 1;
        self.last_timeout = Some(std::time::Instant::now());
        
        // Exponential backoff: double the timeout up to max
        self.current_timeout = std::cmp::min(
            self.current_timeout * 2,
            self.max_timeout
        );
        
        warn!("Timeout #{}, increasing timeout to {:?}", self.timeout_count, self.current_timeout);
    }
    
    fn should_reduce_timeout(&self) -> bool {
        if let Some(last) = self.last_timeout {
            // Reduce timeout if it's been 5 minutes since last timeout
            last.elapsed() > std::time::Duration::from_secs(300)
        } else {
            false
        }
    }
    
    fn maybe_reduce_timeout(&mut self) {
        if self.should_reduce_timeout() && self.current_timeout > std::time::Duration::from_secs(30) {
            self.current_timeout = std::cmp::max(
                self.current_timeout / 2,
                std::time::Duration::from_secs(30)
            );
            info!("Reducing timeout to {:?} after period of stability", self.current_timeout);
        }
    }
    
    fn get_current_timeout(&self) -> std::time::Duration {
        self.current_timeout
    }
}

type ProcessedFiles = Arc<Mutex<FileTracker>>;

#[derive(Debug, Default, Clone)]
pub struct DownloadReport {
    pub files_downloaded: usize,
    pub files_skipped: usize,
    pub processed_files: usize,
    pub processed_xmls: HashSet<PathBuf>,
    pub processed_zips: HashSet<PathBuf>,
    pub downloaded_vibs: HashSet<PathBuf>,
    pub checksum_mismatches: Vec<(PathBuf, String, String)>, // (path, expected, actual)
    pub access_errors: Vec<(PathBuf, String)>, // General access errors (non-403/404)
    pub not_entitled: Vec<(String, PathBuf, String)>, // 403 Forbidden errors (URL, path, error_message)
    pub not_found: Vec<(String, PathBuf, String)>, // 404 Not Found errors (URL, path, error_message)
    pub timeout_errors: Vec<(String, PathBuf)>, // Download timeouts (URL, path)
    pub files_missing: Vec<PathBuf>, // Add new field for missing files
    pub total_xml_processed: usize,  // Add counter for total XML files found - So compiling works for now, remove later
    pub xml_processed: HashSet<String>,  // Changed from usize to HashSet<String>
    pub total_zip_processed: usize,  // Add counter for total ZIP files found - So compiling works for now, remove later
    pub zip_processed: HashSet<String>,  // Changed from usize to HashSet<String>
    pub total_vib_processed: usize,  // Add counter for total VIB files found
    pub retry_attempts: usize,
    pub retry_successes: usize,
}

impl DownloadReport {
    pub fn print_summary(&self) {
        info!("\nDownload Summary:");

        if !self.checksum_mismatches.is_empty() {
            warn!(
                "\nChecksum Mismatches ({}):",
                self.checksum_mismatches.len()
            );
            for (path, expected, actual) in &self.checksum_mismatches {
                warn!(
                    "  {}: expected {} but got {}",
                    path.display(),
                    expected,
                    actual
                );
            }
        }

        if !self.access_errors.is_empty() {
            warn!("\nAccess Errors ({}):", self.access_errors.len());
            for (path, error) in &self.access_errors {
                warn!("  {}: {}", path.display(), error);
            }
        }

        if !self.not_entitled.is_empty() {
            warn!("\nNot Entitled - Access Denied ({}):", self.not_entitled.len());
            for (url, path, error_msg) in &self.not_entitled {
                warn!("  {} -> {} (Error: {})", url, path.display(), error_msg);
            }
        }

        if !self.not_found.is_empty() {
            warn!("\nNot Found - Files Not Available ({}):", self.not_found.len());
            for (url, path, error_msg) in &self.not_found {
                warn!("  {} -> {} (Error: {})", url, path.display(), error_msg);
            }
        }

        if !self.timeout_errors.is_empty() {
            warn!("\nTimeout Errors ({}):", self.timeout_errors.len());
            for (url, path) in &self.timeout_errors {
                warn!("  {} -> {}", url, path.display());
            }
        }

        if !self.files_missing.is_empty() {
            warn!("\nMissing Files - General Download Failures ({}):", self.files_missing.len());
            for path in &self.files_missing {
                warn!("  {}", path.display());
            }
        }

        // Update these lines to use total counters instead of HashSet lengths
        info!("XML files processed: {}", self.xml_processed.len());
        info!("ZIP files processed: {}", self.zip_processed.len());
        info!("Files checked: {}", self.processed_files);
        info!("Files skipped (no issues with): {}", self.files_skipped);
        info!("Files successfully downloaded: {}", self.files_downloaded);
        
        if self.retry_attempts > 0 {
            info!("Retry statistics: {} attempts, {} successes ({:.1}% success rate)", 
                self.retry_attempts, self.retry_successes, 
                (self.retry_successes as f32 / self.retry_attempts as f32) * 100.0);
        }
        
        info!("Errors:");
        info!("  - Access errors: {}", self.access_errors.len());
        info!("  - Not entitled (403 Forbidden): {}", self.not_entitled.len());
        info!("  - Not found (404 Not Found): {}", self.not_found.len());
        info!("  - Timeout errors: {}", self.timeout_errors.len());
        info!("  - Other download failures: {}", self.files_missing.len());
    }
}

#[derive(Clone)]
pub struct Downloader {
    base_path: PathBuf,
    client:
        Client<HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>, Empty<Bytes>>,
    downloaded: DownloadTracker,
    failed: Arc<Mutex<HashMap<String, (PathBuf, Option<(String, String)>)>>>,
    processor: Arc<ProcessManager>,
    verifier: Arc<VerificationManager>,
    xml_parser: Arc<XmlParser>,
    download_semaphore: Arc<Semaphore>,
    processed_files: ProcessedFiles,
    timeout_tracker: Arc<Mutex<TimeoutTracker>>, // Add timeout tracking
    //config: Arc<AppConfig>, // Use AppConfig instead of Config
    download_report: Arc<Mutex<DownloadReport>>, // Add this field
}

#[derive(Debug, Deserialize)]
struct SourceConfig {
    #[serde(rename = "downloadToken")]
    global_download_token: Option<String>,
    sources: Vec<SourceEntry>,
}

#[derive(Debug, Deserialize)]
struct SourceEntry {
    url: String,
    enabled: bool,
    status: String,
    r#type: String,
    version: Option<String>,
    files: Option<Vec<String>>,
    #[serde(rename = "downloadToken")]
    download_token: Option<String>,
    // OAuth-specific fields
    client_id: Option<String>,
    client_secret: Option<String>,
    auth_url: Option<String>,
    output_filename: Option<String>,
}

impl Downloader {
    pub fn new(
        base_path: PathBuf,
        client: Client<
            HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
            Empty<Bytes>,
        >,
        config: AppConfig,
    ) -> Self {
        Self {
            base_path: base_path.clone(),
            client: client.clone(),
            downloaded: Arc::new(Mutex::new(HashSet::new())),
            failed: Arc::new(Mutex::new(HashMap::new())),
            processor: Arc::new(ProcessManager::new(client.clone(), base_path.clone())),
            verifier: Arc::new(VerificationManager::new(config.clone())), // Pass config to VerificationManager
            xml_parser: Arc::new(XmlParser::new()),
            download_semaphore: Arc::new(Semaphore::new(config.download.max_concurrent_downloads())),
            processed_files: Arc::new(Mutex::new(FileTracker::default())),
            timeout_tracker: Arc::new(Mutex::new(TimeoutTracker::default())), // Initialize timeout tracker
            //config: Arc::new(config),
            download_report: Arc::new(Mutex::new(DownloadReport::default())), // Initialize the field
        }
    }

    // Add helper method to check if file was processed
    async fn is_file_processed(&self, relative_path: &str) -> bool {
        let tracker = self.processed_files.lock().await;
        tracker.processed_paths.contains(relative_path)
    }

    // Add helper method to mark file as processed
    async fn mark_file_processed(&self, relative_path: String) {
        let mut tracker = self.processed_files.lock().await;
        if !tracker.processed_paths.contains(&relative_path) {
            tracker.processed_paths.insert(relative_path.clone());
            let target_path = self.base_path.join(&relative_path);
            
            // Track by file type
            if relative_path.ends_with(".xml") {
                tracker.xml_files.insert(target_path);
            } else if relative_path.ends_with(".zip") {
                tracker.zip_files.insert(target_path);
            } else if relative_path.ends_with(".vib") {
                tracker.vib_files.insert(target_path);
            }
        }
    }

    pub async fn process_repository(&self, url: &str) -> Result<()> {
        info!("Processing repository: {}", url);
        
        // Count main XML file
        {
            let mut report = self.download_report.lock().await;
            report.xml_processed.insert(url.to_string());  // Store the URL instead of incrementing
        }


        // Get base URL without filename
        let base_url = url.rsplit_once('/').map(|(base, _)| base).unwrap_or(url);

        // Download and parse the main index XML
        match self.download_xml(url).await {
            Ok(index_content) => {
                // Try to process as a vendor list first (like addon-main)
                if let Ok(vendors) = self.xml_parser.parse_vendor_list(&index_content) {
                    // Count each vendor XML
                    /*{
                        let mut report = self.download_report.lock().await;
                        report.total_xml_processed += 1;
                    }*/
                    // Process vendors concurrently
                    let mut vendor_tasks = Vec::new();
                    for vendor in vendors {
                        let vendor_url =
                            format!("{}/{}/{}", base_url, vendor.relative_path, vendor.indexfile);
                        let this = self.clone();
                        vendor_tasks.push(tokio::spawn(async move {
                            this.process_vendor(&vendor_url, &vendor).await
                        }));
                    }

                    // Wait for all vendor tasks
                    for result in join_all(vendor_tasks).await {
                        if let Err(e) = result? {
                            warn!("Vendor processing error: {}", e);
                        }
                    }
                } else {
                    // If not a vendor list, try direct metadata processing
                    let this = self.clone();
                    this.process_metadata(url).await?;
                }
            }
            Err(e) => {
                warn!("Failed to download or parse index XML from {}: {}", url, e);
                return Err(e);
            }
        }

        Ok(())
    }

    async fn process_vendor(&self, vendor_url: &str, _vendor: &Vendor) -> Result<()> {
        // Get base URL without the XML filename
        let base_url = vendor_url
            .rsplit_once('/')
            .map(|(base, _)| base)
            .unwrap_or(vendor_url);

        match self.download_xml(vendor_url).await {
            Ok(vendor_content) => {
                if let Ok(metadata_list) = self.xml_parser.parse_metadata_list(&vendor_content) {
                    info!("Processing vendor XML: {}", vendor_url);

                    // Count each vendor XML
                    {
                        let mut report = self.download_report.lock().await;
                        report.xml_processed.insert(vendor_url.to_string());  // Store the URL instead of incrementing
                    }

                    // Process all metadata concurrently with rate limiting
                    let mut metadata_tasks = Vec::new();
                    for metadata in metadata_list {
                        let metadata_url = format!("{}/{}", base_url, metadata.url);
                        let this = self.clone();
                        let permit = self.download_semaphore.clone().acquire_owned().await?;
                        metadata_tasks.push(tokio::spawn(async move {
                            // The permit is held for the duration of this task and released when dropped
                            let _permit = permit;
                            if let Err(e) = this.process_metadata(&metadata_url).await {
                                warn!("Error processing metadata {}: {}", metadata_url, e);
                            }
                        }));
                    }
                    join_all(metadata_tasks).await;
                }
            }
            Err(e) => {
                warn!(
                    "Failed to download or parse vendor XML from {}: {}",
                    vendor_url, e
                );
                return Err(e);
            }
        }
        Ok(())
    }

    pub async fn process_sources(&self) -> Result<()> {
        info!("Starting process_sources...");
        let (sources, global_token) = self.read_sources_file().await?;
        info!("Found {} sources to process", sources.len());

        let mut tasks = Vec::new();
        for source in sources {
            if !source.enabled {
                continue;
            }

            let this = self.clone();
            let url = source.url.clone();
            // Use local token if available, otherwise fall back to global token
            let effective_token = source.download_token.clone().or_else(|| global_token.clone());
            
            tasks.push(tokio::spawn(async move {
                match source.r#type.as_str() {
                    "Host" => {
                        // Replace download token in URL if present
                        let processed_url = this.replace_download_token(&url, effective_token.as_deref());
                        info!("Processing Host repository: {}", processed_url);
                        if let Err(e) = this.process_repository(&processed_url).await {
                            warn!("Error processing Host repository {}: {}", processed_url, e);
                        }
                    }
                    "File" => {
                        // Replace download token in URL if present
                        let processed_url = this.replace_download_token(&url, effective_token.as_deref());
                        info!("Processing File download: {}", processed_url);
                        let filename = this.sanitize_filename(&processed_url);
                        let target_path = this.base_path.join(&filename);
                        if let Err(e) = this.download_file(&processed_url, &target_path).await {
                            warn!("Error downloading File {}: {}", processed_url, e);
                        } else {
                            info!("Successfully downloaded File to: {}", target_path.display());
                        }
                    }
                    "VCSA" => {
                        if let (Some(version), Some(files)) = (source.version, source.files) {
                            // Replace download token in base URL if present
                            let processed_base_url = this.replace_download_token(&url, effective_token.as_deref());
                            info!("Processing VCSA source: {}", processed_base_url);
                            
                            // Download all specified files first
                            for file in files.clone() { // Clone to avoid borrowing issues
                                let full_url = this.process_vcsa_url(&processed_base_url, &version, &file);
                                info!("Downloading VCSA file: {}", full_url);
                                
                                let target_path = this.get_vcsa_file_path(&version, &file);
                                if let Err(e) = this.download_file(&full_url, &target_path).await {
                                    warn!("Error downloading {}: {}", full_url, e);
                                    continue;
                                }

                                // If this is the manifest, process it to get additional files
                                if file.ends_with("manifest-latest.xml") {
                                    match tokio::fs::read_to_string(&target_path).await {
                                        Ok(manifest_content) => {
                                            // Process manifest to get package files
                                            if let Err(e) = this.process_vcsa_manifest(&manifest_content, &version, &processed_base_url).await {
                                                warn!("Error processing manifest: {}", e);
                                            } else {
                                                info!("Successfully processed VCSA manifest");
                                            }
                                        }
                                        Err(e) => warn!("Error reading manifest file: {}", e),
                                    }
                                }

                                // rpm-manifest.json lists additional files that are NOT referenced in
                                // manifest-latest.xml - notably container image blobs (.blob) and
                                // container manifests (.manifest). Without these, `software-packages
                                // stage --iso` fails on the VCSA because the stage step can't find
                                // the container layers referenced by the patch metadata.
                                if file.ends_with("rpm-manifest.json") {
                                    match tokio::fs::read_to_string(&target_path).await {
                                        Ok(json_content) => {
                                            if let Err(e) = this.process_vcsa_rpm_manifest_json(&json_content, &version, &processed_base_url).await {
                                                warn!("Error processing rpm-manifest.json: {}", e);
                                            } else {
                                                info!("Successfully processed VCSA rpm-manifest.json");
                                            }
                                        }
                                        Err(e) => warn!("Error reading rpm-manifest.json file: {}", e),
                                    }
                                }
                            }
                        } else {
                            warn!("VCSA source missing version or files: {}", url);
                        }
                    }
                    "OAuth" => {
                        if let (Some(client_id), Some(client_secret), Some(auth_url)) = 
                            (&source.client_id, &source.client_secret, &source.auth_url) {
                            
                            info!("Processing OAuth source: {}", url);
                            
                            // Get OAuth access token
                            match this.get_oauth_token(auth_url, client_id, client_secret).await {
                                Ok(access_token) => {
                                    // Determine output filename
                                    let output_filename = source.output_filename
                                        .as_deref()
                                        .unwrap_or("vvs_data.gz");
                                    
                                    let target_path = this.base_path.join(output_filename);
                                    
                                    // Download with OAuth token
                                    match this.download_with_oauth(&url, &access_token, &target_path).await {
                                        Ok(_) => {
                                            info!("Successfully downloaded OAuth file to: {}", target_path.display());
                                        }
                                        Err(e) => {
                                            warn!("Error downloading OAuth file {}: {}", url, e);
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!("Failed to get OAuth token for {}: {}", url, e);
                                }
                            }
                        } else {
                            warn!("OAuth source missing required fields (client_id, client_secret, auth_url): {}", url);
                        }
                    }
                    _ => warn!("Unsupported source type: {}", source.r#type),
                }
            }));
        }

        info!("Waiting for {} source processing tasks to complete...", tasks.len());
        let results = join_all(tasks).await;
        
        let mut successful = 0;
        let mut failed = 0;
        for (i, result) in results.iter().enumerate() {
            match result {
                Ok(_) => successful += 1,
                Err(e) => {
                    failed += 1;
                    warn!("Source task {} failed: {}", i, e);
                }
            }
        }
        
        info!("Source processing completed: {} successful, {} failed", successful, failed);
        Ok(())
    }

    async fn read_sources_file(&self) -> Result<(Vec<SourceEntry>, Option<String>)> {
        let sources_file = PathBuf::from("sources.toml");
        if !sources_file.exists() {
            return Err(anyhow::anyhow!("sources.toml file not found"));
        }

        let content = tokio::fs::read_to_string(sources_file).await?;
        let config: SourceConfig = toml::from_str(&content)?;

        if config.sources.is_empty() {
            warn!("No valid sources found in sources.toml");
        }

        Ok((config.sources, config.global_download_token))
    }

/*     pub async fn process_sources_file(&self) -> Result<()> {
        let sources_file = PathBuf::from("sources"); // Changed to look in current dir
        if (!sources_file.exists()) {
            warn!("Sources file not found: {}", sources_file.display());
            return Ok(());
        }
        let content = tokio::fs::read_to_string(sources_file).await?;
        for line in content.lines() {
            let url = line.trim();
            if url.starts_with("\"") {
                // Skip CSV header and handle quoted URLs
                continue;
            }
            if url.is_empty() {
                continue;
            }
            self.process_repository(url).await?;
        }
        Ok(())
    }
 */
    fn extract_relative_path(&self, url: &str) -> String {
        // Find the index after "VUM/PRODUCTION/"
        if let Some(relative_idx) = url
            .find("VUM/PRODUCTION/")
            .map(|i| i + "VUM/PRODUCTION/".len())
        {
            self.sanitize_path(&url[relative_idx..])
        } else {
            // For non-VUM URLs (like Broadcom), extract meaningful path components
            if let Ok(parsed_url) = url::Url::parse(url) {
                let path = parsed_url.path().trim_start_matches('/');
                
                // Extract meaningful parts from the path
                let meaningful_path = self.extract_meaningful_path(path);
                self.sanitize_path(&meaningful_path)
            } else {
                // Fallback: sanitize the entire URL as a path
                self.sanitize_path(url)
            }
        }
    }
    
    fn extract_meaningful_path(&self, path: &str) -> String {
        let path_segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        
        // Look for Broadcom-specific patterns first
        for (i, segment) in path_segments.iter().enumerate() {
            // Look for Broadcom path structure: /PROD/COMP/ESX_HOST/ or /PROD/COMP/VCENTER/
            if *segment == "ESX_HOST" {
                // Skip ESX_HOST and start from the next segment
                if i + 1 < path_segments.len() {
                    return path_segments[(i + 1)..].join("/");
                } else {
                    // If ESX_HOST is the last segment, return empty or fallback
                    return String::new();
                }
            } else if *segment == "VCENTER" {
                // Start from VCENTER and include everything after (keep VCENTER in path)
                return path_segments[i..].join("/");
            }
            
            // Look for VMware/ESX related segments
            if segment.contains("vmtools") || segment.contains("vib") {
                // Start from this segment or a few segments before if they seem meaningful
                let start_idx = if i > 0 && path_segments[i-1].len() > 3 && !path_segments[i-1].chars().all(|c| c.is_uppercase() || c.is_numeric()) {
                    i - 1
                } else {
                    i
                };
                return path_segments[start_idx..].join("/");
            }
            
            // Look for addon patterns (after ESX_HOST/VCENTER check)
            if segment.ends_with("-main") || *segment == "addon" || *segment == "main" || *segment == "iovp" {
                // For Broadcom URLs, check if the parent is ESX_HOST or VCENTER
                let start_idx = if i > 0 && path_segments[i-1] == "ESX_HOST" {
                    // Skip ESX_HOST, start from current segment
                    i
                } else if i > 0 && path_segments[i-1] == "VCENTER" {
                    // Include VCENTER in the path
                    i - 1
                } else {
                    i
                };
                return path_segments[start_idx..].join("/");
            }
            
            // Look for common VMware patterns
            if segment.contains("driver") || segment.contains("patch") {
                return path_segments[i..].join("/");
            }
        }
        
        // For Broadcom URLs, look for meaningful segments after filtering generic ones
        if path_segments.len() > 3 {
            // Skip generic segments like PROD, COMP, domain names, tokens, and take meaningful ones
            let mut meaningful_segments = Vec::new();
            let mut found_meaningful = false;
            let mut skip_next = false;
            
            for (_i, segment) in path_segments.iter().enumerate() {
                if skip_next {
                    skip_next = false;
                    continue;
                }
                
                let upper_segment = segment.to_uppercase();
                
                // Skip common generic segments including ESX_HOST
                if matches!(upper_segment.as_str(), "PROD" | "COMP" | "SOFTWARE" | "VUM" | "PRODUCTION" | "ESX_HOST") 
                    || segment.starts_with("dl.") 
                    || segment.contains(".com") 
                    || segment.len() > 20 // Likely a token
                {
                    continue;
                }
                
                // Skip protocol scheme
                if *segment == "https:" {
                    skip_next = true; // Also skip the empty segment after ://
                    continue;
                }
                
                // Skip very short segments at the beginning unless we've found meaningful content
                if segment.len() < 3 && !found_meaningful {
                    continue;
                }
                
                found_meaningful = true;
                meaningful_segments.push(*segment);
            }
            
            if !meaningful_segments.is_empty() {
                return meaningful_segments.join("/");
            }
        }
        
        // Fallback: return the last 2-3 meaningful segments
        let meaningful_count = std::cmp::min(3, path_segments.len());
        let start_idx = path_segments.len().saturating_sub(meaningful_count);
        path_segments[start_idx..].join("/")
    }
    
    fn sanitize_path(&self, path: &str) -> String {
        // Replace invalid path characters with underscores
        let invalid_chars = ['<', '>', ':', '"', '|', '?', '*'];
        let mut result = String::new();
        
        for c in path.chars() {
            if invalid_chars.contains(&c) {
                result.push('_');
            } else if c == '\\' {
                result.push('/'); // Normalize backslashes to forward slashes
            } else {
                result.push(c);
            }
        }
        result
    }
    
    fn sanitize_path_component(&self, component: &str) -> String {
        // Sanitize a single path component (like hostname)
        let invalid_chars = ['<', '>', ':', '"', '/', '\\', '|', '?', '*'];
        let mut result = String::new();
        
        for c in component.chars() {
            if invalid_chars.contains(&c) {
                result.push('_');
            } else {
                result.push(c);
            }
        }
        result
    }

    async fn process_metadata(&self, url: &str) -> Result<()> {
        // Check exclude patterns first
        if self.verifier.config.exclude.should_exclude(url) {
            debug!("Skipping excluded URL: {}", url);
            return Ok(());
        }
        info!("Processing metadata from URL: {}", url);
        let relative_base = self.extract_relative_path(url);

        let source = Source::Http(url.to_string());
        let files = self.processor.process_source(source).await?;
        info!("Found {} files to process", files.len());

        // Count and track files before any processing
        {
            let mut report = self.download_report.lock().await;
            let meta_path = self.base_path.join(&relative_base);
            report.zip_processed.insert(url.to_string());  // Store URL instead of incrementing
            report.processed_zips.insert(meta_path);
            for _file in &files {
                report.total_vib_processed += 1;
            }
        }

        // Use a timeout for the entire batch of tasks
        let timeout_duration = std::time::Duration::from_secs(300); // 5 minute timeout
        
        // Process files concurrently with timeouts
        let mut tasks = Vec::new();
        let max_concurrent = self.verifier.config.verification.max_concurrent_files(); // Use verification config
        let file_semaphore = Arc::new(Semaphore::new(max_concurrent));

        for file in files {
            // Check exclusion patterns first, before any processing
            if let Source::Http(url) = &file.source {
                if self.verifier.config.exclude.should_exclude(url) {
                    debug!("Skipping excluded file: {}", url);
                    continue;
                }
            }

            let full_relative_path = if file.relative_path.starts_with("http") {
                self.extract_relative_path(&file.relative_path)
            } else {
                let base_dir = Path::new(&relative_base)
                    .parent()
                    .unwrap_or_else(|| Path::new(""))
                    .join(&file.relative_path);
                base_dir.to_string_lossy().into_owned()
            };

            // Skip if already processed
            if self.is_file_processed(&full_relative_path).await {
                debug!("Skipping already processed file: {}", full_relative_path);
                continue;
            }

            // Mark as processed before starting work
            self.mark_file_processed(full_relative_path.clone()).await;

            // Create target path
            let target_path = self.base_path.join(&full_relative_path);
            info!("Target path: {}", target_path.display());

            // Treat discovered files as VIB
            let mut report = self.download_report.lock().await;
            report.downloaded_vibs.insert(target_path.clone());
/*                     if !target_path.exists() {
                        report.files_downloaded += 1;
                    } */
            drop(report);

            match file.file_type {
                FileType::Vib => {
                    // Track VIB file immediately when first encountered
                    let mut report = self.download_report.lock().await;
                    report.downloaded_vibs.insert(target_path.clone());
/*                     if !target_path.exists() {
                        report.files_downloaded += 1;
                    } */
                    drop(report);

                    let this = self.clone();
                    let permit = file_semaphore.clone().acquire_owned().await?; // Use verification semaphore for VIB processing
                    let file_clone = file.clone();
                    let target_path_clone = target_path.clone();

                    tasks.push(tokio::spawn(async move {
                        let _permit_guard = permit;
                        
                        match tokio::time::timeout(timeout_duration, async {
                            // Always verify existing file if checksum is available
                            let needs_download = if target_path_clone.exists() {
                                if let (Some(checksum), Some(checksum_type)) = 
                                    (&file_clone.checksum, &file_clone.checksum_type) 
                                {
                                    match this.verifier
                                        .verify_checksum(&target_path_clone, checksum, checksum_type)
                                        .await 
                                    {
                                        Ok(true) => false, // Checksum matches, no download needed
                                        Ok(false) => {
                                            info!("Checksum mismatch for {}, will redownload", 
                                                target_path_clone.display());
                                            this.add_checksum_mismatch(
                                                &target_path_clone,
                                                checksum,
                                                "failed_verification"
                                            ).await;
                                            // Delete invalid file
                                            if let Err(e) = tokio::fs::remove_file(&target_path_clone).await {
                                                warn!("Failed to delete invalid file: {}", e);
                                                this.add_access_error(&target_path_clone, e.to_string()).await;
                                            }
                                            true
                                        }
                                        Err(e) => {
                                            warn!("Checksum verification failed: {}", e);
                                            this.add_access_error(&target_path_clone, e.to_string()).await;
                                            if e.to_string().contains("Access is denied") {
                                                false // Don't try to redownload if we don't have access
                                            } else {
                                                true
                                            }
                                        }
                                    }
                                } else {
                                    false // No checksum available, keep existing file
                                }
                            } else {
                                true // File doesn't exist, needs download
                            };

                            if needs_download {
                                if let Source::Http(url) = &file_clone.source {
                                    // Download with timeout
                                    match tokio::time::timeout(
                                        std::time::Duration::from_secs(60), // Increased timeout
                                        this.download_file(url, &target_path_clone)
                                    ).await {
                                        Ok(result) => {
                                            match result {
                                                Ok(_) => {
                                                    // Verify downloaded file
                                                    if let (Some(checksum), Some(checksum_type)) = 
                                                        (&file_clone.checksum, &file_clone.checksum_type) 
                                                    {
                                                        match this.verifier
                                                            .verify_checksum(&target_path_clone, checksum, checksum_type)
                                                            .await 
                                                        {
                                                            Ok(true) => Ok(()),
                                                            Ok(false) => {
                                                                // Delete invalid download
                                                                let _ = tokio::fs::remove_file(&target_path_clone).await;
                                                                this.add_checksum_mismatch(
                                                                    &target_path_clone,
                                                                    checksum,
                                                                    checksum_type
                                                                ).await;
                                                                Err(anyhow::anyhow!("Checksum verification failed after download"))
                                                            }
                                                            Err(e) => Err(e)
                                                        }
                                                    } else {
                                                        Ok(())
                                                    }
                                                }
                                                Err(e) => Err(e)
                                            }
                                        }
                                        Err(_) => {
                                            warn!("Download timeout for {}", url);
                                            this.add_failed_download(
                                                url.clone(),
                                                target_path_clone.clone(),
                                                None
                                            ).await;
                                            Err(anyhow::anyhow!("Download timeout"))
                                        }
                                    }
                                } else {
                                    Ok(())
                                }
                            } else {
                                Ok(())
                            }
                        }).await {
                            Ok(result) => result,
                            Err(e) => {
                                warn!("Task timeout for {}", target_path_clone.display());
                                this.add_access_error(&target_path_clone, format!("Task timeout: {}", e)).await;
                                Err(anyhow::anyhow!("Task timeout"))
                            }
                        }
                    }));

                    if let (Some(checksum), Some(checksum_type)) = (&file.checksum, &file.checksum_type) {
                        if let Source::Path(vib_path) = &file.source {
                            // Cache metadata during download
                            self.verifier.cache_vib_info(
                                vib_path.clone(),
                                checksum.clone(),
                                checksum_type.clone()
                            ).await;
                        }
                    }
                }
                _ => debug!("Skipping unknown file type: {}", file.relative_path),
            }
        }

        // Wait for all tasks to complete with progress reporting
        let total_tasks = tasks.len();
        info!("Waiting for {} file processing tasks to complete...", total_tasks);
        
        let results = join_all(tasks).await;
        let mut completed = 0;
        let mut errors = 0;
        
        for result in results {
            match result {
                Ok(Ok(())) => completed += 1,
                Ok(Err(e)) => {
                    errors += 1;
                    warn!("Task error: {}", e);
                },
                Err(e) => {
                    errors += 1;
                    warn!("Task join error: {}", e);
                },
            }
        }
        
        info!("File processing completed: {}/{} successful, {} errors", 
              completed, total_tasks, errors);

        Ok(())
    }

/*     // Add new helper method to handle download and verification
    async fn download_file_with_verify(&self, relative_path: &str) -> Result<()> {
        let target_path = self.base_path.join(relative_path);
        if (!target_path.exists()) {
            let url = format!(
                "https://hostupdate.vmware.com/software/VUM/PRODUCTION/{}",
                relative_path
            );
            self.download_file(&url, &target_path).await?;
        }
        Ok(())
    }
 */
    async fn download_xml(&self, url: &str) -> Result<String> {
        // Create the target path for the XML file
        let relative_path = self.extract_relative_path(url);
        let target_path = self.base_path.join(&relative_path);

        /*// Check if XML exists and try to read it first
        if target_path.exists() {
            match tokio::fs::read_to_string(&target_path).await {
                Ok(content) => {
                    debug!("Using cached XML file: {}", target_path.display());
                    return Ok(content);
                }
                Err(e) => warn!("Failed to read cached XML {}: {}", target_path.display(), e),
            }
        }*/

        // Download if not cached or cache read failed
        info!("Downloading XML: {}", url);
        let response = self.client.get(url.parse()?).await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!("HTTP error {}: {}", response.status(), url));
        }

        let bytes = http_body_util::BodyExt::collect(response.into_body())
            .await?
            .to_bytes();
        let content = String::from_utf8(bytes.to_vec())?;

        // Save the XML file
        if let Some(parent) = target_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&target_path, &content).await?;

        // Update stats when saving XML file
        if tokio::fs::write(&target_path, &content).await.is_ok() {
            let mut report = self.download_report.lock().await;
            report.processed_xmls.insert(target_path.clone());
            report.processed_files += 1;
        }

        // Update download stats for XML
        self.update_download_stats(&target_path).await;

        info!("Saved XML file: {}", target_path.display());

        Ok(content)
    }

    async fn download_file(&self, url: &str, target_path: &Path) -> Result<()> {
        // Check exclude patterns first
        if self.verifier.config.exclude.should_exclude(url) {
            debug!("Skipping excluded URL: {}", url);
            return Ok(());
        }
        // Skip if file exists and is tracked
        let relative_path = target_path.strip_prefix(&self.base_path).map_or_else(
            |_| target_path.to_string_lossy().to_string(),
            |p| p.to_string_lossy().to_string(),
        );

        if self.is_file_processed(&relative_path).await && target_path.exists() {
            debug!(
                "Skipping already downloaded file: {}",
                target_path.display()
            );
            return Ok(());
        }

        let download_start = std::time::Instant::now();
        
        // Use shorter initial timeout for connection establishment
        let connect_timeout = std::time::Duration::from_secs(30);
        let response = match tokio::time::timeout(connect_timeout, self.client.get(url.parse()?)).await {
            Ok(Ok(response)) => response,
            Ok(Err(e)) => {
                warn!("HTTP request failed for {}: {}", url, e);
                self.add_failed_download(url.to_string(), target_path.to_path_buf(), None).await;
                return Err(e.into());
            }
            Err(_) => {
                warn!("HTTP request timeout for URL: {} (timeout: {:?})", url, connect_timeout);
                self.add_timeout_error(url.to_string(), target_path.to_path_buf()).await;
                return Err(anyhow::anyhow!("HTTP request timeout for: {}", url));
            }
        };
        
        let status = response.status();

        // Check status code before proceeding with specific handling for 403 and 404
        if !status.is_success() {
            if status.as_u16() == 403 {
                // Read the response body to get the actual error message
                let error_body = match http_body_util::BodyExt::collect(response.into_body()).await {
                    Ok(bytes) => {
                        match String::from_utf8(bytes.to_bytes().to_vec()) {
                            Ok(body_text) => {
                                // Limit the error message to prevent extremely long messages
                                if body_text.len() > 500 {
                                    format!("{}...", &body_text[..500])
                                } else {
                                    body_text
                                }
                            }
                            Err(_) => "Unable to read error message (non-UTF8 response)".to_string()
                        }
                    }
                    Err(e) => format!("Unable to read error message: {}", e)
                };
                warn!("Access denied (403 Forbidden) for URL: {} - Error: {}", url, error_body);
                self.add_not_entitled_error(url.to_string(), target_path.to_path_buf(), error_body).await;
                return Err(anyhow::anyhow!("Access denied (403 Forbidden): {}", url));
            } else if status.as_u16() == 404 {
                // Read the response body to get the actual error message
                let error_body = match http_body_util::BodyExt::collect(response.into_body()).await {
                    Ok(bytes) => {
                        match String::from_utf8(bytes.to_bytes().to_vec()) {
                            Ok(body_text) => {
                                // Limit the error message to prevent extremely long messages
                                if body_text.len() > 500 {
                                    format!("{}...", &body_text[..500])
                                } else {
                                    body_text
                                }
                            }
                            Err(_) => "Unable to read error message (non-UTF8 response)".to_string()
                        }
                    }
                    Err(e) => format!("Unable to read error message: {}", e)
                };
                warn!("File not found (404 Not Found) for URL: {} - Error: {}", url, error_body);
                self.add_not_found_error(url.to_string(), target_path.to_path_buf(), error_body).await;
                return Err(anyhow::anyhow!("File not found (404 Not Found): {}", url));
            } else {
                warn!("HTTP {} error for URL: {}", status, url);
                self.add_failed_download(url.to_string(), target_path.to_path_buf(), None).await;
                return Err(anyhow::anyhow!("HTTP error {}: {}", status, url));
            }
        }

        // Create parent directories if they don't exist
        if let Some(parent) = target_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Download body with data-based timeout (activity-based instead of fixed time)
        let bytes = match self.download_with_data_timeout(response.into_body()).await {
            Ok(bytes) => bytes,
            Err(e) => {
                if e.to_string().contains("timeout") {
                    self.add_timeout_error(url.to_string(), target_path.to_path_buf()).await;
                } else {
                    self.add_failed_download(url.to_string(), target_path.to_path_buf(), None).await;
                }
                return Err(e);
            }
        };

        tokio::fs::write(target_path, bytes).await?;

        let download_duration = download_start.elapsed();
        
        if !target_path.exists() {
            let mut report = self.download_report.lock().await;
            report.files_missing.push(target_path.to_path_buf());
            debug!("Adding missing file during download: {}", target_path.display());
        }

        // Update timeout tracker with successful download
        self.record_successful_download(url, download_duration).await;

        // Update download stats
        self.update_download_stats(target_path).await;

        info!("Downloaded: {} (took {:?})", target_path.display(), download_duration);
        self.mark_file_processed(relative_path).await;

        Ok(())
    }

    async fn add_failed_download(
        &self,
        url: String,
        path: PathBuf,
        checksum: Option<(String, String)>,
    ) {
        let mut failed = self.failed.lock().await;
        // Only add to failed downloads if it's not already there
        if !failed.contains_key(&url) {
            failed.insert(url, (path.clone(), checksum));
            // Update the report when adding a new failed download
            let mut report = self.download_report.lock().await;
            if !report.files_missing.contains(&path) {
                report.files_missing.push(path);
            }
        }
    }

    pub async fn retry_failed_downloads(&self) -> Result<()> {
        let failed_downloads = {
            let failed = self.failed.lock().await;
            failed.clone()
        };

        if failed_downloads.is_empty() {
            info!("No failed downloads to retry");
            return Ok(());
        }

        info!("Starting multi-threaded retry process for {} failed downloads...", failed_downloads.len());
        
        // First check which URLs we should skip (403 errors in not_entitled list)
        let not_entitled_urls: HashSet<String> = {
            let report = self.download_report.lock().await;
            report.not_entitled.iter().map(|(url, _, _)| url.clone()).collect()
        };
        
        let original_failed_count = failed_downloads.len();
        
        // Filter out URLs that should be skipped
        let retry_candidates: Vec<(String, PathBuf)> = failed_downloads
            .into_iter()
            .filter_map(|(url, (path, _checksum))| {
                if not_entitled_urls.contains(&url) {
                    debug!("Skipping retry for not entitled URL: {}", url);
                    None
                } else {
                    Some((url, path))
                }
            })
            .collect();
        
        let retry_candidates_count = retry_candidates.len();
        let skipped_count = original_failed_count - retry_candidates_count;
        
        if retry_candidates.is_empty() {
            info!("No downloads to retry after filtering (all were 403 Forbidden)");
            return Ok(());
        }
        
        info!("Retrying {} downloads concurrently (skipping {} not entitled)...", 
              retry_candidates_count, skipped_count);
        
        // Use a semaphore to control concurrency during retries
        let retry_semaphore = Arc::new(Semaphore::new(self.verifier.config.download.max_concurrent_downloads()));
        let mut retry_tasks = Vec::new();
        
        for (url, path) in retry_candidates {
            let this = self.clone();
            let permit = retry_semaphore.clone().acquire_owned().await?;
            let url_clone = url.clone();
            let path_clone = path.clone();
            
            retry_tasks.push(tokio::spawn(async move {
                let _permit = permit; // Hold permit for duration of retry
                
                info!("Retrying download: {}", url_clone);
                
                // Update retry attempt counter
                {
                    let mut report = this.download_report.lock().await;
                    report.retry_attempts += 1;
                }
                
                match this.download_file(&url_clone, &path_clone).await {
                    Ok(_) => {
                        info!("Retry successful for: {}", url_clone);
                        
                        // Remove from failed downloads
                        {
                            let mut failed = this.failed.lock().await;
                            failed.remove(&url_clone);
                        }
                        
                        // Update stats
                        {
                            let mut report = this.download_report.lock().await;
                            if let Some(pos) = report.files_missing.iter().position(|x| x == &path_clone) {
                                report.files_missing.remove(pos);
                                report.files_downloaded += 1;
                                report.retry_successes += 1;
                            }
                        }
                        
                        Ok(())
                    }
                    Err(e) => {
                        if e.to_string().contains("403 Forbidden") {
                            debug!("Retry returned 403 for {}, marking as not entitled", url_clone);
                            this.add_not_entitled_error(url_clone.clone(), path_clone, "Retry failed with 403 Forbidden".to_string()).await;
                        } else {
                            warn!("Retry failed for {}: {}", url_clone, e);
                        }
                        Err(e)
                    }
                }
            }));
        }
        
        // Wait for all retry tasks to complete
        let results = join_all(retry_tasks).await;
        
        let mut retry_successful = 0;
        let mut retry_failed = 0;
        
        for result in results {
            match result {
                Ok(Ok(_)) => retry_successful += 1,
                Ok(Err(_)) => retry_failed += 1,
                Err(e) => {
                    retry_failed += 1;
                    warn!("Retry task join error: {}", e);
                }
            }
        }
        
        info!("Multi-threaded retry process completed: {} successful, {} failed, {} skipped (not entitled)", 
               retry_successful, retry_failed, skipped_count);
        Ok(())
    }

    pub async fn get_download_report(&self) -> DownloadReport {
        // Collect data from each mutex separately to avoid holding multiple locks
        let mut report = {
            let report_guard = self.download_report.lock().await;
            report_guard.clone()
        };
        
        // Get current failed downloads (separate lock)
        let failed_downloads: Vec<PathBuf> = {
            let failed = self.failed.lock().await;
            failed.values().map(|(path, _)| path.clone()).collect()
        };
        
        // Update missing files count to match failed downloads
        report.files_missing = failed_downloads;

        // Update other stats (separate lock)
        let processed_count = {
            let processed = self.processed_files.lock().await;
            processed.processed_paths.len()
        };
        report.processed_files = processed_count;
        
        // Calculate skipped files correctly
        let non_skipped = report.files_downloaded           // Successfully downloaded
            + report.files_missing.len()                    // Failed downloads
            + report.access_errors.len()                    // Access errors
            + report.checksum_mismatches.len();            // Checksum mismatches

        report.files_skipped = report.processed_files.saturating_sub(non_skipped);
        
        report
    }

    async fn update_download_stats(&self, path: &Path) {
        let mut downloaded = self.downloaded.lock().await;
        let is_new = downloaded.insert(path.to_path_buf());

        if is_new {
            let mut report = self.download_report.lock().await;
            report.files_downloaded += 1;
            
            // We don't increment totals here since they're counted during process_metadata
            if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
                match ext {
                    "xml" => { report.processed_xmls.insert(path.to_path_buf()); }
                    "zip" => { report.processed_zips.insert(path.to_path_buf()); }
                    "vib" => { report.downloaded_vibs.insert(path.to_path_buf()); }
                    _ => {}
                }
            }
        }
    }

    // Add helper method for tracking checksum mismatches
    async fn add_checksum_mismatch(&self, path: &Path, expected: &str, actual: &str) {
        let mut report = self.download_report.lock().await;
        report.checksum_mismatches.push((
            path.to_path_buf(),
            expected.to_string(),
            actual.to_string()
        ));
    }

    async fn add_access_error(&self, path: &Path, error: String) {
        let mut report = self.download_report.lock().await;
        report.access_errors.push((path.to_path_buf(), error));
    }

    async fn add_missing_file(&self, path: PathBuf) {
        let mut report = self.download_report.lock().await;
        if !report.files_missing.contains(&path) {
            debug!("Adding missing file: {}", path.display());
            report.files_missing.push(path);
        }
    }

    // Add helper method for tracking 403 not entitled errors
    async fn add_not_entitled_error(&self, url: String, path: PathBuf, error_message: String) {
        let mut report = self.download_report.lock().await;
        report.not_entitled.push((url, path, error_message));
    }

    // Add helper method for tracking 404 not found errors
    async fn add_not_found_error(&self, url: String, path: PathBuf, error_message: String) {
        let mut report = self.download_report.lock().await;
        report.not_found.push((url, path, error_message));
    }

    // Add helper method for tracking timeout errors
    async fn add_timeout_error(&self, url: String, path: PathBuf) {
        let mut report = self.download_report.lock().await;
        report.timeout_errors.push((url, path));
        
        // Also update timeout tracker
        let mut tracker = self.timeout_tracker.lock().await;
        tracker.record_timeout();
    }

    // Calculate adaptive timeout based on file type and historical data
    async fn calculate_adaptive_timeout(&self, url: &str) -> std::time::Duration {
        let mut tracker = self.timeout_tracker.lock().await;
        tracker.maybe_reduce_timeout();
        
        let base_timeout = tracker.get_current_timeout();
        
        // Increase timeout for large files based on URL patterns
        if url.ends_with(".vib") || url.contains("vib20") {
            // VIB files can be large (50-200MB), give them more time
            base_timeout * 2
        } else if url.ends_with(".zip") || url.contains("metadata") {
            // Metadata ZIP files are usually smaller but still need reasonable time
            std::cmp::max(base_timeout, std::time::Duration::from_secs(60))
        } else if url.ends_with(".rpm") {
            // RPM packages can be large
            base_timeout * 2
        } else {
            // XML files and other small files
            std::cmp::min(base_timeout, std::time::Duration::from_secs(60))
        }
    }

    // Record successful download for timeout adaptation
    async fn record_successful_download(&self, _url: &str, duration: std::time::Duration) {
        // If download was very fast, we could consider reducing timeouts
        if duration < std::time::Duration::from_secs(5) {
            let mut tracker = self.timeout_tracker.lock().await;
            tracker.maybe_reduce_timeout();
        }
    }

    /*async fn verify_checksum(&self, path: &Path, expected: &str, checksum_type: &str) -> Result<bool> {
        // Convert path to URL format for pattern matching
        let path_str = path.to_string_lossy().replace('\\', "/");
        if self.config.exclude.should_exclude(&path_str) {
            debug!("Skipping checksum verification for excluded file: {}", path.display());
            return Ok(true);
        }
        // ...existing code...
    }*/

    pub async fn get_failed_downloads(&self) -> Vec<String> {
        let failed = self.failed.lock().await;
        failed.keys().cloned().collect()
    }

    fn sanitize_filename(&self, url: &str) -> String {
        // Get the last part of the URL before query parameters
        let base_name = url.split('?').next().unwrap_or(url)
            .split('/').last().unwrap_or("downloaded_file");
            
        // Replace invalid characters with underscores
        let invalid_chars = ['<', '>', ':', '"', '/', '\\', '|', '?', '*'];
        let mut filename = String::new();
        for c in base_name.chars() {
            if invalid_chars.contains(&c) {
                filename.push('_');
            } else {
                filename.push(c);
            }
        }
        
        // Add .json extension if it's a JSON file and doesn't have an extension
        if url.contains("type=vsan-updates-json") && !filename.ends_with(".json") {
            filename.push_str(".json");
        }
        
        filename
    }

    fn replace_download_token(&self, url: &str, token: Option<&str>) -> String {
        if let Some(token_value) = token {
            if url.contains("<downloadToken>") {
                debug!("Replacing <downloadToken> in URL with provided token");
                return url.replace("<downloadToken>", token_value);
            }
        } else if url.contains("<downloadToken>") {
            warn!("URL contains <downloadToken> placeholder but no token provided: {}", url);
        }
        url.to_string()
    }

    fn process_vcsa_url(&self, template_url: &str, version: &str, file: &str) -> String {
        format!("{}/{}/{}", template_url, version, file)
    }

    fn get_vcsa_file_path(&self, version: &str, file: &str) -> PathBuf {
        // Extract filename from path (after last /)
        let filename = file.split('/').last().unwrap_or(file);
        
        // Create path: base_path/valm/version/filename
        self.base_path
            .join("valm")
            .join(version)
            .join(filename)
    }

    async fn process_vcsa_manifest(&self, content: &str, version: &str, base_url: &str) -> Result<()> {
        let packages = self.xml_parser.parse_vcsa_packages(content)?;
        info!("Found {} packages in VCSA manifest-latest.xml for version {}", packages.len(), version);
        self.queue_vcsa_packages(packages, version, base_url, "manifest-latest.xml").await
    }

    /// Parse VCSA rpm-manifest.json and queue any files it lists that aren't
    /// already covered by manifest-latest.xml (blobs, container manifests, and
    /// any extra RPMs listed only in the JSON).
    async fn process_vcsa_rpm_manifest_json(&self, content: &str, version: &str, base_url: &str) -> Result<()> {
        let packages = self.xml_parser.parse_vcsa_rpm_manifest_json(content)?;
        info!("Found {} packages in VCSA rpm-manifest.json for version {}", packages.len(), version);
        self.queue_vcsa_packages(packages, version, base_url, "rpm-manifest.json").await
    }

    /// Shared back-end for processing a list of VcsaPackage entries: builds URLs,
    /// skips already-processed / already-valid files, and spawns concurrent
    /// download tasks.
    async fn queue_vcsa_packages(
        &self,
        packages: Vec<VcsaPackage>,
        version: &str,
        base_url: &str,
        source_label: &str,
    ) -> Result<()> {
        let start_time = std::time::Instant::now();

        let mut tasks = Vec::new();
        let max_concurrent = self.verifier.config.verification.max_concurrent_files();
        let file_semaphore = Arc::new(Semaphore::new(max_concurrent));

        for package in packages {
            let location = package.location.clone();
            // pkg_info is only used for log output - use the filename with the
            // package-pool/ prefix stripped, keeping the file extension so that
            // non-RPM entries (like .blob / .manifest) are still readable in logs.
            let pkg_info = location
                .strip_prefix("package-pool/")
                .unwrap_or(&location);

            let url = format!(
                "{}/{}/{}",
                base_url,
                version,
                location
            );

            /*// Clone the values we need before the async move
            let location = package.location.clone();
            let pkg_info = location
                .strip_prefix("package-pool/")
                .and_then(|s| s.strip_suffix(".rpm"))
                .unwrap_or(&location);
                
            let url = format!(
                //https://vapp-updates.vmware.com/vai-catalog/valm/vmw/8d167796-34d5-4899-be0a-6daade4005a3/{version}.latest/{file}
                "https://vapp-updates.vmware.com/vai-catalog/valm/vmw/8d167796-34d5-4899-be0a-6daade4005a3/{}.latest/{}",
                version,
                location
            );*/
            let target_path = self.get_vcsa_file_path(version, &location);
            let checksum = package.checksum.clone();
            let checksum256 = package.checksum256.clone();

            // Skip if already processed and exists
            if self.is_file_processed(&target_path.to_string_lossy()).await && target_path.exists() {
                debug!("Skipping already processed VCSA package: {}", pkg_info);
                continue;
            }

            let this = self.clone();
            let permit = file_semaphore.clone().acquire_owned().await?;
            let target_path_clone = target_path.clone();
            let pkg_info = pkg_info.to_string(); // Clone for the closure

            tasks.push(tokio::spawn(async move {
                let _permit = permit;
                
                let needs_download = if target_path_clone.exists() {
                    // Prefer SHA256 if available, fall back to SHA1
                    let (checksum, checksum_type) = if !checksum256.is_empty() {
                        (checksum256.as_str(), "sha-256")
                    } else {
                        (checksum.as_str(), "sha-1")
                    };

                    match this.verifier.verify_checksum(&target_path_clone, checksum, checksum_type).await {
                        Ok(true) => false,
                        Ok(false) => {
                            info!("Checksum mismatch for {}, will redownload", pkg_info);
                            this.add_checksum_mismatch(&target_path_clone, checksum, "failed_verification").await;
                            let _ = tokio::fs::remove_file(&target_path_clone).await;
                            true
                        }
                        Err(e) => {
                            warn!("Verification error for {}: {}", pkg_info, e);
                            this.add_access_error(&target_path_clone, e.to_string()).await;
                            true
                        }
                    }
                } else {
                    true
                };

                if needs_download {
                    info!("Downloading VCSA package: {} -> {}", pkg_info, target_path_clone.display());
                    if let Err(e) = this.download_file(&url, &target_path_clone).await {
                        warn!("Error downloading package {} ({}): {}", pkg_info, url, e);
                        this.add_failed_download(url, target_path_clone, None).await;
                    }
                } else {
                    debug!("Skipping download of VCSA package (checksum valid): {}", pkg_info);
                }

                Ok::<(), anyhow::Error>(())
            }));
        }

        // Wait for all tasks to complete with progress tracking
        let total_tasks = tasks.len();
        info!("Processing {} VCSA packages concurrently (max_concurrent: {})...", total_tasks, max_concurrent);
        
        let results = join_all(tasks).await;
        let mut completed = 0;
        let mut errors = 0;
        
        for result in results {
            match result {
                Ok(Ok(())) => completed += 1,
                Ok(Err(e)) => {
                    errors += 1;
                    warn!("VCSA package task error: {}", e);
                },
                Err(e) => {
                    errors += 1;
                    warn!("VCSA package task join error: {}", e);
                }
            }
        }
        
        let duration = start_time.elapsed();
        info!("VCSA {} processing completed in {:?}: {}/{} packages successful, {} errors",
              source_label, duration, completed, total_tasks, errors);

        Ok(())
    }

    // Data-based timeout function that monitors data reception instead of fixed time
    async fn download_with_data_timeout<B>(&self, body: B) -> Result<Bytes>
    where
        B: Body + Send + 'static + std::marker::Unpin,
        B::Data: Send + AsRef<[u8]>,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        let mut body_stream = BodyExt::into_data_stream(body);
        let mut buffer = BytesMut::new();
        let mut last_data_time = Instant::now();
        let data_timeout = Duration::from_secs(30); // 30 seconds without data reception
        let max_total_time = Duration::from_secs(600); // 10 minutes total maximum
        let start_time = Instant::now();

        loop {
            match tokio::time::timeout(Duration::from_millis(100), body_stream.next()).await {
                Ok(Some(Ok(chunk))) => {
                    // Received data, reset timeout
                    last_data_time = Instant::now();
                    buffer.extend_from_slice(chunk.as_ref());
                }
                Ok(Some(Err(e))) => {
                    return Err(anyhow::anyhow!("Body stream error: {}", e));
                }
                Ok(None) => {
                    // Stream ended successfully
                    break;
                }
                Err(_) => {
                    // Timeout on waiting for next chunk - check if we should abort
                    let elapsed_since_data = last_data_time.elapsed();
                    let total_elapsed = start_time.elapsed();
                    
                    if elapsed_since_data > data_timeout {
                        return Err(anyhow::anyhow!(
                            "Data timeout: no data received for {:?}", 
                            elapsed_since_data
                        ));
                    }
                    
                    if total_elapsed > max_total_time {
                        return Err(anyhow::anyhow!(
                            "Total timeout: download took longer than {:?}", 
                            max_total_time
                        ));
                    }
                    
                    // Continue waiting
                    continue;
                }
            }
        }

        Ok(buffer.freeze())
    }

    // Output 403 and 404 errors to JSON file for use with exclusion patterns
    pub async fn output_403_errors_to_json(&self, _base_path: &Path) -> Result<()> {
        let report = self.download_report.lock().await;
        
        // Collect both 403 and 404 errors
        if report.not_entitled.is_empty() && report.not_found.is_empty() {
            info!("No 403 Forbidden or 404 Not Found errors to output");
            return Ok(());
        }
        
        // Create exclusion patterns from both 403 and 404 errors
        let mut all_error_urls = Vec::new();
        
        // Collect 403 errors
        for (url, _, _) in &report.not_entitled {
            all_error_urls.push(url.clone());
        }
        
        // Collect 404 errors  
        for (url, _, _) in &report.not_found {
            all_error_urls.push(url.clone());
        }
        
        let exclusion_patterns: Vec<String> = all_error_urls
            .iter()
            .map(|url| {
                // Extract meaningful patterns from URLs that can be used for exclusion
                self.extract_exclusion_pattern(url)
            })
            .collect::<HashSet<_>>() // Remove duplicates
            .into_iter()
            .collect();
        
        // Combine detailed errors from both categories
        let mut detailed_errors = Vec::new();
        
        // Add 403 errors
        for (url, path, error_message) in &report.not_entitled {
            detailed_errors.push(serde_json::json!({
                "url": url,
                "target_path": path.display().to_string(),
                "error_type": "403 Forbidden - Not Entitled",
                "error_message": error_message
            }));
        }
        
        // Add 404 errors
        for (url, path, error_message) in &report.not_found {
            detailed_errors.push(serde_json::json!({
                "url": url,
                "target_path": path.display().to_string(),
                "error_type": "404 Not Found - File Does Not Exist",
                "error_message": error_message
            }));
        }
        
        let json_data = serde_json::json!({
            "description": "Auto-generated exclusion patterns from 403 Forbidden and 404 Not Found errors",
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "total_403_errors": report.not_entitled.len(),
            "total_404_errors": report.not_found.len(),
            "total_errors": report.not_entitled.len() + report.not_found.len(),
            "unique_patterns": exclusion_patterns.len(),
            "exclusion_patterns": exclusion_patterns,
            "detailed_errors": detailed_errors
        });
        
        let json_file_path = std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")).join("exclusions.json");
        
        // Check if file already exists
        if json_file_path.exists() {
            warn!("Exclusions file already exists: {}. Cannot overwrite existing file.", json_file_path.display());
            warn!("To update exclusions, please delete or rename the existing file and run again.");
            return Ok(());
        }
        
        let json_content = serde_json::to_string_pretty(&json_data)?;
        tokio::fs::write(&json_file_path, json_content).await?;
        
        info!("Exported {} HTTP errors to: {} ({} 403 Forbidden, {} 404 Not Found)", 
              report.not_entitled.len() + report.not_found.len(), 
              json_file_path.display(),
              report.not_entitled.len(),
              report.not_found.len());
        info!("Generated {} unique exclusion patterns", exclusion_patterns.len());
        info!("To use these patterns, add 'exclude_file = {:?}' to your config.toml", 
              json_file_path.display().to_string());
        
        Ok(())
    }
    
    // Extract meaningful exclusion patterns from URLs
    fn extract_exclusion_pattern(&self, url: &str) -> String {
        // Try to extract meaningful patterns that can be used for exclusion
        if let Ok(parsed_url) = url::Url::parse(url) {
            let path = parsed_url.path();
            let path_segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
            
            // For Broadcom URLs, we want to preserve meaningful path context
            // Look for key path markers that indicate where meaningful paths start
            let mut meaningful_start_idx = 0;
            
            for (i, segment) in path_segments.iter().enumerate() {
                // Skip generic segments at the beginning
                if matches!(segment.to_uppercase().as_str(), "PROD" | "COMP" | "SOFTWARE" | "VUM" | "PRODUCTION") 
                    || segment.starts_with("dl.") 
                    || segment.contains(".com") 
                    || segment.len() > 20 // Likely a token
                {
                    meaningful_start_idx = i + 1;
                    continue;
                }
                
                // Found a meaningful segment, break and use this as start
                if segment.len() > 2 && !segment.chars().all(|c| c.is_numeric()) {
                    meaningful_start_idx = i;
                    break;
                }
            }
            
            // If we found meaningful segments, use them
            if meaningful_start_idx < path_segments.len() {
                let meaningful_path = path_segments[meaningful_start_idx..].join("/");
                
                // Don't make it too broad - if it's just a filename, add some parent context
                if meaningful_path.contains('/') {
                    return meaningful_path;
                } else {
                    // Single segment (likely just filename), try to include parent context
                    let context_start = if meaningful_start_idx > 0 { meaningful_start_idx - 1 } else { meaningful_start_idx };
                    return path_segments[context_start..].join("/");
                }
            }
            
            // Fallback: use the path without protocol/domain, but keep significant structure
            let clean_path = path.trim_start_matches('/');
            if !clean_path.is_empty() {
                return clean_path.to_string();
            }
        }
        
    // Final fallback: use the URL as-is
        url.to_string()
    }

    // OAuth authentication helper methods
    async fn get_oauth_token(&self, auth_url: &str, client_id: &str, client_secret: &str) -> Result<String> {
        info!("Getting OAuth token from: {}", auth_url);
        
        // Create OAuth request body
        let oauth_body = serde_json::json!({
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret
        });
        
        let oauth_body_str = oauth_body.to_string();
        
        // Create a new client instance for OAuth request with Full body type
        let https_connector = HttpsConnector::new();
        let oauth_client = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
            .build::<_, http_body_util::Full<Bytes>>(https_connector);
            
        // Make OAuth request
        let request = hyper::Request::builder()
            .method(hyper::Method::POST)
            .uri(auth_url)
            .header("Content-Type", "application/json")
            .header("User-Agent", "DSvClient/1.0.0")
            .body(http_body_util::Full::new(Bytes::from(oauth_body_str)))?;
            
        let response = oauth_client.request(request).await?;
        
        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "OAuth token request failed with status: {}", 
                response.status()
            ));
        }
        
        let body_bytes = http_body_util::BodyExt::collect(response.into_body())
            .await?
            .to_bytes();
        let body_str = String::from_utf8(body_bytes.to_vec())?;
        
        // Parse response to extract access token
        let token_response: serde_json::Value = serde_json::from_str(&body_str)?;
        
        let access_token = token_response
            .get("access_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("No access_token in OAuth response"))?;
            
        info!("Successfully obtained OAuth access token");
        Ok(access_token.to_string())
    }
    
    async fn download_with_oauth(&self, url: &str, access_token: &str, target_path: &Path) -> Result<()> {
        info!("Downloading OAuth-protected resource: {}", url);
        
        let mut current_url = url.to_string();
        let mut redirect_count = 0;
        const MAX_REDIRECTS: usize = 5;
        
        loop {
            // Create request with OAuth token in X-Vmw-Esp-Client header
            let request = hyper::Request::builder()
                .method(hyper::Method::GET)
                .uri(&current_url)
                .header("X-Vmw-Esp-Client", access_token)
                .header("User-Agent", "HCL_Python/1.0.0/python")
                .body(http_body_util::Empty::<Bytes>::new())?;
                
            let response = self.client.request(request).await?;
            let status = response.status();
            
            if status.is_success() {
                // Success - process the response
                info!("OAuth download successful after {} redirects", redirect_count);
                
                // Create parent directories if they don't exist
                if let Some(parent) = target_path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                
                // Download the response body
                let body_bytes = http_body_util::BodyExt::collect(response.into_body())
                    .await?
                    .to_bytes();
                    
                // Save the file as-is without decompression
                info!("Saving {} bytes to file", body_bytes.len());
                tokio::fs::write(target_path, body_bytes).await?;
                
                info!("Successfully saved OAuth file to: {}", target_path.display());
                return Ok(());
            } else if status.is_redirection() {
                // Handle redirect
                if redirect_count >= MAX_REDIRECTS {
                    return Err(anyhow::anyhow!(
                        "Too many redirects ({}) for OAuth download: {}", 
                        MAX_REDIRECTS, 
                        url
                    ));
                }
                
                // Get the Location header for the redirect
                let location = response.headers()
                    .get("location")
                    .and_then(|v| v.to_str().ok())
                    .ok_or_else(|| anyhow::anyhow!(
                        "Redirect response missing Location header for URL: {}", 
                        current_url
                    ))?;
                
                // Handle relative vs absolute URLs
                current_url = if location.starts_with("http") {
                    location.to_string()
                } else {
                    // Construct absolute URL from relative redirect
                    let base_url = url::Url::parse(&current_url)?;
                    base_url.join(location)?.to_string()
                };
                
                redirect_count += 1;
                info!("Following OAuth redirect #{} to: {}", redirect_count, current_url);
                
                // Consume the response body to avoid connection issues
                let _ = http_body_util::BodyExt::collect(response.into_body()).await;
                
                // Continue the loop to make the redirected request
                continue;
            } else {
                // Error status
                return Err(anyhow::anyhow!(
                    "OAuth download failed with status: {} for URL: {}", 
                    status,
                    current_url
                ));
            }
        }
    }
}
