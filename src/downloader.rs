use crate::config::AppConfig;
use crate::parser::{Vendor, XmlParser};
use crate::process::{FileInfo, FileType, ProcessManager, Source};
use crate::verify::VerificationManager;
use anyhow::Result;
use bytes::Bytes;
use futures::future::join_all;
use http_body_util::Empty;
use hyper_tls::HttpsConnector;
use hyper_util::client::legacy::Client;
use log::{debug, info, warn};
//use rayon::prelude::*;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore}; // Change to use tokio's Mutex // Use renamed import

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

type ProcessedFiles = Arc<Mutex<FileTracker>>;

#[derive(Debug, Default, Clone)]
pub struct DownloadReport {
    pub files_downloaded: usize,
    pub files_skipped: usize,
    pub failed_downloads: Vec<String>,
    pub processed_files: usize,
    pub processed_xmls: HashSet<PathBuf>,
    pub processed_zips: HashSet<PathBuf>,
    pub downloaded_vibs: HashSet<PathBuf>,
    pub checksum_mismatches: Vec<(PathBuf, String, String)>, // (path, expected, actual)
    pub access_errors: Vec<(PathBuf, String)>, // Add new field for access errors
    pub files_missing: Vec<PathBuf>, // Add new field for missing files
    pub total_xml_processed: usize,  // Add counter for total XML files found
    pub total_zip_processed: usize,  // Add counter for total ZIP files found
    pub total_vib_processed: usize,  // Add counter for total VIB files found
}

impl DownloadReport {
    pub fn print_summary(&self) {
        info!("\nDownload Summary:");

        if (!self.failed_downloads.is_empty()) {
            warn!("\nFailed Downloads ({}):", self.failed_downloads.len());
            for url in &self.failed_downloads {
                warn!("  {}", url);
            }
        }

        if (!self.checksum_mismatches.is_empty()) {
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

        if (!self.access_errors.is_empty()) {
            warn!("\nAccess Errors ({}):", self.access_errors.len());
            for (path, error) in &self.access_errors {
                warn!("  {}: {}", path.display(), error);
            }
        }

        if (!self.files_missing.is_empty()) {
            warn!("\nMissing Files ({}):", self.files_missing.len());
            for path in &self.files_missing {
                warn!("  {}", path.display());
            }
        }

        // Update these lines to use total counters instead of HashSet lengths
        info!("XML files processed: {}", self.total_xml_processed);
        info!("ZIP files processed: {}", self.total_zip_processed);
        info!("Total files downloaded: {}", self.files_downloaded);
        info!("      Files with checksum mismatches: {}", self.checksum_mismatches.len());


        info!("Total files checked: {}", self.processed_files);
        info!("      Files skipped: {}", self.files_skipped);
        info!("      Files with access errors: {}", self.access_errors.len());
        //info!("Files missing on disk: {}", self.files_missing.len());

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
    //config: Arc<AppConfig>, // Use AppConfig instead of Config
    download_report: Arc<Mutex<DownloadReport>>, // Add this field
}

#[derive(Debug, Deserialize)]
struct SourceConfig {
    sources: Vec<SourceEntry>,
}

#[derive(Debug, Deserialize)]
struct SourceEntry {
    url: String,
    enabled: bool,
    status: String,
    r#type: String,
    //vendor: String,
    //#[serde(rename = "type")]
    //source_type: String,
    //description: String,
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
            verifier: Arc::new(VerificationManager::new(config.clone())),
            xml_parser: Arc::new(XmlParser::new()),
            download_semaphore: Arc::new(Semaphore::new(config.download.max_concurrent_downloads)),
            processed_files: Arc::new(Mutex::new(FileTracker::default())),
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
            report.total_xml_processed += 1;
        }

        // Get base URL without filename
        let base_url = url.rsplit_once('/').map(|(base, _)| base).unwrap_or(url);

        // Download and parse the main index XML
        match self.download_xml(url).await {
            Ok(index_content) => {
                // Try to process as a vendor list first (like addon-main)
                if let Ok(vendors) = self.xml_parser.parse_vendor_list(&index_content) {
                    // Count each vendor XML
                    {
                        let mut report = self.download_report.lock().await;
                        report.total_xml_processed += vendors.len();
                    }
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
                    // Process all metadata concurrently with rate limiting
                    let mut metadata_tasks = Vec::new();
                    for metadata in metadata_list {
                        let metadata_url = format!("{}/{}", base_url, metadata.url);
                        let this = self.clone();
                        let _permit = self.download_semaphore.clone().acquire_owned().await?;
                        metadata_tasks.push(tokio::spawn(async move {
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
        let sources = self.read_sources_file().await?;

        // Process all sources concurrently
        let mut tasks = Vec::new();
        for source in sources {
            if source.enabled && source.r#type == "Host" {
                let this = self.clone();
                let url = source.url.clone();
                tasks.push(tokio::spawn(async move {
                    info!("Processing enabled source: {}", url);
                    if let Err(e) = this.process_repository(&url).await {
                        warn!("Error processing {}: {}", url, e);
                    }
                }));
            }
        }

        // Wait for all tasks to complete
        join_all(tasks).await;
        Ok(())
    }

    async fn read_sources_file(&self) -> Result<Vec<SourceEntry>> {
        let sources_file = PathBuf::from("sources.toml");
        if (!sources_file.exists()) {
            return Err(anyhow::anyhow!("sources.toml file not found"));
        }

        let content = tokio::fs::read_to_string(sources_file).await?;
        let config: SourceConfig = toml::from_str(&content)?;

        if config.sources.is_empty() {
            warn!("No valid sources found in sources.toml");
        }

        Ok(config.sources)
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
            url[relative_idx..].to_string()
        } else {
            // Fallback: use the last part of the URL
            url.rsplit('/').next().unwrap_or(url).to_string()
        }
    }

    async fn process_metadata(&self, url: &str) -> Result<()> {
        info!("Processing metadata from URL: {}", url);
        let relative_base = self.extract_relative_path(url);

        let source = Source::Http(url.to_string());
        let files = self.processor.process_source(source).await?;
        info!("Found {} files to process", files.len());

        // Count and track files before any processing
        {
            let mut report = self.download_report.lock().await;
            let meta_path = self.base_path.join(&relative_base);
            report.total_zip_processed += 1;
            report.processed_zips.insert(meta_path);
            for file in &files {
                report.total_vib_processed += 1;
            }
        }

        // Use a timeout for the entire batch of tasks
        let timeout_duration = std::time::Duration::from_secs(300); // 5 minute timeout
        
        // Process files concurrently with timeouts
        let mut tasks = Vec::new();
        for file in files {
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
            report.total_vib_processed += 1;
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
                    let permit = self.download_semaphore.clone().acquire_owned().await?;
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

        // Wait for all tasks to complete
        for result in join_all(tasks).await {
            match result {
                Ok(Ok(())) => (),
                Ok(Err(e)) => warn!("Task error: {}", e),
                Err(e) => warn!("Task join error: {}", e),
            }
        }

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

        // Check if XML exists and try to read it first
        if target_path.exists() {
            match tokio::fs::read_to_string(&target_path).await {
                Ok(content) => {
                    debug!("Using cached XML file: {}", target_path.display());
                    return Ok(content);
                }
                Err(e) => warn!("Failed to read cached XML {}: {}", target_path.display(), e),
            }
        }

        // Download if not cached or cache read failed
        info!("Downloading XML: {}", url);
        let response = self.client.get(url.parse()?).await?;

        if (!response.status().is_success()) {
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

        let response = self.client.get(url.parse()?).await?;
        let status = response.status();

        // Check status code before proceeding
        if (!status.is_success()) {
            warn!("HTTP {} error for URL: {}", status, url);
            // Add to failed downloads without saving the file
            self.add_failed_download(url.to_string(), target_path.to_path_buf(), None)
                .await;
            return Err(anyhow::anyhow!("HTTP error {}: {}", status, url));
        }

        // Create parent directories if they don't exist
        if let Some(parent) = target_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let bytes = http_body_util::BodyExt::collect(response.into_body())
            .await?
            .to_bytes();

        tokio::fs::write(target_path, bytes).await?;

        if !target_path.exists() {
            let mut report = self.download_report.lock().await;
            report.files_missing.push(target_path.to_path_buf());
            debug!("Adding missing file during download: {}", target_path.display());
        }

/*         // Only update downloaded count for new files
        if !target_path.exists() {
            let mut report = self.download_report.lock().await;
            report.files_downloaded += 1;
        } */

        // Update download stats
        self.update_download_stats(target_path).await;

        info!("Downloaded: {}", target_path.display());
        self.mark_file_processed(relative_path).await;

        Ok(())
    }

    async fn add_failed_download(
        &self,
        url: String,
        path: PathBuf,
        checksum: Option<(String, String)>,
    ) {
        let mut failed: tokio::sync::MutexGuard<'_, HashMap<String, (PathBuf, Option<(String, String)>)>> = self.failed.lock().await;
        failed.insert(url, (path, checksum));
    }

    pub async fn retry_failed_downloads(&self) -> Result<()> {
        let failed_downloads = {
            let failed = self.failed.lock().await;
            failed.clone()
        };

        for (url, (path, _checksum)) in failed_downloads {
            info!("Retrying download: {}", url);
            if let Err(e) = self.download_file(&url, &path).await {
                warn!("Retry failed for {}: {}", url, e);
            } else {
                let mut failed = self.failed.lock().await;
                failed.remove(&url);
            }
        }

        Ok(())
    }

/*     pub async fn get_file_type_stats(&self) -> HashMap<String, usize> {
        let mut stats = HashMap::new();
        let downloaded = self.downloaded.lock().await;

        for path in downloaded.iter() {
            if let Some(ext) = path.extension() {
                if let Some(ext_str) = ext.to_str() {
                    *stats.entry(ext_str.to_string()).or_insert(0) += 1;
                }
            }
        }

        stats
    }
 */
    pub async fn get_failed_downloads(&self) -> Vec<String> {
        let failed = self.failed.lock().await;
        failed.keys().cloned().collect()
    }

    pub async fn get_download_report(&self) -> DownloadReport {
        let mut report = self.download_report.lock().await.clone();
        let tracker = self.processed_files.lock().await;
        
        // Check for missing files across all tracked files
        for path in tracker.xml_files.iter()
            .chain(tracker.zip_files.iter())
            .chain(tracker.vib_files.iter())
            .chain(report.processed_xmls.iter())
            .chain(report.processed_zips.iter())
            .chain(report.downloaded_vibs.iter())
        {
            if !path.exists() && !report.files_missing.contains(path) {
                report.files_missing.push(path.clone());
                debug!("Adding missing file during report generation: {}", path.display());
            }
        }

        report.processed_files = tracker.processed_paths.len();
        report.files_skipped = tracker.processed_paths.len().saturating_sub(report.files_downloaded);
        
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
}
