#![allow(unused)]
use anyhow::Result;
use std::path::{Path, PathBuf};
use std::collections::{HashSet, HashMap};
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};  // Change to use tokio's Mutex
use log::{info, warn, debug};
use hyper_util::client::legacy::Client;
use hyper_tls::HttpsConnector;
use http_body_util::Empty;
use bytes::Bytes;
use crate::process::{ProcessManager, Source, FileType};
use crate::verify::{self, VerificationManager};
use crate::parser::{XmlParser, VmwarePackage, DepotParser, AddonPackage, AddonMetadata, Vendor};  // Add VmwarePackage, AddonPackage, and AddonMetadata to imports
use std::pin::Pin;
use std::future::Future;
use futures::future::join_all;
use rayon::prelude::*;

const MAX_CONCURRENT_DOWNLOADS: usize = 10;

type DownloadTracker = Arc<Mutex<HashSet<PathBuf>>>;
// Add a new type for tracking processed files
type ProcessedFiles = Arc<Mutex<HashSet<String>>>;

#[derive(Clone)]
pub struct Downloader {
    base_path: PathBuf,
    client: Client<HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>, Empty<Bytes>>,
    downloaded: DownloadTracker,
    failed: Arc<Mutex<HashMap<String, (PathBuf, Option<(String, String)>)>>>,
    processor: Arc<ProcessManager>,
    verifier: Arc<VerificationManager>,
    xml_parser: Arc<XmlParser>,
    download_semaphore: Arc<Semaphore>,
    processed_files: ProcessedFiles,
}

#[derive(Debug)]
struct SourceEntry {
    url: String,
    enabled: bool,
    connected: bool,
}

impl Downloader {
    pub fn new(base_path: PathBuf, client: Client<HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>, Empty<Bytes>>) -> Self {
        Self {
            base_path: base_path.clone(),
            client: client.clone(),
            downloaded: Arc::new(Mutex::new(HashSet::new())),
            failed: Arc::new(Mutex::new(HashMap::new())),
            processor: Arc::new(ProcessManager::new(client.clone(), base_path.clone())),
            verifier: Arc::new(VerificationManager::new_with_concurrency(100)),
            xml_parser: Arc::new(XmlParser::new()),
            download_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_DOWNLOADS)),
            processed_files: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    // Add helper method to check if file was processed
    async fn is_file_processed(&self, relative_path: &str) -> bool {
        let processed = self.processed_files.lock().await;
        processed.contains(relative_path)
    }

    // Add helper method to mark file as processed
    async fn mark_file_processed(&self, relative_path: String) {
        let mut processed = self.processed_files.lock().await;
        processed.insert(relative_path);
    }

    async fn parse_vendors(&self, url: &str, content: &str) -> Result<Vec<(String, String)>> {
        let mut parser = DepotParser::new(content);
        let mut vendor_urls = Vec::new();

        if let Ok(vendors) = parser.parse_vendors() {
            for vendor in vendors {
                let vendor_url = format!(
                    "{}/{}/{}", 
                    url, 
                    vendor.relative_path, 
                    vendor.indexfile
                );
                debug!("Found vendor {} at {}", vendor.name, vendor_url);
                vendor_urls.push((vendor.name.clone(), vendor_url.clone()));
            }
        }

        Ok(vendor_urls)
    }

    pub async fn process_repository(&self, url: &str) -> Result<()> {
        info!("Processing repository: {}", url);
        
        // Get base URL without filename
        let base_url = url.rsplit_once('/').map(|(base, _)| base).unwrap_or(url);
        
        // Download and parse the main index XML
        let index_content = self.download_xml(url).await?;
        let vendors = self.xml_parser.parse_vendor_list(&index_content)?;
        
        // Process vendors concurrently
        let mut vendor_tasks = Vec::new();
        for vendor in vendors {
            let vendor_url = format!("{}/{}/{}", base_url, vendor.relative_path, vendor.indexfile);
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

        Ok(())
    }

    async fn process_vendor(&self, vendor_url: &str, vendor: &Vendor) -> Result<()> {
        // Get base URL without the XML filename
        let base_url = vendor_url.rsplit_once('/').map(|(base, _)| base).unwrap_or(vendor_url);
        
        if let Ok(vendor_content) = self.download_xml(vendor_url).await {
            if let Ok(metadata_list) = self.xml_parser.parse_metadata_list(&vendor_content) {
                let mut metadata_tasks = Vec::new();
                for metadata in metadata_list {
                    // Construct metadata URL from base URL without the index XML
                    let metadata_url = format!("{}/{}", 
                        base_url,
                        metadata.url
                    );
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
        Ok(())
    }

    pub async fn process_sources(&self) -> Result<()> {
        let sources = self.read_sources_file().await?;
        for source in sources {
            if source.enabled && source.connected {
                info!("Processing enabled source: {}", source.url);
                if let Err(e) = self.process_repository(&source.url).await {
                    warn!("Error processing {}: {}", source.url, e);
                    continue;
                }
            }
        }
        Ok(())
    }

    async fn read_sources_file(&self) -> Result<Vec<SourceEntry>> {
        let sources_file = PathBuf::from("sources");
        if !sources_file.exists() {
            return Err(anyhow::anyhow!("Sources file not found"));
        }

        let content = tokio::fs::read_to_string(sources_file).await?;
        let mut sources = Vec::new();
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(content.as_bytes());

        for result in rdr.records() {
            let record = result?;
            if let (Some(url), Some(enabled), Some(status)) = (
                record.get(0),
                record.get(1),
                record.get(2)
            ) {
                // Remove quotes from URL if present
                let clean_url = url.trim_matches('"').to_string();
                sources.push(SourceEntry {
                    url: clean_url,
                    enabled: enabled.trim() == "Yes",
                    connected: status.trim() == "Connected",
                });
            }
        }

        if sources.is_empty() {
            warn!("No valid sources found in sources file");
        }

        Ok(sources)
    }

    pub async fn process_sources_file(&self) -> Result<()> {
        let sources_file = PathBuf::from("sources");  // Changed to look in current dir
        if (!sources_file.exists()) {
            warn!("Sources file not found: {}", sources_file.display());
            return Ok(());
        }
        let content = tokio::fs::read_to_string(sources_file).await?;
        for line in content.lines() {
            let url = line.trim();
            if url.starts_with("\"") {  // Skip CSV header and handle quoted URLs
                continue;
            }
            if url.is_empty() { continue; }
            self.process_repository(url).await?;
        }
        Ok(())
    }

    fn extract_relative_path(&self, url: &str) -> String {
        // Find the index after "VUM/PRODUCTION/"
        if let Some(relative_idx) = url.find("VUM/PRODUCTION/").map(|i| i + "VUM/PRODUCTION/".len()) {
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

        // Process files concurrently while maintaining order for ZIP contents
        let mut tasks = Vec::new();
        for file in files {
            let full_relative_path = if file.relative_path.starts_with("http") {
                self.extract_relative_path(&file.relative_path)
            } else {
                let base_dir = Path::new(&relative_base).parent()
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

            let target_path = self.base_path.join(&full_relative_path);
            info!("Target path: {}", target_path.display());

            match file.file_type {
                FileType::Xml => {
                    // Process XML files sequentially to maintain dependencies
                    if let Source::Path(xml_path) = &file.source {
                        let content = tokio::fs::read_to_string(xml_path).await?;
                        let mut parser = DepotParser::new(&content);
                        
                        if let Ok(vibs) = parser.parse_vib_files() {
                            for vib in vibs {
                                let vib_url = if vib.relative_path.starts_with("http") {
                                    vib.relative_path.clone()
                                } else {
                                    format!("{}/{}", url, vib.relative_path)
                                };

                                let vib_relative_path = self.extract_relative_path(&vib_url);
                                let vib_target_path = self.base_path.join(&vib_relative_path);
                                let _permit = self.download_semaphore.clone().acquire_owned().await?;

                                // Add VIB download task with proper string handling
                                let this = self.clone();
                                tasks.push(tokio::spawn(async move {
                                    if !vib_target_path.exists() || 
                                       (!vib.checksum.is_empty() && !this.verifier.verify_checksum(
                                            &vib_target_path,
                                            &vib.checksum,
                                            &vib.checksum_type
                                        ).await?)
                                    {
                                        this.download_file(&vib_url, &vib_target_path).await?;
                                    }
                                    Ok::<(), anyhow::Error>(())
                                }));
                            }
                        }
                    }
                }
                FileType::Zip => {
                    // Process ZIP files immediately to maintain content extraction
                    let zip_files = self.processor.process_source(file.source).await?;
                    for zip_file in zip_files {
                        if let FileType::Vib = zip_file.file_type {
                            let this = self.clone();
                            let _permit = self.download_semaphore.clone().acquire_owned().await?;
                            let relative_path = zip_file.relative_path.clone();
                            
                            // Make the task Send-safe by moving all required data
                            tasks.push(tokio::spawn(async move {
                                if let Err(e) = this.download_file_with_verify(&relative_path).await {
                                    warn!("Error processing ZIP VIB {}: {}", relative_path, e);
                                }
                                Ok::<(), anyhow::Error>(())
                            }));
                        }
                    }
                }
                FileType::Vib => {
                    let this = self.clone();
                    let _permit = self.download_semaphore.clone().acquire_owned().await?;
                    let file_clone = file.clone();  // Clone file info for the task
                    
                    tasks.push(tokio::spawn(async move {
                        // First verify if exists
                        if target_path.exists() {
                            if let (Some(checksum), Some(checksum_type)) = (&file_clone.checksum, &file_clone.checksum_type) {
                                let valid = this.verifier.verify_checksum(&target_path, checksum, checksum_type).await?;
                                if !valid {
                                    info!("Re-downloading due to checksum mismatch: {}", target_path.display());
                                    if let Source::Http(url) = file_clone.source {
                                        this.download_file(&url, &target_path).await?;
                                        
                                        // Verify after download
                                        let valid = this.verifier.verify_checksum(&target_path, checksum, checksum_type).await?;
                                        if !valid {
                                            warn!("Checksum still mismatches after redownload: {}", target_path.display());
                                            this.add_failed_download(
                                                url.clone(),
                                                target_path.clone(),
                                                Some((checksum_type.clone(), checksum.clone()))
                                            ).await;
                                        }
                                    }
                                }
                            }
                        } else {
                            // File doesn't exist, download it
                            if let Source::Http(url) = file_clone.source {
                                this.download_file(&url, &target_path).await?;
                            }
                        }
                        Ok::<(), anyhow::Error>(())
                    }));
                }
                _ => debug!("Skipping unknown file type: {}", file.relative_path),
            }
        }

        // Wait for all tasks to complete
        for result in join_all(tasks).await {
            if let Err(e) = result? {
                warn!("Task error: {}", e);
            }
        }

        Ok(())
    }

    // Add new helper method to handle download and verification
    async fn download_file_with_verify(&self, relative_path: &str) -> Result<()> {
        let target_path = self.base_path.join(relative_path);
        if !target_path.exists() {
            let url = format!("https://hostupdate.vmware.com/software/VUM/PRODUCTION/{}", relative_path);
            self.download_file(&url, &target_path).await?;
        }
        Ok(())
    }

    async fn download_xml(&self, url: &str) -> Result<String> {
        let response = self.client.get(url.parse()?).await?;
        let bytes = http_body_util::BodyExt::collect(response.into_body())
            .await?
            .to_bytes();
        let content = String::from_utf8(bytes.to_vec())?;
        Ok(content)
    }

    async fn download_file(&self, url: &str, target_path: &Path) -> Result<()> {
        // Skip if file exists and is tracked
        let relative_path = target_path.strip_prefix(&self.base_path)
            .map_or_else(|_| target_path.to_string_lossy().to_string(),
                        |p| p.to_string_lossy().to_string());

        if self.is_file_processed(&relative_path).await && target_path.exists() {
            debug!("Skipping already downloaded file: {}", target_path.display());
            return Ok(());
        }

        // Create parent directories if they don't exist
        if let Some(parent) = target_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let response = self.client.get(url.parse()?).await?;
        let bytes = http_body_util::BodyExt::collect(response.into_body())
            .await?
            .to_bytes();
        tokio::fs::write(target_path, bytes).await?;
        
        // Track the downloaded file
        let mut downloaded = self.downloaded.lock().await;
        downloaded.insert(target_path.to_path_buf());
        
        info!("Downloaded: {}", target_path.display());

        self.mark_file_processed(relative_path).await;
        
        Ok(())
    }

    async fn add_failed_download(&self, url: String, path: PathBuf, checksum: Option<(String, String)>) {
        let mut failed = self.failed.lock().await;
        failed.insert(url, (path, checksum));
    }

    pub async fn retry_failed_downloads(&self) -> Result<()> {
        let failed_downloads = {
            let failed = self.failed.lock().await;
            failed.clone()
        };

        for (url, (path, checksum)) in failed_downloads {
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

    pub async fn get_file_type_stats(&self) -> HashMap<String, usize> {
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

    pub async fn get_failed_downloads(&self) -> Vec<String> {
        let failed = self.failed.lock().await;
        failed.keys().cloned().collect()
    }
}