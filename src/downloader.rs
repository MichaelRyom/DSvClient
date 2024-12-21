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
use crate::parser::{XmlParser, VmwarePackage, DepotParser, AddonPackage, AddonMetadata};  // Add VmwarePackage, AddonPackage, and AddonMetadata to imports
use std::pin::Pin;
use std::future::Future;

type DownloadTracker = Arc<Mutex<HashSet<PathBuf>>>;

#[derive(Clone)]
pub struct Downloader {
    base_path: PathBuf,
    client: Client<HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>, Empty<Bytes>>,
    downloaded: DownloadTracker,
    failed: Arc<Mutex<HashMap<String, (PathBuf, Option<(String, String)>)>>>,
    processor: Arc<ProcessManager>,
    verifier: Arc<VerificationManager>,
    xml_parser: Arc<XmlParser>,
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
        }
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
        
        // Process each vendor
        for vendor in vendors {
            // Construct vendor URL using base URL
            let vendor_url = format!("{}/{}/{}", 
                base_url,
                vendor.relative_path, 
                vendor.indexfile
            );
            info!("Processing vendor {} at {}", vendor.name, vendor_url);
            
            // Download and parse vendor index
            match self.download_xml(&vendor_url).await {
                Ok(vendor_content) => {
                    if let Ok(metadata_list) = self.xml_parser.parse_metadata_list(&vendor_content) {
                        for metadata in metadata_list {
                            let metadata_url = format!("{}/{}/{}", 
                                base_url,
                                vendor.relative_path, 
                                metadata.url
                            );
                            if let Err(e) = self.process_metadata(&metadata_url).await {
                                warn!("Error processing metadata {}: {}", metadata_url, e);
                            }
                        }
                    } else {
                        // Try parsing as addon index if metadata list fails
                        let mut parser = DepotParser::new(&vendor_content);
                        if let Ok(addons) = parser.parse_addon_index() {
                            for addon in addons {
                                let addon_url = if addon.url.starts_with("http") {
                                    addon.url
                                } else {
                                    format!("{}/{}/{}", base_url, vendor.relative_path, addon.url)
                                };
                                if let Err(e) = self.process_metadata(&addon_url).await {
                                    warn!("Error processing addon {}: {}", addon_url, e);
                                }
                            }
                        } else {
                            warn!("Failed to parse vendor index from {}", vendor_url);
                        }
                    }
                }
                Err(e) => {
                    warn!("Error downloading vendor index {}: {}", vendor_url, e);
                }
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

    async fn process_metadata(&self, url: &str) -> Result<()> {
        info!("Processing metadata from URL: {}", url);
        
        // Extract base URL for resolving relative paths
        let base_url = url.rsplit_once('/').map(|(base, _)| base).unwrap_or(url);
        
        let source = Source::Http(url.to_string());
        let files = self.processor.process_source(source).await?;
        info!("Found {} files to process", files.len());

        for file in files {
            // Use the relative path from file info to construct target path
            let target_path = self.base_path.join(&file.relative_path);
            info!("Target path: {}", target_path.display());

            match file.file_type {
                FileType::Xml => {
                    info!("Processing XML: {}", file.relative_path);
                    if let Source::Path(xml_path) = &file.source {
                        let content = tokio::fs::read_to_string(xml_path).await?;
                        let mut parser = DepotParser::new(&content);  // Changed to mut
                        
                        if let Ok(vibs) = parser.parse_vib_files() {
                            for vib in vibs {
                                let vib_source = Source::Http(format!("{}/{}", url, vib.relative_path));
                                info!("Found VIB: {} ({})", vib.relative_path, vib.checksum);
                                
                                // Process each VIB file through the processor
                                let vib_files = self.processor.process_source(vib_source).await?;
                                for vib_file in vib_files {
                                    let mut downloaded = self.downloaded.lock().await;
                                    downloaded.insert(self.base_path.join(&vib_file.relative_path));
                                }
                            }
                        }
                    }
                }
                FileType::Zip => {
                    info!("Processing ZIP: {}", file.relative_path);
                    // ZIP files are already processed by processor.process_source
                    let mut downloaded = self.downloaded.lock().await;
                    downloaded.insert(target_path);
                }
                FileType::Vib => {
                    info!("Processing VIB: {} -> {}", file.relative_path, target_path.display());
                    if (!target_path.exists() || 
                       (file.checksum.is_some() && !self.verifier.verify_checksum(
                            &target_path,
                            file.checksum.as_ref().unwrap(),
                            file.checksum_type.as_ref().unwrap()
                        ).await?)) 
                    {
                        if let Source::Http(url) = file.source {
                            info!("Downloading VIB: {}", url);
                            self.download_file(&url, &target_path).await?;
                        }
                    }
                }
                _ => debug!("Skipping unknown file type: {}", file.relative_path),
            }
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