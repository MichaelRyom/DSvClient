use anyhow::Result;
use std::path::{Path, PathBuf};
use std::collections::{HashSet, HashMap};
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};  // Change to use tokio's Mutex
use log::{info, warn};
use hyper_util::client::legacy::Client;
use hyper_tls::HttpsConnector;
use http_body_util::Empty;
use bytes::Bytes;
use crate::process::{ProcessManager, Source, FileType};
use crate::verify::{self, VerificationManager};

type DownloadTracker = Arc<Mutex<HashSet<(String, String)>>>;  // Updated to tokio Mutex

const MAX_CONCURRENT_DOWNLOADS: usize = 5;

#[derive(Clone)]
pub struct DownloadService {
    download_path: PathBuf,
    client: Client<HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>, Empty<Bytes>>,
    semaphore: Arc<Semaphore>,
    downloaded: DownloadTracker,
    failed_downloads: Arc<Mutex<HashMap<String, (PathBuf, Option<(String, String)>)>>>,
    file_types: Arc<Mutex<HashMap<String, usize>>>,
    processor: Arc<Mutex<ProcessManager>>,
    verification_manager: Arc<VerificationManager>,
}

impl DownloadService {
    pub fn new(download_path: PathBuf, client: Client<HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>, Empty<Bytes>>) -> Self {
        let processor = ProcessManager::new(client.clone(), download_path.clone());
        let verification_manager = Arc::new(VerificationManager::new());

        Self {
            download_path: download_path.clone(),
            client,
            semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_DOWNLOADS)),
            downloaded: Arc::new(Mutex::new(HashSet::new())),
            failed_downloads: Arc::new(Mutex::new(HashMap::new())),
            file_types: Arc::new(Mutex::new(HashMap::new())),
            processor: Arc::new(Mutex::new(processor)),  // Wrap in Arc<Mutex>
            verification_manager,
        }
    }

    async fn is_downloaded(&self, url: &str, path: &PathBuf) -> bool {  // Make async
        let downloaded = self.downloaded.lock().await;  // Change to async lock
        downloaded.contains(&(url.to_string(), path.to_string_lossy().to_string()))
    }

    async fn mark_as_downloaded(&self, url: &str, path: &PathBuf) {  // Make async
        let mut downloaded = self.downloaded.lock().await;  // Change to async lock
        downloaded.insert((url.to_string(), path.to_string_lossy().to_string()));
    }

    async fn add_failed_download(&self, url: String, path: PathBuf, checksum: Option<(String, String)>) {  // Make async
        if !url.contains("404") {
            let mut failed = self.failed_downloads.lock().await;  // Change to async lock
            failed.insert(url, (path, checksum));
        }
    }

    pub async fn retry_failed_downloads(&self) -> Result<()> {
        let failed_downloads = {
            let failed = self.failed_downloads.lock().await;  // Change to async lock
            if failed.is_empty() {
                info!("No failed downloads to retry");
                return Ok(());
            }
            info!("Retrying {} failed downloads...", failed.len());
            failed.clone()
        };

        for (url, (_path, checksum)) in failed_downloads {
            info!("Retrying download: {}", url);
            if let Err(e) = self.download_file(&url, checksum).await {
                warn!("Retry failed for {}: {}", url, e);
            } else {
                let mut failed = self.failed_downloads.lock().await;  // Change to async lock
                failed.remove(&url);
                info!("Successfully retried: {}", url);
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }

        Ok(())
    }

    pub async fn process_sources(&self) -> Result<()> {
        info!("Starting download process using sources file...");
        let mut rdr = csv::Reader::from_reader(include_str!("../sources").as_bytes());
        
        // Create a verification manager for checksums
        let verification_manager = VerificationManager::new();

        for result in rdr.records() {
            let record = result?;
            if record.get(1) == Some("Yes") && record.get(2) == Some("Connected") {
                if let Some(url) = record.get(0) {
                    info!("Processing source: {}", url);
                    let source = Source::Http(url.to_string());
                    let processor = self.processor.lock().await;
                    
                    // First try to process the index file
                    let files = processor.process_source(source.clone()).await?;
                    drop(processor);

                    for file in files {
                        let target_path = self.download_path.join(&file.relative_path);
                        
                        // Store checksum info in a reusable structure
                        let checksum_info = match (&file.checksum, &file.checksum_type) {
                            (Some(checksum), Some(checksum_type)) => Some((checksum.clone(), checksum_type.clone())),
                            _ => None,
                        };
                        
                        // Check if file exists and verify checksum if available
                        if target_path.exists() {
                            info!("Found existing file: {}", target_path.display());
                            if let Some((checksum, checksum_type)) = &checksum_info {
                                match verification_manager.verify_checksum(&target_path, checksum, checksum_type).await {
                                    Ok(true) => {
                                        info!("Checksum verified, skipping download: {}", target_path.display());
                                        self.mark_as_downloaded(&url, &target_path).await;
                                        continue;
                                    }
                                    Ok(false) => {
                                        warn!("Checksum mismatch, will redownload: {}", target_path.display());
                                    }
                                    Err(e) => {
                                        warn!("Checksum verification failed: {}", e);
                                    }
                                }
                            }
                        }

                        // Download the file if needed
                        if let Source::Http(url) = &file.source {
                            let checksum = checksum_info.clone();
                            if let Err(e) = self.download_file(url, checksum.clone()).await {
                                warn!("Failed to download {}: {}", url, e);
                                self.add_failed_download(url.clone(), target_path, checksum).await;
                            } else {
                                info!("Successfully downloaded: {}", target_path.display());
                                
                                // Verify downloaded file
                                if let Some((checksum, checksum_type)) = &checksum_info {
                                    if !verification_manager.verify_checksum(&target_path, checksum, checksum_type).await? {
                                        warn!("Post-download checksum verification failed: {}", target_path.display());
                                        self.add_failed_download(
                                            url.clone(), 
                                            target_path, 
                                            Some((checksum_type.clone(), checksum.clone()))
                                        ).await;
                                        continue;
                                    }
                                }

                                // Process ZIP files for additional content
                                if matches!(file.file_type, FileType::Zip) {
                                    info!("Processing downloaded ZIP file: {}", target_path.display());
                                    let processor = self.processor.lock().await;
                                    if let Ok(zip_files) = processor.process_source(Source::Path(target_path.clone())).await {
                                        drop(processor);
                                        for zip_file in zip_files {
                                            if let Source::Http(url) = zip_file.source {
                                                self.download_file(&url, None).await?;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn download_file(&self, url: &str, checksum: Option<(String, String)>) -> Result<()> {
        let _permit = self.semaphore.clone().acquire_owned().await?;
        let source = Source::Http(url.to_string());
        
        let mut processor = self.processor.lock().await;
        let files = processor.process_source(source).await?;
        drop(processor);
        
        for file in files {
            if let Source::Http(url) = file.source {
                let path = self.download_path.join(&file.relative_path);
                if self.is_downloaded(&url, &path).await {  // Add .await
                    continue;
                }

                let download_source = Source::Http(url.clone());
                let mut processor = self.processor.lock().await;
                if let Err(e) = processor.process_source(download_source).await {
                    self.add_failed_download(url, path, checksum.clone()).await;  // Add .await
                    return Err(e);
                }
                drop(processor);

                if let Some((checksum_type, expected)) = &checksum {
                    if !self.verification_manager.verify_checksum(&path, expected, checksum_type).await? {
                        self.add_failed_download(url, path, Some((checksum_type.clone(), expected.clone()))).await;  // Add .await
                        continue;
                    }
                }

                self.mark_as_downloaded(&url, &path).await;  // Add .await
            }
        }

        Ok(())
    }

    pub async fn get_failed_downloads(&self) -> Vec<String> {  // Make async
        let failed = self.failed_downloads.lock().await;  // Change to async lock
        failed.keys().cloned().collect()
    }

    pub async fn get_file_type_stats(&self) -> HashMap<String, usize> {  // Make async
        let types = self.file_types.lock().await;  // Change to async lock
        types.clone()
    }
}