use anyhow::Result;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, AsyncReadExt, BufReader}; // Updated import
use crate::parser::{DepotParser, VibFile}; // Updated import
use std::future::Future;
use std::pin::Pin;
use zip::read::ZipArchive;
use tokio::task;
use tokio::sync::Semaphore;
use std::sync::Arc;
use log::{warn, info}; // Remove unused error import
use std::collections::{HashSet, HashMap};
use std::sync::Mutex;
use sha2::{Sha256, Digest};
use reqwest::header::HeaderMap;
use std::time::Duration;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware}; // Added ClientWithMiddleware
use reqwest_retry::{RetryTransientMiddleware, policies::ExponentialBackoff};
use tokio_retry::{Retry, strategy::FixedInterval};  // Replace retry imports
use reqwest::Client as ReqwestClient;
use reqwest::StatusCode;

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;
type DownloadResult = Result<()>;
type DownloadTracker = Arc<Mutex<HashSet<(String, String)>>>;

const CHUNK_SIZE: u64 = 5 * 1024 * 1024; // 5MB chunks
const CHUNKED_THRESHOLD: u64 = 10 * 1024 * 1024; // Only use chunks for files > 10MB
const CHUNK_TIMEOUT: Duration = Duration::from_secs(30); // Timeout for chunk downloads
const MAX_RETRIES: u32 = 5;

pub struct DownloadService {
    download_path: PathBuf,
    client: ClientWithMiddleware,  // Change from reqwest::Client to ClientWithMiddleware
    semaphore: Arc<Semaphore>, // Add semaphore
    downloaded: DownloadTracker, // Add tracker
    failed_downloads: Arc<Mutex<HashMap<String, (PathBuf, Option<(String, String)>)>>>, // Track failed downloads
}

impl DownloadService {
    pub fn new(download_path: PathBuf) -> Self {
        let retry_policy = RetryTransientMiddleware::new_with_policy(
            ExponentialBackoff::builder().build_with_max_retries(5)
        );
        
        let client = ClientBuilder::new(ReqwestClient::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to build reqwest client"))
            .with(retry_policy)
            .build();

        Self {
            download_path,
            client,
            semaphore: Arc::new(Semaphore::new(5)),
            downloaded: Arc::new(Mutex::new(HashSet::new())),
            failed_downloads: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn is_downloaded(&self, url: &str, path: &PathBuf) -> bool {
        let downloaded = self.downloaded.lock().unwrap();
        downloaded.contains(&(url.to_string(), path.to_string_lossy().to_string()))
    }

    fn mark_as_downloaded(&self, url: &str, path: &PathBuf) {
        let mut downloaded = self.downloaded.lock().unwrap();
        downloaded.insert((url.to_string(), path.to_string_lossy().to_string()));
    }

    // Add method to track failed downloads
    fn add_failed_download(&self, url: String, path: PathBuf, checksum: Option<(String, String)>) {
        if !url.contains("404") { // Don't track 404s
            let mut failed = self.failed_downloads.lock().unwrap();
            failed.insert(url, (path, checksum));
        }
    }

    // Add method to retry failed downloads
    pub async fn retry_failed_downloads(&self) -> Result<()> {
        let failed_downloads = {
            let failed = self.failed_downloads.lock().unwrap();
            if failed.is_empty() {
                info!("No failed downloads to retry");
                return Ok(());
            }
            info!("Retrying {} failed downloads...", failed.len());
            failed.clone()
        };

        for (url, (_path, checksum)) in failed_downloads {
            info!("Retrying download: {}", url);
            if let Err(e) = self.download_package(url.clone(), checksum).await {
                warn!("Retry failed for {}: {}", url, e);
            } else {
                let mut failed = self.failed_downloads.lock().unwrap();
                failed.remove(&url);
                info!("Successfully retried: {}", url);
            }
            // Add a small delay between retries
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        // Report any remaining failed downloads
        let remaining = self.failed_downloads.lock().unwrap();
        if (!remaining.is_empty()) {
            warn!("Failed downloads after retry:");
            for url in remaining.keys() {
                warn!("  {}", url);
            }
        }

        Ok(())
    }

    pub async fn process_sources(&self) -> Result<()> {
        let mut rdr = csv::Reader::from_reader(include_str!("../sources.csv").as_bytes());

        // Collect depot processing tasks
        let mut tasks = Vec::new();

        for result in rdr.records() {
            let record = result?;
            if record.get(1) == Some("Yes") && record.get(2) == Some("Connected") {
                if let Some(url) = record.get(0) {
                    let url = url.to_string(); // Clone the URL to own it
                    let fut = self.process_depot(url);
                    tasks.push(fut);
                }
            }
        }

        // Process depots concurrently
        let results = futures::future::join_all(tasks).await;

        // Collect and report errors
        let mut errors = Vec::new();
        for result in results {
            if let Err(e) = result {
                errors.push(e);
            }
        }

        if !errors.is_empty() {
            eprintln!("Errors occurred during processing:");
            for error in errors {
                eprintln!("{}", error);
            }
            // Return an error or decide to continue based on your requirements
            // For now, we'll return an error
            return Err(anyhow::anyhow!("One or more errors occurred during processing."));
        }

        Ok(())
    }

    async fn save_xml(&self, url: &str, content: &str) -> Result<()> {
        let url_path = url.split("VUM/PRODUCTION/").nth(1).unwrap_or(url);
        let full_path = self.download_path.join(url_path);
        
        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        
        println!("Saving XML: {}", full_path.display());
        tokio::fs::write(full_path, content).await?;
        Ok(())
    }

    async fn process_metadata_zip(&self, zip_url: String, base_url: String) -> DownloadResult {
        info!("Processing metadata archive: {}", zip_url);
        
        // Download the zip file
        self.download_package(zip_url.clone(), None).await?;
        
        // Get the zip data
        let response = self.client.get(&zip_url)
            .send()
            .await?
            .bytes()
            .await?;

        let zip_data = response.to_vec();

        // Use spawn_blocking to handle blocking ZIP operations
        let vib_files = task::spawn_blocking(move || -> Result<Vec<VibFile>> {
            let reader = std::io::Cursor::new(zip_data);
            let mut archive = ZipArchive::new(reader)?;
            let mut vib_files = Vec::new();

            for i in 0..archive.len() {
                let mut file = archive.by_index(i)?;
                if file.name().ends_with("vmware.xml") {
                    info!("Found metadata file in archive:");
                    info!("  Archive: {}", file.name());
                    info!("  Size: {} bytes", file.size());
                    
                    // Format the modification date
                    let modified = file.last_modified()
                        .map(|dt| format!("{:02}/{:02}/{} {:02}:{:02}",
                            dt.month(), dt.day(), dt.year(), dt.hour(), dt.minute()))
                        .unwrap_or_else(|| "Unknown date".to_string());
                    
                    info!("  Modified: {}", modified);
                    
                    let mut contents = String::new();
                    use std::io::Read;
                    file.read_to_string(&mut contents)?;
                    
                    let mut parser = DepotParser::new(&contents);
                    vib_files = parser.parse_vib_files()?;
                    break;
                }
            }

            Ok(vib_files)
        }).await??;

        info!("Found {} VIB files to process", vib_files.len());

        // Download VIB files concurrently
        let vib_downloads = vib_files.into_iter().map(|vib| {
            let vib_url = format!("{}/{}", base_url, vib.relative_path);
            self.download_package(
                vib_url,
                Some((vib.checksum_type.clone(), vib.checksum.clone()))
            )
        });

        // Execute all VIB downloads concurrently
        let results = futures::future::join_all(vib_downloads).await;

        // Handle errors
        for result in results {
            if let Err(e) = result {
                eprintln!("Failed to download a VIB file: {}", e);
                // Decide whether to continue or return an error
            }
        }

        Ok(())
    }

    pub async fn process_depot(&self, url: String) -> Result<()> {
        println!("\n=== Processing URL ===");
        println!("Input URL: {}", url);
        
        let content = self.client.get(&url)
            .send()
            .await?
            .text()
            .await?
            .to_string(); // Convert to owned String

        self.save_xml(&url, &content).await?;

        let base_url = url.rsplit_once('/').map(|(dir, _)| dir).unwrap_or(&url).to_string();
        println!("Base URL: {}", base_url);
        let mut parser = DepotParser::new(&content);
        
        // Process vmw-depot-index.xml files
        if url.contains("vmw-depot-index.xml") {
            println!("Processing vendor list");
            let vendors = parser.parse_vendors()?;
            println!("Found {} vendors", vendors.len());
            
            let vendor_tasks = vendors.into_iter().map(|vendor| {
                let vendor_url = format!("{}/{}/{}", base_url, vendor.relative_path, vendor.index_file);
                self.process_depot(vendor_url)
            });

            let results = futures::future::join_all(vendor_tasks).await;
            for result in results {
                if let Err(e) = result {
                    eprintln!("Failed to process vendor depot: {}", e);
                }
            }
        } else {
            // Process both regular packages and addon packages for metadata
            println!("Processing packages");
            
            let packages = parser.parse_packages()?;
            println!("Found {} regular packages", packages.len());
            
            let client = self.client.clone();
            let download_path = self.download_path.clone();
            let semaphore = self.semaphore.clone();
            let downloaded = self.downloaded.clone();
            
            let mut metadata_tasks: Vec<BoxFuture<DownloadResult>> = Vec::new();
            
            // Check regular packages for metadata zip files
            for package in packages {
                let package_url = format!("{}/{}", &base_url, package.url);
                let base_url_clone = base_url.clone();
                
                if package.url.ends_with(".zip") {
                    let service = DownloadService {
                        download_path: download_path.clone(),
                        client: client.clone(),
                        semaphore: semaphore.clone(),
                        downloaded: downloaded.clone(), // Include tracker
                        failed_downloads: self.failed_downloads.clone(), // Include failed downloads tracker
                    };
                    metadata_tasks.push(Box::pin(async move {
                        service.process_metadata_zip(package_url, base_url_clone).await
                    }));
                } else {
                    let service = DownloadService {
                        download_path: download_path.clone(),
                        client: client.clone(),
                        semaphore: semaphore.clone(),
                        downloaded: downloaded.clone(), // Include tracker
                        failed_downloads: self.failed_downloads.clone(), // Include failed downloads tracker
                    };
                    metadata_tasks.push(Box::pin(async move {
                        service.download_package(package_url, None).await
                    }));
                }
            }

            // Also try parsing as addon metadata
            if let Ok(addon_metadata) = parser.parse_addon_metadata() {
                println!("Found {} addon metadata entries", addon_metadata.len());
                for metadata in addon_metadata {
                    let zip_url = format!("{}/{}", &base_url, metadata.url);
                    let base_url_clone = base_url.clone();
                    let service = DownloadService {
                        download_path: download_path.clone(),
                        client: client.clone(),
                        semaphore: semaphore.clone(),
                        downloaded: downloaded.clone(), // Include tracker
                        failed_downloads: self.failed_downloads.clone(), // Include failed downloads tracker
                    };
                    
                    if metadata.url.ends_with(".zip") {
                        metadata_tasks.push(Box::pin(async move {
                            service.process_metadata_zip(zip_url, base_url_clone).await
                        }));
                    } else {
                        metadata_tasks.push(Box::pin(async move {
                            service.download_package(zip_url, None).await
                        }));
                    }
                }
            }

            // Execute all tasks concurrently
            let results = futures::future::join_all(metadata_tasks).await;
            for result in results {
                if let Err(e) = result {
                    eprintln!("Failed to process/download file: {}", e);
                }
            }
        }
        
        Ok(())
    }

    async fn download_package(&self, url: String, expected_checksum: Option<(String, String)>) -> DownloadResult {
        let url_path = url.split("VUM/PRODUCTION/").nth(1).unwrap_or(&url);
        let full_path = self.download_path.join(url_path);

        // First check if file exists and matches checksums
        if full_path.exists() {
            if let Some((checksum_type, checksum)) = &expected_checksum {
                let checksums = vec![(checksum_type.clone(), checksum.clone())];
                match Self::verify_checksums(&full_path, &checksums).await {
                    Ok(true) => {
                        info!("File exists and checksums match, skipping: {}", full_path.display());
                        self.mark_as_downloaded(&url, &full_path);
                        return Ok(());
                    }
                    Ok(false) => {
                        info!("File exists but checksum mismatch, redownloading: {}", full_path.display());
                    }
                    Err(e) => {
                        warn!("Failed to verify checksums for {}: {}", full_path.display(), e);
                    }
                }
            }
        }

        // Check download tracker
        if self.is_downloaded(&url, &full_path) {
            info!("Skipping already downloaded: {}", url);
            return Ok(());
        }

        // Acquire a permit from the semaphore
        let permit = self.semaphore.clone().acquire_owned().await?;

        println!("Starting download for: {}", url);
        
        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        if !full_path.starts_with(&self.download_path) {
            return Err(anyhow::anyhow!("Invalid path"));
        }

        let result = self.download_with_retry(&url, &full_path).await;

        // Release the semaphore permit
        drop(permit);

        match result {
            Ok(_) => {
                // Verify file exists and has size > 0
                if let Ok(metadata) = tokio::fs::metadata(&full_path).await {
                    if (metadata.len() > 0) {
                        info!("Downloaded: {} ({} bytes)", full_path.display(), metadata.len());
                        self.mark_as_downloaded(&url, &full_path);
                        Ok(())
                    } else {
                        let _ = tokio::fs::remove_file(&full_path).await;
                        Err(anyhow::anyhow!("Downloaded file is empty"))
                    }
                } else {
                    Err(anyhow::anyhow!("Failed to verify downloaded file"))
                }
            }
            Err(e) => {
                // Clean up any partially downloaded file
                let _ = tokio::fs::remove_file(&full_path).await;
                if let Some(reqwest_error) = e.downcast_ref::<reqwest::Error>() {
                    if let Some(status) = reqwest_error.status() {
                        if status == StatusCode::NOT_FOUND {
                            warn!("File not found (404): {}", url);
                            return Ok(());  // Continue processing other files
                        }
                    }
                }
                // Track the failed download
                self.add_failed_download(url.clone(), full_path.clone(), expected_checksum);
                eprintln!("Failed to download {}: {}", url, e);
                Err(e)
            }
        }
    }

    async fn verify_checksums(path: impl AsRef<Path>, checksums: &[(String, String)]) -> Result<bool> {
        // Open file once for all checksums
        let file = tokio::fs::File::open(&path).await?;
        let mut reader = BufReader::new(file);
        let mut buffer = vec![0; 64 * 1024]; // 64KB buffer
        
        // Create a hasher for each checksum we need to verify
        let mut hashers: Vec<(String, Sha256)> = checksums
            .iter()
            .filter(|(type_, _)| type_.to_lowercase() == "sha-256")
            .map(|(_, checksum)| (checksum.to_string(), Sha256::new()))
            .collect();

        // Read file in chunks and update all hashers
        loop {
            let n = reader.read(&mut buffer).await?;
            if n == 0 { break; }
            for (_, hasher) in hashers.iter_mut() {
                hasher.update(&buffer[..n]);
            }
        }

        // Compare all checksums
        for (expected, hasher) in hashers {
            let result = format!("{:x}", hasher.finalize());
            if result != expected.to_lowercase() {
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn should_retry(error: &anyhow::Error) -> bool {
        if let Some(reqwest_error) = error.downcast_ref::<reqwest::Error>() {
            if let Some(status) = reqwest_error.status() {
                // Don't retry 404s
                if status == StatusCode::NOT_FOUND {
                    return false;
                }
                // Retry other server errors and rate limits
                if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
                    return true;
                }
            }
            // Retry connection errors and timeouts
            reqwest_error.is_connect() || reqwest_error.is_timeout()
        } else {
            false
        }
    }

    async fn download_with_retry(&self, url: &str, full_path: &PathBuf) -> Result<()> {
        let retry_strategy = FixedInterval::new(Duration::from_secs(1))
            .take(3);
        
        Retry::spawn(retry_strategy, || async {
            let response = self.client.get(url)
                .send()
                .await
                .map_err(|e| {
                    // Return 404 errors immediately without retry
                    if let Some(error) = e.status() {
                        if error == StatusCode::NOT_FOUND {
                            warn!("File not found (404): {}", url);
                            return e;
                        }
                    }
                    warn!("Request failed: {}", e);
                    e
                })?;

            let status = response.status();
            let headers = response.headers().clone();
            let content_length = response.content_length();

            info!("Response info for {}:", url);
            info!("Status: {}", status);
            info!("Content-Length: {:?}", content_length);
            for (name, value) in headers.iter() {
                if let Ok(value_str) = value.to_str() {
                    info!("{}: {}", name, value_str);
                }
            }

            if !status.is_success() {
                // Return 404 errors immediately
                if status == StatusCode::NOT_FOUND {
                    warn!("File not found (404): {}", url);
                    return Err(anyhow::anyhow!("HTTP 404: File not found"));
                }
                let text = response.text().await.unwrap_or_default();
                warn!("Server returned error status: {} with body: {}", status, text);
                return Err(anyhow::anyhow!("HTTP error: {}", status));
            }

            let total_size = content_length
                .ok_or_else(|| anyhow::anyhow!("Content length not available"))?;

            let mut file = File::create(full_path).await?;
            let content = match response.bytes().await {
                Ok(bytes) => bytes,
                Err(e) => {
                    warn!("Error decoding response body for {}:", url);
                    warn!("Error details: {:#?}", e);
                    warn!("Response status: {}", status);
                    warn!("Response headers: {:#?}", headers);
                    return Err(anyhow::anyhow!("Failed to decode response body: {}", e));
                }
            };

            let downloaded = content.len() as u64;

            file.write_all(&content).await?;

            if downloaded != total_size {
                let _ = tokio::fs::remove_file(full_path).await;
                return Err(anyhow::anyhow!(
                    "Size mismatch: expected {} bytes, got {} bytes",
                    total_size, downloaded
                ));
            }

            info!("Downloaded: {}/{} bytes (100%)", downloaded, total_size);
            Ok(())
        }).await
    }

    // Add verify_file_streaming method
    async fn verify_file_streaming(&self, path: &PathBuf, expected_checksum: &str, checksum_type: &str) -> Result<bool> {
        if checksum_type.to_lowercase() != "sha-256" {
            warn!("Unsupported checksum type: {}", checksum_type);
            return Ok(false);
        }

        let file = File::open(path).await?;
        let mut reader = BufReader::with_capacity(8192, file);
        let mut hasher = Sha256::new();
        let mut buffer = vec![0; 8192];

        loop {
            let bytes_read = reader.read(&mut buffer).await?;
            if bytes_read == 0 { break; }
            hasher.update(&buffer[..bytes_read]);
        }

        let result = format!("{:x}", hasher.finalize());
        Ok(result == expected_checksum.to_lowercase())
    }

    // Method to get a list of failed downloads
    pub fn get_failed_downloads(&self) -> Vec<String> {
        let failed = self.failed_downloads.lock().unwrap();
        failed.keys().cloned().collect()
    }

    // Method to verify downloaded files
    pub async fn verify_downloads(&self) -> Result<(usize, usize, usize)> {
        info!("Starting verification process...");
        info!("Scanning directory: {}", self.download_path.display());
        
        let index_files = self.get_metadata_files().await?;
        if index_files.is_empty() {
            warn!("No index files found (looking for *-index.xml files)");
            return Ok((0, 0, 0));
        }

        let mut total_checked = 0;
        let mut total_missing = 0;
        let mut total_wrong_checksum = 0;

        // First, process all index files to find zip files
        for index_path in &index_files {
            info!("Processing index file: {}", index_path.display());
            let content = tokio::fs::read_to_string(index_path).await?;
            let mut parser = DepotParser::new(&content);

            // Get the directory containing the index file
            let index_dir = index_path.parent()
                .ok_or_else(|| anyhow::anyhow!("Cannot get parent directory of index file"))?;

            // Parse addon metadata to get zip files
            let addon_packages = parser.parse_addon_metadata()?;
            info!("Found {} addon packages in index", addon_packages.len());

            // Process each zip file
            for package in addon_packages {
                let zip_path = index_dir.join(&package.url);
                info!("Processing zip file: {}", zip_path.display());
                
                if !zip_path.exists() {
                    warn!("Missing metadata zip: {}", zip_path.display());
                    continue;
                }

                // Get the directory containing the zip file to use as base path
                let zip_dir = zip_path.parent()
                    .ok_or_else(|| anyhow::anyhow!("Cannot get parent directory of zip file"))?;

                // Use spawn_blocking to handle zip operations with local file
                let zip_path_clone = zip_path.clone();
                let vib_files = task::spawn_blocking(move || -> Result<Vec<VibFile>> {
                    let file = std::fs::File::open(&zip_path_clone)?;
                    let mut archive = ZipArchive::new(file)?;
                    let mut vib_files = Vec::new();

                    for i in 0..archive.len() {
                        let mut file = archive.by_index(i)?;
                        if file.name().ends_with("vmware.xml") {
                            info!("Found VIB metadata in zip: {}", file.name());
                            let mut contents = String::new();
                            use std::io::Read;
                            file.read_to_string(&mut contents)?;
                            
                            let mut parser = DepotParser::new(&contents);
                            vib_files = parser.parse_vib_files()?;
                            break;
                        }
                    }
                    Ok(vib_files)
                }).await??;

                // Verify each VIB file
                for vib in vib_files {
                    total_checked += 1;
                    // Use zip directory as base path for VIB files
                    let file_path = zip_dir.join(&vib.relative_path);
                    info!("Checking VIB file: {}", file_path.display());
                    
                    if !file_path.exists() {
                        total_missing += 1;
                        warn!("Missing VIB file: {}", file_path.display());
                        continue;
                    }

                    match self.verify_file_streaming(&file_path, &vib.checksum, &vib.checksum_type).await {
                        Ok(true) => info!("Checksum verified: {}", file_path.display()),
                        Ok(false) => {
                            total_wrong_checksum += 1;
                            warn!("Checksum mismatch for file: {}", file_path.display());
                        },
                        Err(e) => {
                            total_wrong_checksum += 1;
                            warn!("Failed to verify checksum for {}: {}", file_path.display(), e);
                        }
                    }
                }
            }
        }

        info!("\nVerification completed:");
        info!("Files Checked: {}", total_checked);
        info!("Files Missing: {}", total_missing);
        info!("Files With Wrong Checksum: {}", total_wrong_checksum);

        Ok((total_checked, total_missing, total_wrong_checksum))
    }

    // Update to only look for index files
    async fn get_metadata_files(&self) -> Result<Vec<PathBuf>> {
        let mut index_files = Vec::new();
        info!("Scanning directory for index files: {}", self.download_path.display());
        self.scan_directory_for_xml(&self.download_path, &mut index_files).await?;
        
        if index_files.is_empty() {
            warn!("No index files found in {}", self.download_path.display());
        } else {
            info!("Found {} index files", index_files.len());
            for file in &index_files {
                info!("  {}", file.display());
            }
        }
        
        Ok(index_files)
    }

    async fn scan_directory_for_xml<'a>(&'a self, dir: &'a Path, files: &'a mut Vec<PathBuf>) -> Result<()> {
        info!("Scanning directory: {}", dir.display());
        let mut entries = tokio::fs::read_dir(dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_dir() {
                Box::pin(self.scan_directory_for_xml(&path, files)).await?;
            } else if path.extension().and_then(|s| s.to_str()) == Some("xml") {
                // Only look for index files
                if path.to_string_lossy().contains("-index.xml") {
                    info!("Found index file: {}", path.display());
                    files.push(path);
                }
            }
        }
        Ok(())
    }
}