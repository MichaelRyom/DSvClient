use anyhow::Result;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, AsyncReadExt, BufReader};
use crate::parser::{DepotParser, VibFile};
use std::future::Future;
use std::pin::Pin;
use zip::read::ZipArchive;
use tokio::task;
use tokio::sync::Semaphore;
use std::sync::Arc;
use log::{warn, info};
use std::collections::{HashSet, HashMap};
use std::sync::Mutex;
use sha2::{Sha256, Digest};
use std::time::Duration;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{RetryTransientMiddleware, policies::ExponentialBackoff};
use tokio_retry::{Retry, strategy::FixedInterval};
use reqwest::Client as ReqwestClient;
use reqwest::StatusCode;
use rayon::ThreadPoolBuilder;
use std::io::Read;
use rayon::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;
type DownloadResult = Result<()>;
type DownloadTracker = Arc<Mutex<HashSet<(String, String)>>>;

const CHUNK_SIZE: u64 = 5 * 1024 * 1024; // 5MB chunks
const CHUNKED_THRESHOLD: u64 = 10 * 1024 * 1024; // Only use chunks for files > 10MB
const CHUNK_TIMEOUT: Duration = Duration::from_secs(30); // Timeout for chunk downloads
const MAX_RETRIES: u32 = 5;

#[derive(Clone)]
pub struct DownloadService {
    download_path: PathBuf,
    client: ClientWithMiddleware,
    semaphore: Arc<Semaphore>,
    downloaded: DownloadTracker,
    failed_downloads: Arc<Mutex<HashMap<String, (PathBuf, Option<(String, String)>)>>>,
    file_types: Arc<Mutex<HashMap<String, usize>>>,
    unverifiable_files: Arc<Mutex<HashMap<String, String>>>,
    compute_pool: Arc<rayon::ThreadPool>,
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

        let compute_pool = ThreadPoolBuilder::new()
            .num_threads(num_cpus::get())
            .stack_size(2 * 1024 * 1024)
            .build()
            .unwrap();

        Self {
            download_path,
            client,
            semaphore: Arc::new(Semaphore::new(5)),
            downloaded: Arc::new(Mutex::new(HashSet::new())),
            failed_downloads: Arc::new(Mutex::new(HashMap::new())),
            file_types: Arc::new(Mutex::new(HashMap::new())),
            unverifiable_files: Arc::new(Mutex::new(HashMap::new())),
            compute_pool: Arc::new(compute_pool),
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

    fn add_failed_download(&self, url: String, path: PathBuf, checksum: Option<(String, String)>) {
        if !url.contains("404") {
            let mut failed = self.failed_downloads.lock().unwrap();
            failed.insert(url, (path, checksum));
        }
    }

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
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        let remaining = self.failed_downloads.lock().unwrap();
        if !remaining.is_empty() {
            warn!("Failed downloads after retry:");
            for url in remaining.keys() {
                warn!("  {}", url);
            }
        }

        Ok(())
    }

    pub async fn process_sources(&self) -> Result<()> {
        let mut rdr = csv::Reader::from_reader(include_str!("../sources.csv").as_bytes());

        let mut tasks = Vec::new();

        for result in rdr.records() {
            let record = result?;
            if record.get(1) == Some("Yes") && record.get(2) == Some("Connected") {
                if let Some(url) = record.get(0) {
                    let url = url.to_string();
                    let fut = self.process_depot(url);
                    tasks.push(fut);
                }
            }
        }

        let results = futures::future::join_all(tasks).await;

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
            return Err(anyhow::anyhow!("One or more errors occurred during processing."));
        }

        Ok(())
    }

    async fn save_xml(&self, url: &str, content: &str) -> Result<()> {
        let url_path = url.split("VUM/PRODUCTION/").nth(1).unwrap_or(url);
        let full_path = self.download_path.join(url_path);
        
        self.track_file_type(&full_path);

        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        
        println!("Saving XML: {}", full_path.display());
        tokio::fs::write(full_path, content).await?;
        Ok(())
    }

    async fn process_metadata_zip(&self, zip_url: String, base_url: String) -> DownloadResult {
        info!("Processing metadata archive: {}", zip_url);
        
        let url_path = PathBuf::from(&zip_url);
        self.track_file_type(&url_path);

        self.download_package(zip_url.clone(), None).await?;
        
        let response = self.client.get(&zip_url)
            .send()
            .await?
            .bytes()
            .await?;

        let zip_data = response.to_vec();

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

        let vib_downloads = vib_files.into_iter().map(|vib| {
            let vib_url = format!("{}/{}", base_url, vib.relative_path);
            self.download_package(
                vib_url,
                Some((vib.checksum_type.clone(), vib.checksum.clone()))
            )
        });

        let results = futures::future::join_all(vib_downloads).await;

        for result in results {
            if let Err(e) = result {
                eprintln!("Failed to download a VIB file: {}", e);
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
            .to_string();

        self.save_xml(&url, &content).await?;

        let base_url = url.rsplit_once('/').map(|(dir, _)| dir).unwrap_or(&url).to_string();
        println!("Base URL: {}", base_url);
        let mut parser = DepotParser::new(&content);
        
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
            println!("Processing packages");
            
            let packages = parser.parse_packages()?;
            println!("Found {} regular packages", packages.len());
            
            let client = self.client.clone();
            let download_path = self.download_path.clone();
            let semaphore = self.semaphore.clone();
            let downloaded = self.downloaded.clone();
            
            let mut metadata_tasks: Vec<BoxFuture<DownloadResult>> = Vec::new();
            
            for package in packages {
                let package_url = format!("{}/{}", &base_url, package.url);
                let base_url_clone = base_url.clone();
                
                if package.url.ends_with(".zip") {
                    let service = DownloadService {
                        download_path: download_path.clone(),
                        client: client.clone(),
                        semaphore: semaphore.clone(),
                        downloaded: downloaded.clone(),
                        failed_downloads: self.failed_downloads.clone(),
                        file_types: self.file_types.clone(),
                        unverifiable_files: self.unverifiable_files.clone(),
                        compute_pool: self.compute_pool.clone(),
                    };
                    metadata_tasks.push(Box::pin(async move {
                        service.process_metadata_zip(package_url, base_url_clone).await
                    }));
                } else {
                    let service = DownloadService {
                        download_path: download_path.clone(),
                        client: client.clone(),
                        semaphore: semaphore.clone(),
                        downloaded: downloaded.clone(),
                        failed_downloads: self.failed_downloads.clone(),
                        file_types: self.file_types.clone(),
                        unverifiable_files: self.unverifiable_files.clone(),
                        compute_pool: self.compute_pool.clone(),
                    };
                    metadata_tasks.push(Box::pin(async move {
                        service.download_package(package_url, None).await
                    }));
                }
            }

            if let Ok(addon_metadata) = parser.parse_addon_metadata() {
                println!("Found {} addon metadata entries", addon_metadata.len());
                for metadata in addon_metadata {
                    let zip_url = format!("{}/{}", &base_url, metadata.url);
                    let base_url_clone = base_url.clone();
                    let service = DownloadService {
                        download_path: download_path.clone(),
                        client: client.clone(),
                        semaphore: semaphore.clone(),
                        downloaded: downloaded.clone(),
                        failed_downloads: self.failed_downloads.clone(),
                        file_types: self.file_types.clone(),
                        unverifiable_files: self.unverifiable_files.clone(),
                        compute_pool: self.compute_pool.clone(),
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

        self.track_file_type(&full_path);

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

        if self.is_downloaded(&url, &full_path) {
            info!("Skipping already downloaded: {}", url);
            return Ok(());
        }

        let permit = self.semaphore.clone().acquire_owned().await?;

        println!("Starting download for: {}", url);
        
        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        if !full_path.starts_with(&self.download_path) {
            return Err(anyhow::anyhow!("Invalid path"));
        }

        let result = self.download_with_retry(&url, &full_path).await;

        drop(permit);

        match result {
            Ok(_) => {
                if let Ok(metadata) = tokio::fs::metadata(&full_path).await {
                    if metadata.len() > 0 {
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
                let _ = tokio::fs::remove_file(&full_path).await;
                if let Some(reqwest_error) = e.downcast_ref::<reqwest::Error>() {
                    if let Some(status) = reqwest_error.status() {
                        if status == StatusCode::NOT_FOUND {
                            warn!("File not found (404): {}", url);
                            return Ok(());
                        }
                    }
                }
                self.add_failed_download(url.clone(), full_path.clone(), expected_checksum);
                eprintln!("Failed to download {}: {}", url, e);
                Err(e)
            }
        }
    }

    async fn verify_checksums(path: impl AsRef<Path>, checksums: &[(String, String)]) -> Result<bool> {
        let file = tokio::fs::File::open(&path).await?;
        let mut reader = BufReader::new(file);
        let mut buffer = vec![0; 64 * 1024];
        
        let mut hashers: Vec<(String, Sha256)> = checksums
            .iter()
            .filter(|(type_, _)| type_.to_lowercase() == "sha-256")
            .map(|(_, checksum)| (checksum.to_string(), Sha256::new()))
            .collect();

        loop {
            let n = reader.read(&mut buffer).await?;
            if n == 0 { break; }
            for (_, hasher) in hashers.iter_mut() {
                hasher.update(&buffer[..n]);
            }
        }

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
                if status == StatusCode::NOT_FOUND {
                    return false;
                }
                if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
                    return true;
                }
            }
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

    async fn verify_file_streaming(&self, path: &PathBuf, expected_checksum: &str, checksum_type: &str) -> Result<bool> {
        if checksum_type.to_lowercase() != "sha-256" {
            warn!("Unsupported checksum type: {}", checksum_type);
            return Ok(false);
        }

        let path = path.clone();
        let expected = expected_checksum.to_string();
        
        let result = tokio::task::spawn_blocking(move || {
            let mut hasher = Sha256::new();
            let file = std::fs::File::open(&path)?;
            
            let mut buffer = vec![0; 1024 * 1024];
            let mut reader = std::io::BufReader::with_capacity(1024 * 1024, file);
            
            loop {
                match reader.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(n) => hasher.update(&buffer[..n]),
                    Err(e) => return Err(anyhow::anyhow!("Read error: {}", e)),
                }
            }
            
            Ok(format!("{:x}", hasher.finalize()) == expected.to_lowercase())
        }).await??;

        Ok(result)
    }

    pub fn get_failed_downloads(&self) -> Vec<String> {
        let failed = self.failed_downloads.lock().unwrap();
        failed.keys().cloned().collect()
    }

    pub async fn verify_downloads(&self) -> Result<()> {
        // Delegate to the verify module
        let report = crate::verify::verify_directory(&self.download_path).await?;
        report.print_summary();
        Ok(())
    }

    async fn get_metadata_files(&self) -> Result<Vec<PathBuf>> {
        let mut index_files: Vec<PathBuf> = Vec::new();
        info!("Scanning directory for index files: {}", self.download_path.display());
        
        // Implement directory scanning here instead of using verify module
        let mut stack = vec![self.download_path.clone()];
        while let Some(dir) = stack.pop() {
            let mut entries = tokio::fs::read_dir(&dir).await?;
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                if path.is_file() {
                    if path.to_string_lossy().contains("index.xml") {
                        index_files.push(path);
                    }
                } else if path.is_dir() {
                    stack.push(path);
                }
            }
        }
        
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

    fn track_file_type(&self, path: &Path) {
        if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
            let mut stats = self.file_types.lock().unwrap();
            *stats.entry(ext.to_lowercase()).or_insert(0) += 1;
        }
    }

    pub fn get_file_type_stats(&self) -> HashMap<String, usize> {
        self.file_types.lock().unwrap().clone()
    }

    fn track_unverifiable_file(&self, path: &Path, reason: &str) {
        let mut unverifiable = self.unverifiable_files.lock().unwrap();
        unverifiable.insert(path.to_string_lossy().to_string(), reason.to_string());
    }
}