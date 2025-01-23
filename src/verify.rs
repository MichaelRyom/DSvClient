use anyhow::Result;
use log::{info, warn, error};
use sha2::{Sha256, Digest};
use std::path::{Path, PathBuf};
use std::collections::HashSet;
use tokio::fs;
//use tokio::io::AsyncReadExt;
use crate::process::{ProcessManager, Source, FileType};
use hyper_util::client::legacy::Client;
use hyper_tls::HttpsConnector;
use http_body_util::Empty;
use bytes::Bytes;
use tokio::sync::Semaphore;
use std::sync::Arc;
//use rayon::ThreadPoolBuilder;
use tokio::task;
use tokio::sync::Mutex as TokioMutex;
use std::collections::HashMap;
use crate::config::AppConfig;  // Use renamed import
use tokio::sync::Mutex;
use log::debug;


// Increase parallelism and optimize buffer sizes
//const VERIFICATION_CHUNK_SIZE: usize = 256 * 1024; // 256KB is optimal for most filesystems
//const MAX_CONCURRENT_FILES: usize = 1000; // Process many files simultaneously
//const THREAD_SLEEP_MS: u64 = 0; // Remove artificial delay

#[derive(Debug, Default, Clone)]
pub struct VerificationReport {
    pub files_checked: usize,
    pub vib_files_missing: Vec<PathBuf>,
    pub checksum_mismatches: Vec<(PathBuf, String)>,
    pub error_files: Vec<(PathBuf, String)>,
    pub processed_xmls: HashSet<PathBuf>,
    pub processed_zips: HashSet<PathBuf>,
}

impl VerificationReport {
    pub fn print_summary(&self) {
        info!("\nVerification Summary:");
        
        if !self.vib_files_missing.is_empty() {
            warn!("\nMissing VIB Files ({}):", self.vib_files_missing.len());
            for path in &self.vib_files_missing {
                warn!("  {}", path.display());
            }
        }

        if !self.checksum_mismatches.is_empty() {
            warn!("\nChecksum Mismatches ({}):", self.checksum_mismatches.len());
            for (path, expected) in &self.checksum_mismatches {
                warn!("Checksum mismatch for {} Expected: {}", path.display(), expected);
            }
        }

        if !self.error_files.is_empty() {
            error!("\nFiles with Errors ({}):", self.error_files.len());
            for (path, error) in &self.error_files {
                error!("  {}: {}", path.display(), error);
            }
        }

        /*
        info!("XML files processed: {}", self.processed_xmls.len());
        info!("ZIP files processed: {}", self.processed_zips.len());
        info!("Total VIB files checked: {}", self.files_checked);
        info!("Files missing on disk: {}", self.vib_files_missing.len());  // Add this line
        info!("Total VIB files missing: {}", self.vib_files_missing.len());
        info!("Files with checksum mismatches: {}", self.checksum_mismatches.len());
        info!("Files with errors: {}", self.error_files.len());
        */

        info!("XML files processed: {}", self.processed_xmls.len());
        info!("ZIP files processed: {}", self.processed_zips.len());
        info!("Files on disk checked: {}", self.files_checked);
        info!("Errors:");
        info!("  - Access errors: {}", self.error_files.len());
        info!("  - Checksum mismatches: {}", self.checksum_mismatches.len());
        info!("  - Files missing on disk: {}", self.vib_files_missing.len());
    }

    fn add_error(&mut self, path: PathBuf, error: String) {
        error!("Error processing {}: {}", path.display(), error);
        self.error_files.push((path, error));
    }
}

#[derive(Clone)]  // Add Clone derive
pub struct AsyncReport {
    inner: Arc<TokioMutex<VerificationReport>>,  // Change to Arc
}

impl AsyncReport {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(TokioMutex::new(VerificationReport::default())),
        }
    }

    async fn add_xml(&self, path: PathBuf) {
        info!("Processing XML: {}", path.display());
        let mut report = self.inner.lock().await;
        report.processed_xmls.insert(path);
    }

    async fn add_zip(&self, path: PathBuf) {
        let mut report = self.inner.lock().await;
        report.processed_zips.insert(path);
    }

    async fn increment_checked(&self) {
        let mut report = self.inner.lock().await;
        report.files_checked += 1;
    }

    async fn add_missing(&self, path: PathBuf) {
        let mut report = self.inner.lock().await;
        report.vib_files_missing.push(path.clone());
        warn!("Missing VIB file: {}", path.display());
    }

    async fn add_mismatch(&self, path: PathBuf, checksum: String) {
        let mut report = self.inner.lock().await;
        report.checksum_mismatches.push((path, checksum));
    }

    async fn add_error(&self, path: PathBuf, error: String) {
        let mut report = self.inner.lock().await;
        report.add_error(path, error);
    }

    async fn into_inner(self) -> VerificationReport {
        match Arc::try_unwrap(self.inner) {
            Ok(mutex) => mutex.into_inner(),
            Err(arc) => arc.lock().await.clone()
        }
    }
}

#[derive(Clone)]  // Add Clone derive
pub struct VerificationManager {
    //thread_pool: Arc<rayon::ThreadPool>,
    semaphore: Arc<Semaphore>,
    config: Arc<AppConfig>,  // Use AppConfig instead of Config
    verified_files: Arc<Mutex<HashSet<PathBuf>>>,
    metadata_cache: Arc<Mutex<HashMap<PathBuf, (String, String)>>>, // Cache for (checksum, checksum_type)
}

impl VerificationManager {
    pub fn new(config: AppConfig) -> Self {
        Self::new_with_concurrency(config.verification.max_concurrent_verifications, config)
    }

    pub fn new_with_concurrency(concurrent_verifications: usize, config: AppConfig) -> Self {
        /* let thread_pool = ThreadPoolBuilder::new()
            .num_threads(concurrent_verifications)
            .build()
            .unwrap(); */

        Self {
            //thread_pool: Arc::new(thread_pool),
            semaphore: Arc::new(Semaphore::new(concurrent_verifications)),
            config: Arc::new(config),  // Initialize config
            verified_files: Arc::new(Mutex::new(HashSet::new())),
            metadata_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    
    pub async fn verify_checksum(&self, path: &Path, expected: &str, checksum_type: &str) -> Result<bool> {
        // Check if already verified
        {
            let verified = self.verified_files.lock().await;
            if verified.contains(path) {
                debug!("Already verified: {}", path.display());
                return Ok(true);
            }
        }

        // Do verification
        let result = self.do_verify_checksum(path, expected, checksum_type).await?;
        
        // Track successful verifications
        if result {
            let mut verified = self.verified_files.lock().await;
            verified.insert(path.to_path_buf());
        }

        Ok(result)
    }

    async fn do_verify_checksum(&self, path: &Path, expected: &str, checksum_type: &str) -> Result<bool> {
        if checksum_type.to_lowercase() != "sha-256" {
            return Ok(false);
        }

        let _permit = self.semaphore.acquire().await?;
        let path_for_closure = path.to_path_buf();
        let expected = expected.to_string();
        let chunk_size = self.config.verification.chunk_size;  // Use config

        let result = task::spawn_blocking(move || -> Result<bool> {
            use std::fs::File;
            use std::io::{BufReader, Read};
            
            let file = File::open(&path_for_closure)?;
            let mut reader = BufReader::with_capacity(chunk_size, file);  // Use config
            let mut hasher = Sha256::new();
            let mut buffer = vec![0; chunk_size];  // Use config

            while let Ok(n) = reader.read(&mut buffer) {
                if n == 0 { break; }
                hasher.update(&buffer[..n]);
            }

            Ok(format!("{:x}", hasher.finalize()) == expected.to_lowercase())
        }).await?;

        result
    }

    pub async fn cache_vib_info(&self, path: PathBuf, checksum: String, checksum_type: String) {
        let mut cache = self.metadata_cache.lock().await;
        cache.insert(path, (checksum, checksum_type));
    }

    pub async fn get_cached_metadata(&self, path: &Path) -> Option<(String, String)> {
        let cache = self.metadata_cache.lock().await;
        cache.get(path).cloned()
    }
}

// Update function signature to accept Path
pub async fn verify_directory(source: &Path) -> Result<VerificationReport> {
    let config = AppConfig::load_or_default();
    let report = Arc::new(AsyncReport::new());  // Wrap in Arc
    let verifier = VerificationManager::new(config);

    // Check if source is a path or URL
    if let Some(s) = source.to_str() {
        if s.starts_with("http://") || s.starts_with("https://") {
            // Handle URL verification
            let client = Client::builder(hyper_util::rt::TokioExecutor::new())
                .build::<_, Empty<Bytes>>(HttpsConnector::new());
            let base_path = PathBuf::from("temp"); // Temporary directory for downloads
            let processor = ProcessManager::new(client.clone(), base_path);
            processor.process_source(Source::Http(s.to_string())).await?;
        }
    }
    
    // Handle directory verification
    verify_directory_internal(source, &verifier, report.clone()).await?;

    // Use try_unwrap or other Arc handling here if needed
    match Arc::try_unwrap(report) {
        Ok(report) => Ok(report.into_inner().await),
        Err(_) => Err(anyhow::anyhow!("Could not unwrap report"))
    }
}

// Update function signature to take Arc
async fn verify_directory_internal(path: &Path, verifier: &VerificationManager, report: Arc<AsyncReport>) -> Result<()> {
    let https = HttpsConnector::new();
    let client = Client::builder(hyper_util::rt::TokioExecutor::new())
        .build::<_, Empty<Bytes>>(https);
    let processor = ProcessManager::new(client, path.to_path_buf());
    
    // Load max concurrent files from config
    let max_concurrent_files = verifier.config.verification.max_concurrent_files;
    let semaphore = Arc::new(Semaphore::new(max_concurrent_files));

    // First scan and count all files by type
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        match fs::read_dir(&dir).await {
            Ok(mut entries) => {
                while let Some(entry) = entries.next_entry().await? {
                    let path = entry.path();
                    if path.is_file() {
                        match path.extension().and_then(|e| e.to_str()) {
                            Some("xml") => {
                                match fs::metadata(&path).await {
                                    Ok(_) => report.add_xml(path.clone()).await,
                                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                                        report.add_missing(path.clone()).await;
                                    }
                                    Err(e) => {
                                        report.add_error(path.clone(), format!("Access error: {}", e)).await;
                                    }
                                }
                            }
                            Some("zip") => {
                                match fs::metadata(&path).await {
                                    Ok(_) => report.add_zip(path.clone()).await,
                                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                                        report.add_missing(path.clone()).await;
                                    }
                                    Err(e) => {
                                        report.add_error(path.clone(), format!("Access error: {}", e)).await;
                                    }
                                }
                            }
                            Some("vib") => {
                                match fs::metadata(&path).await {
                                    Ok(_) => (), // Will be processed later
                                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                                        report.add_missing(path.clone()).await;
                                    }
                                    Err(e) => {
                                        report.add_error(path.clone(), format!("Access error: {}", e)).await;
                                    }
                                }
                            }
                            _ => {}
                        }
                    } else if path.is_dir() {
                        stack.push(path);
                    }
                }
            }
            Err(e) => {
                report.add_error(dir.clone(), format!("Directory access error: {}", e)).await;
                continue;
            }
        }
    }

    // Now populate metadata cache if empty
    {
        let cache = verifier.metadata_cache.lock().await;
        if cache.is_empty() {
            drop(cache);  // Release lock before scanning
            
            // Find and process XML/ZIP files first
            let mut stack = vec![path.to_path_buf()];
            while let Some(dir) = stack.pop() {
                let mut entries = fs::read_dir(&dir).await?;
                while let Some(entry) = entries.next_entry().await? {
                    let path = entry.path();
                    if path.is_file() {
                        match path.extension().and_then(|e| e.to_str()) {
                            Some("xml") | Some("zip") => {
                                let source = Source::Path(path.clone());
                                if let Ok(files) = processor.process_source(source).await {
                                    for file in files {
                                        if let FileType::Vib = file.file_type {
                                            if let (Some(checksum), Some(checksum_type)) = (file.checksum, file.checksum_type) {
                                                if let Source::Path(vib_path) = file.source {
                                                    // Check if file exists before caching metadata
                                                    match fs::metadata(&vib_path).await {
                                                        Ok(_) => {
                                                            verifier.cache_vib_info(vib_path, checksum, checksum_type).await;
                                                        }
                                                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                                                            report.add_missing(vib_path.clone()).await;
                                                        }
                                                        Err(e) => {
                                                            report.add_error(vib_path, format!("Access error: {}", e)).await;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    } else if path.is_dir() {
                        stack.push(path);
                    }
                }
            }
        }
    }

    // Now verify VIB files using populated cache
    let mut vib_paths = Vec::new();
    
    // Just scan for VIB files, don't process XML/ZIP yet
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut entries = fs::read_dir(&dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_file() {
                if path.extension().and_then(|e| e.to_str()) == Some("vib") {
                    match fs::metadata(&path).await {
                        Ok(_) => vib_paths.push(path),
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            report.add_missing(path).await;
                        }
                        Err(e) => {
                            report.add_error(path.clone(), format!("Access error: {}", e)).await;
                        }
                    }
                }
            } else if path.is_dir() {
                stack.push(path);
            }
        }
    }

    // Process VIBs in parallel using cached metadata
    // Use the config value instead of constant
    let mut tasks = Vec::new();

    for vib_path in vib_paths {
        if !vib_path.exists() {
            report.add_missing(vib_path).await;
            continue; // Skip verification of missing files
        }
        let permit = semaphore.clone().acquire_owned().await?;
        let verifier = verifier.clone();
        let report = report.clone();  // Now cloning Arc<AsyncReport>
        
        tasks.push(tokio::spawn(async move {
            let _permit = permit;
            report.increment_checked().await;
            
            // Check metadata cache first
            if let Some((checksum, checksum_type)) = verifier.get_cached_metadata(&vib_path).await {
                match verifier.verify_checksum(&vib_path, &checksum, &checksum_type).await {
                    Ok(true) => (),
                    Ok(false) => report.add_mismatch(vib_path.clone(), checksum).await,
                    Err(e) => report.add_error(vib_path.clone(), format!("Verification failed: {}", e)).await,
                }
            } else {
                report.add_error(vib_path, "No metadata found".to_string()).await;
            }
            Ok::<(), anyhow::Error>(())
        }));
    }

    // Wait for all verifications
    for task in tasks {
        task.await??;
    }

    Ok(())
}

/* pub async fn verify_vib_file(
    manager: &VerificationManager,
    file_info: crate::process::FileInfo,  // Take ownership instead of borrowing
    base_path: &Path,
    report: &AsyncReport
) -> Result<()> {
    let vib_path = match &file_info.source {
        Source::Path(p) => p.clone(),
        Source::Http(url) => {
            let relative = url.split("VUM/PRODUCTION/").nth(1).unwrap_or(url);
            base_path.join(relative)
        }
    };

    // Early return if file doesn't exist or can't be accessed
    match tokio::fs::metadata(&vib_path).await {
        Ok(_) => (),
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                report.add_missing(vib_path).await;
            } else {
                report.add_error(vib_path, format!("Cannot access file: {}", e)).await;
            }
            return Ok(());
        }
    } 

    // Only proceed with verification if we have checksum info
    if let (Some(checksum), Some(checksum_type)) = (&file_info.checksum, &file_info.checksum_type) {
        // Reduce logging to improve performance
        match manager.verify_checksum(&vib_path, checksum, checksum_type).await {
            Ok(true) => (),  // Skip success logging
            Ok(false) => {
                report.add_mismatch(vib_path, checksum.clone()).await;
            }
            Err(e) => {
                report.add_error(vib_path, format!("Verification failed: {}", e)).await;
            }
        }
    }

    Ok(())

} */

/* pub fn verify_files(files: &[String]) -> Result<(), String> {
    let expected_count = files.len();
    let verified_count = 0; // replace with real logic
    println!(
        "All files verified successfully! ({} of {} files)",
        verified_count, expected_count
    );
    Ok(())
} */
