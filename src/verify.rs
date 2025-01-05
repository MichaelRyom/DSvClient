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
  // Rename conflicting imports


// Increase parallelism and optimize buffer sizes
//const VERIFICATION_CHUNK_SIZE: usize = 256 * 1024; // 256KB is optimal for most filesystems
const MAX_CONCURRENT_FILES: usize = 1000; // Process many files simultaneously
//const THREAD_SLEEP_MS: u64 = 0; // Remove artificial delay

#[derive(Debug, Default)]
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
                warn!("  {} (Expected: {})", path.display(), expected);
            }
        }

        if !self.error_files.is_empty() {
            error!("\nFiles with Errors ({}):", self.error_files.len());
            for (path, error) in &self.error_files {
                error!("  {}: {}", path.display(), error);
            }
        }

        info!("XML files processed: {}", self.processed_xmls.len());
        info!("ZIP files processed: {}", self.processed_zips.len());
        info!("Total VIB files checked: {}", self.files_checked);
        info!("Total VIB files missing: {}", self.vib_files_missing.len());
        info!("Files with checksum mismatches: {}", self.checksum_mismatches.len());
        info!("Files with errors: {}", self.error_files.len());
    }

    fn add_error(&mut self, path: PathBuf, error: String) {
        error!("Error processing {}: {}", path.display(), error);
        self.error_files.push((path, error));
    }
}

pub struct AsyncReport {
    inner: TokioMutex<VerificationReport>,
}

impl AsyncReport {
    pub fn new() -> Self {  // Change from private to public
        Self {
            inner: TokioMutex::new(VerificationReport::default()),
        }
    }

    async fn add_xml(&self, path: PathBuf) {
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
        report.vib_files_missing.push(path);
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
        self.inner.into_inner()
    }
}

pub struct VerificationManager {
    //thread_pool: Arc<rayon::ThreadPool>,
    semaphore: Arc<Semaphore>,
    config: Arc<AppConfig>,  // Use AppConfig instead of Config
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
        }
    }
    
    pub async fn verify_checksum(&self, path: &Path, expected: &str, checksum_type: &str) -> Result<bool> {
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
}

// Update function signature to accept Path
pub async fn verify_directory(source: &Path) -> Result<VerificationReport> {
    let config = AppConfig::load_or_default();
    let report = AsyncReport::new();
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
    verify_directory_internal(source, &verifier, &report).await?;

    Ok(report.into_inner().await)
}

async fn verify_directory_internal(path: &Path, verifier: &VerificationManager, report: &AsyncReport) -> Result<()> {
    let https = HttpsConnector::new();
    let client = Client::builder(hyper_util::rt::TokioExecutor::new())
        .build::<_, Empty<Bytes>>(https);
    
    let processor = ProcessManager::new(client, path.to_path_buf());
    
    // Use Arc<Mutex<>> for shared state
    let vib_checksums: Arc<TokioMutex<HashMap<PathBuf, Option<(String, String)>>>> = 
        Arc::new(TokioMutex::new(HashMap::new()));
    let mut to_process = Vec::new();

    // First pass: collect files to process
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut entries = fs::read_dir(&dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_file() {
                match path.extension().and_then(|e| e.to_str()) {
                    Some("xml") => {
                        info!("Found XML file: {}", path.display());
                        to_process.push(Source::Path(path.clone()));
                        report.add_xml(path).await;
                    }
                    Some("zip") => {
                        info!("Found ZIP file: {}", path.display());
                        to_process.push(Source::Path(path.clone()));
                        report.add_zip(path).await;
                    }
                    Some("vib") => {
                        // Add VIBs to shared map
                        let mut checksums = vib_checksums.lock().await;
                        checksums.insert(path, None);
                    }
                    _ => {}
                }
            } else if path.is_dir() {
                stack.push(path);
            }
        }
    }

    // Process XML/ZIP files
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_FILES));
    let mut tasks = Vec::new();

    for source in to_process {
        let permit = semaphore.clone().acquire_owned().await?;
        let source_clone = source.clone();
        let processor = processor.clone();
        let checksums = vib_checksums.clone();
        //let path = path.to_path_buf();

        tasks.push(tokio::spawn(async move {
            let _permit = permit;
            if let Ok(files) = processor.process_source(source_clone).await {
                for file in files {
                    if let FileType::Vib = file.file_type {
                        if let (Some(checksum), Some(checksum_type)) = (file.checksum, file.checksum_type) {
                            if checksum_type.to_lowercase() == "sha-256" {
                                if let Source::Path(vib_path) = file.source {
                                    let mut checksums = checksums.lock().await;
                                    checksums.insert(vib_path, Some((checksum, checksum_type)));
                                }
                            }
                        }
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        }));
    }

    // Wait for metadata processing
    for task in tasks {
        task.await??;
    }

    // Verify VIBs
    let checksums = vib_checksums.lock().await;
    
    for (vib_path, checksum_info) in checksums.iter() {
        // Check if file exists
        match tokio::fs::metadata(vib_path).await {
            Ok(_) => {
                if let Some((checksum, checksum_type)) = checksum_info {
                    report.increment_checked().await;
                    match verifier.verify_checksum(vib_path, checksum, checksum_type).await {
                        Ok(true) => (),
                        Ok(false) => report.add_mismatch(vib_path.clone(), checksum.clone()).await,
                        Err(e) => report.add_error(vib_path.clone(), format!("Verification failed: {}", e)).await,
                    }
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                report.add_missing(vib_path.clone()).await;
            },
            Err(e) => {
                report.add_error(vib_path.clone(), format!("Cannot access file: {}", e)).await;
            }
        }
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
