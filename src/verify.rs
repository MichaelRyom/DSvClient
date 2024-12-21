use anyhow::Result;
use log::{info, warn, error, debug};
use sha2::{Sha256, Digest};
use std::path::{Path, PathBuf};
use std::collections::HashSet;
use tokio::fs;
use tokio::io::{AsyncReadExt, BufReader};
use crate::process::{ProcessManager, Source, FileType};
use hyper_util::client::legacy::Client;
use hyper_tls::HttpsConnector;
use http_body_util::Empty;
use bytes::Bytes;
use tokio::sync::Semaphore;
use std::sync::Arc;
use rayon::ThreadPoolBuilder;
use tokio::task;
use std::time::Duration;
use tokio::time::sleep;
use std::sync::Mutex;
use tokio::sync::Mutex as TokioMutex;

// Add new constants for controlling CPU usage
const MAX_CONCURRENT_VERIFICATIONS: usize = 100; // Adjust based on CPU cores
const VERIFICATION_CHUNK_SIZE: usize = 1024 * 1024; // 1MB chunks
const THREAD_SLEEP_MS: u64 = 10; // Sleep time between chunks

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
        info!("XML files processed: {}", self.processed_xmls.len());
        info!("ZIP files processed: {}", self.processed_zips.len());
        info!("Total VIB files checked: {}", self.files_checked);
        
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
    fn new() -> Self {
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
    thread_pool: Arc<rayon::ThreadPool>,
    semaphore: Arc<Semaphore>,
}

impl VerificationManager {
    pub fn new() -> Self {
        let thread_pool = ThreadPoolBuilder::new()
            .num_threads(MAX_CONCURRENT_VERIFICATIONS)
            .build()
            .unwrap();

        Self {
            thread_pool: Arc::new(thread_pool),
            semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_VERIFICATIONS)),
        }
    }

    pub async fn verify_checksum(&self, path: &Path, expected: &str, checksum_type: &str) -> Result<bool> {
        if checksum_type.to_lowercase() != "sha-256" {
            warn!("Unsupported checksum type '{}' for {}", checksum_type, path.display());
            return Ok(false);
        }

        info!("Verifying checksum for: {}", path.display());
        info!("Expected SHA-256: {}", expected);

        let _permit = self.semaphore.acquire().await?;
        let path_for_closure = path.to_path_buf();  // Create a clone for the closure
        let path_for_info = path.to_path_buf();     // Create another clone for the info message
        let expected = expected.to_string();

        let result = task::spawn_blocking(move || -> Result<bool> {
            use std::io::Read;
            let file = std::fs::File::open(&path_for_closure)?;
            let mut reader = std::io::BufReader::new(file);
            let mut hasher = Sha256::new();
            let mut buffer = vec![0; VERIFICATION_CHUNK_SIZE];
            
            loop {
                match reader.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(n) => {
                        hasher.update(&buffer[..n]);
                        std::thread::sleep(Duration::from_millis(THREAD_SLEEP_MS));
                    }
                    Err(e) => return Err(anyhow::anyhow!("Read error: {}", e)),
                }
            }

            let calculated = format!("{:x}", hasher.finalize());
            Ok(calculated == expected.to_lowercase())
        }).await??;

        match result {
            true => info!("Checksum verified successfully for {} ({} bytes)", 
                path_for_info.display(), 
                path_for_info.metadata()?.len()),
            false => warn!("Checksum mismatch for {}", path_for_info.display()),
        }

        Ok(result)
    }
}

pub async fn verify_directory(base_path: &Path) -> Result<VerificationReport> {
    let report = AsyncReport::new();
    let verification_manager = VerificationManager::new();
    
    let https = HttpsConnector::new();
    let client = Client::builder(hyper_util::rt::TokioExecutor::new())
        .build::<_, Empty<Bytes>>(https);
    
    let mut processor = ProcessManager::new(client, base_path.to_path_buf());
    
    // Find all XML and ZIP files in the directory first
    let mut found_files = Vec::new();
    let mut stack = vec![base_path.to_path_buf()];
    
    while let Some(dir) = stack.pop() {
        let mut entries = fs::read_dir(&dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_file() {
                match path.extension().and_then(|e| e.to_str()) {
                    Some("xml") => {
                        info!("Found XML file: {}", path.display());
                        found_files.push(Source::Path(path));
                    }
                    Some("zip") => {
                        info!("Found ZIP file: {}", path.display());
                        found_files.push(Source::Path(path));
                    }
                    _ => {}
                }
            } else if path.is_dir() {
                stack.push(path);
            }
        }
    }

    info!("Found {} files to process", found_files.len());
    
    // Process each file
    for source in found_files {
        let source_clone = source.clone();  // Clone the source
        match processor.process_source(source_clone).await {
            Ok(files) => {
                let mut tasks = Vec::new();
                for file in files.into_iter() {  // Use into_iter() to move ownership
                    match file.file_type {
                        FileType::Xml => {
                            if let Source::Path(path) = file.source {
                                report.add_xml(path).await;
                            }
                        }
                        FileType::Zip => {
                            if let Source::Path(path) = file.source {
                                report.add_zip(path).await;
                            }
                        }
                        FileType::Vib => {
                            report.increment_checked().await;
                            // Move ownership of file into the task
                            tasks.push(verify_vib_file(&verification_manager, file, base_path, &report));
                        }
                        _ => {}
                    }
                }
                futures::future::join_all(tasks).await;
            }
            Err(e) => {
                if let Source::Path(path) = source {  // Original source is still available here
                    report.add_error(path, e.to_string()).await;
                }
            }
        }
    }

    // Process sources from sources file
    let sources = include_str!("../sources");
    let mut rdr = csv::Reader::from_reader(sources.as_bytes());
    
    for result in rdr.records() {
        let record = result?;
        if record.get(1) == Some("Yes") && record.get(2) == Some("Connected") {
            if let Some(url) = record.get(0) {
                let relative_path = url.split("VUM/PRODUCTION/").nth(1).unwrap_or(url);
                let xml_path = base_path.join(relative_path);

                // Try both HTTP and local path
                let source = if xml_path.exists() {
                    report.add_xml(xml_path.clone()).await;  // Use async method
                    Source::Path(xml_path)
                } else {
                    Source::Http(url.to_string())
                };

                match processor.process_source(source).await {
                    Ok(files) => {
                        let mut tasks = Vec::new();
                        for file in files {
                            match file.file_type {
                                FileType::Xml => {
                                    if let Source::Path(path) = &file.source {
                                        report.add_xml(path.to_path_buf()).await;  // Use async method
                                    }
                                }
                                FileType::Zip => {
                                    if let Source::Path(path) = &file.source {
                                        report.add_zip(path.to_path_buf()).await;  // Use async method
                                    }
                                }
                                FileType::Vib => {
                                    report.increment_checked().await;  // Use async method
                                    // Move ownership of file into the task
                                    tasks.push(verify_vib_file(&verification_manager, file, base_path, &report));
                                }
                                _ => {}
                            }
                        }
                        futures::future::join_all(tasks).await;
                    }
                    Err(e) => {
                        report.add_error(PathBuf::from(url), e.to_string()).await;  // Use async method
                    }
                }
            }
        }
    }

    Ok(report.into_inner().await)
}

async fn verify_vib_file(
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

    if !vib_path.exists() {
        warn!("Missing VIB file: {}", vib_path.display());
        report.add_missing(vib_path).await;
        return Ok(());
    }

    if let (Some(checksum), Some(checksum_type)) = (&file_info.checksum, &file_info.checksum_type) {
        let file_size = vib_path.metadata()?.len();
        info!("Verifying VIB file: {} ({} bytes)", vib_path.display(), file_size);
        info!("Expected {} checksum: {}", checksum_type, checksum);

        match manager.verify_checksum(&vib_path, checksum, checksum_type).await {
            Ok(true) => {
                info!("✓ Verified: {}", vib_path.display());
            }
            Ok(false) => {
                warn!("✗ Checksum mismatch: {}", vib_path.display());
                warn!("  Expected: {}", checksum);
                report.add_mismatch(vib_path, checksum.clone()).await;
            }
            Err(e) => {
                error!("! Verification error for {}: {}", vib_path.display(), e);
                report.add_error(vib_path, format!("Verification failed: {}", e)).await;
            }
        }
    } else {
        warn!("No checksum information available for: {}", vib_path.display());
    }

    Ok(())
}
