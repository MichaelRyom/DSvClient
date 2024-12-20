use anyhow::{Result, Context};
use log::{info, warn, error};
use sha2::{Sha256, Digest};
use std::path::{Path, PathBuf};
use std::io::Read;
use std::collections::HashSet;
use tokio::fs;
use crate::parser::{DepotParser, VibFile};
use std::pin::Pin;
use std::future::Future;
use tokio::io::{AsyncReadExt, BufReader as TokioBufReader};
use std::sync::Arc;


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

pub async fn verify_directory(base_path: &Path) -> Result<VerificationReport> {
    let mut report = VerificationReport::default();
    
    // Find top-level directories
    let mut entries = fs::read_dir(base_path).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.is_dir() {
            process_directory(&path, &mut report).await?;
        }
    }

    Ok(report)
}

async fn process_directory(dir: &Path, report: &mut VerificationReport) -> Result<()> {
    // Look for index.xml files first
    let mut index_found = false;
    let mut entries = fs::read_dir(dir).await?;
    
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.is_file() && path.file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.contains("index.xml"))
            .unwrap_or(false) 
        {
            index_found = true;
            process_xml_file(&path, dir, report).await
                .with_context(|| format!("Failed to process index file: {}", path.display()))?;
        }
    }

    // If no index.xml found, scan for vmware.xml files
    if !index_found {
        scan_for_xml_files(dir, report).await?;
    }

    Ok(())
}

fn process_xml_file<'a>(
    xml_path: &'a Path,
    base_dir: &'a Path,
    report: &'a mut VerificationReport,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        if report.processed_xmls.contains(xml_path) {
            return Ok(());
        }
        
        report.processed_xmls.insert(xml_path.to_path_buf());
        
        let content = match fs::read_to_string(xml_path).await {
            Ok(content) => content,
            Err(e) => {
                report.add_error(xml_path.to_path_buf(), format!("Failed to read XML: {}", e));
                return Ok(());
            }
        };

        let mut parser = DepotParser::new(&content);
        
        // Process VIB files
        if let Ok(vibs) = parser.parse_vib_files() {
            process_vib_files(vibs, base_dir, report).await?;
        }

        // Process vendor index
        if let Ok(vendors) = parser.parse_vendors() {
            for vendor in vendors {
                let vendor_path = base_dir.join(&vendor.relative_path).join(&vendor.index_file);
                process_xml_file(&vendor_path, base_dir, report).await?;
            }
        }

        // Process packages
        if let Ok(packages) = parser.parse_packages() {
            for package in packages {
                let package_path = base_dir.join(&package.url);
                if package_path.extension().and_then(|e| e.to_str()) == Some("zip") {
                    process_zip_file(&package_path, base_dir, report).await?;
                }
            }
        }

        Ok(())
    })
}

async fn scan_for_xml_files(dir: &Path, report: &mut VerificationReport) -> Result<()> {
    let mut stack = vec![dir.to_path_buf()];

    while let Some(current_dir) = stack.pop() {
        let mut entries = fs::read_dir(&current_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_file() {
                if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
                    match ext {
                        "xml" if path.to_string_lossy().contains("vmware.xml") => {
                            process_xml_file(&path, dir, report).await?;
                        },
                        "zip" => {
                            process_zip_file(&path, dir, report).await?;
                        },
                        _ => {}
                    }
                }
            } else if path.is_dir() {
                stack.push(path);
            }
        }
    }
    
    Ok(())
}

async fn process_zip_file(zip_path: &Path, base_dir: &Path, report: &mut VerificationReport) -> Result<()> {
    if report.processed_zips.contains(zip_path) {
        return Ok(());
    }

    report.processed_zips.insert(zip_path.to_path_buf());

    // Read the entire file into memory first
    let zip_data = match tokio::fs::read(zip_path).await {
        Ok(data) => data,
        Err(e) => {
            report.add_error(zip_path.to_path_buf(), format!("Failed to read ZIP: {}", e));
            return Ok(());
        }
    };

    // Process ZIP contents in a blocking task
    match tokio::task::spawn_blocking(move || -> Result<Vec<VibFile>> {
        let reader = std::io::Cursor::new(zip_data);
        let mut archive = zip::ZipArchive::new(reader)?;
        let mut vib_files = Vec::new();

        for i in 0..archive.len() {
            if let Ok(mut file) = archive.by_index(i) {
                if file.name().ends_with("vmware.xml") {
                    let mut contents = String::new();
                    if file.read_to_string(&mut contents).is_ok() {
                        let mut parser = DepotParser::new(&contents);
                        if let Ok(files) = parser.parse_vib_files() {
                            vib_files.extend(files);
                        }
                    }
                }
            }
        }
        Ok(vib_files)
    }).await? {
        Ok(vibs) => process_vib_files(vibs, base_dir, report).await?,
        Err(e) => report.add_error(zip_path.to_path_buf(), format!("Failed to process ZIP: {}", e)),
    }

    Ok(())
}

async fn process_vib_files(vibs: Vec<VibFile>, base_dir: &Path, report: &mut VerificationReport) -> Result<()> {
    for vib in vibs {
        let vib_path = base_dir.join(&vib.relative_path);
        
        if !vib_path.exists() {
            report.vib_files_missing.push(vib_path);
            continue;
        }

        report.files_checked += 1;
        
        match verify_checksum(&vib_path, &vib.checksum, &vib.checksum_type).await {
            Ok(true) => (),
            Ok(false) => {
                report.checksum_mismatches.push((vib_path, vib.checksum));
            },
            Err(e) => {
                report.add_error(vib_path, format!("Checksum verification failed: {}", e));
            }
        }
    }
    Ok(())
}

async fn verify_checksum(path: &Path, expected: &str, checksum_type: &str) -> Result<bool> {
    if checksum_type.to_lowercase() != "sha-256" {
        return Ok(false);
    }

    let file = tokio::fs::File::open(path).await?;
    let mut reader = TokioBufReader::new(file);
    let mut buffer = vec![0; 64 * 1024];
    let mut hasher = Sha256::new();

    loop {
        let n = reader.read(&mut buffer).await?;
        if n == 0 { break; }
        hasher.update(&buffer[..n]);
    }
    
    Ok(format!("{:x}", hasher.finalize()) == expected.to_lowercase())
}
