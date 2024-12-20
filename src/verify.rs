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
use zip::ZipArchive;

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
    
    // Read the sources file
    let sources = include_str!("../sources");
    let mut rdr = csv::Reader::from_reader(sources.as_bytes());
    
    // Process each enabled source
    for result in rdr.records() {
        let record = result?;
        if record.get(1) == Some("Yes") && record.get(2) == Some("Connected") {
            if let Some(url) = record.get(0) {
                let relative_path = url.split("VUM/PRODUCTION/").nth(1).unwrap_or(url);
                let xml_path = base_path.join(relative_path);
                if xml_path.exists() {
                    process_xml_file(&xml_path, base_path, &mut report).await?;
                } else {
                    warn!("Source XML file not found: {}", xml_path.display());
                }
            }
        }
    }

    Ok(report)
}

fn process_xml_file<'a>(
    xml_path: &'a Path,
    base_path: &'a Path,
    report: &'a mut VerificationReport,
) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>> {
    Box::pin(async move {
        if report.processed_xmls.contains(xml_path) {
            return Ok(());
        }
        
        info!("Processing XML file: {}", xml_path.display());
        report.processed_xmls.insert(xml_path.to_path_buf());
        
        let content = fs::read_to_string(xml_path).await?;
        let mut parser = DepotParser::new(&content);
        
        // Process depot index
        if xml_path.to_string_lossy().contains("vmw-depot-index.xml") {
            if let Ok(vendors) = parser.parse_vendors() {
                for vendor in vendors {
                    let vendor_path = base_path
                        .join(&vendor.relative_path)
                        .join(&vendor.index_file);
                    if vendor_path.exists() {
                        process_xml_file(&vendor_path, base_path, report).await?;
                    } else {
                        warn!("Vendor XML not found: {}", vendor_path.display());
                    }
                }
            }
        }

        // Process packages and their metadata
        if let Ok(packages) = parser.parse_packages() {
            for package in packages {
                let package_path = base_path.join(&package.url);
                if package_path.exists() {
                    if package_path.extension().and_then(|e| e.to_str()) == Some("zip") {
                        process_zip_file(&package_path, base_path, report).await?;
                    }
                } else {
                    warn!("Package file not found: {}", package_path.display());
                }
            }
        }

        // Process addon metadata
        if let Ok(addon_metadata) = parser.parse_addon_metadata() {
            for metadata in addon_metadata {
                let metadata_path = base_path.join(&metadata.url);
                if metadata_path.exists() {
                    if metadata_path.extension().and_then(|e| e.to_str()) == Some("zip") {
                        process_zip_file(&metadata_path, base_path, report).await?;
                    }
                } else {
                    warn!("Addon metadata file not found: {}", metadata_path.display());
                }
            }
        }

        // Process VIB files directly in this XML
        if let Ok(vibs) = parser.parse_vib_files() {
            process_vib_files(vibs, base_path, report).await?;
        }

        Ok(())
    })
}

async fn process_zip_file(zip_path: &Path, base_path: &Path, report: &mut VerificationReport) -> Result<()> {
    if report.processed_zips.contains(zip_path) {
        return Ok(());
    }

    info!("Processing ZIP file: {}", zip_path.display());
    report.processed_zips.insert(zip_path.to_path_buf());

    let zip_data = fs::read(zip_path).await?;
    
    let vib_files = tokio::task::spawn_blocking(move || -> Result<Vec<VibFile>> {
        let reader = std::io::Cursor::new(zip_data);
        let mut archive = ZipArchive::new(reader)?;
        let mut vib_files = Vec::new();

        for i in 0..archive.len() {
            if let Ok(mut file) = archive.by_index(i) {
                if file.name().ends_with("vmware.xml") {
                    let mut contents = String::new();
                    file.read_to_string(&mut contents)?;
                    let mut parser = DepotParser::new(&contents);
                    if let Ok(files) = parser.parse_vib_files() {
                        vib_files.extend(files);
                    }
                }
            }
        }
        Ok(vib_files)
    }).await??;

    process_vib_files(vib_files, base_path, report).await?;
    Ok(())
}

async fn process_vib_files(vibs: Vec<VibFile>, base_path: &Path, report: &mut VerificationReport) -> Result<()> {
    for vib in vibs {
        let vib_path = base_path.join(&vib.relative_path);
        report.files_checked += 1;
        
        if !vib_path.exists() {
            report.vib_files_missing.push(vib_path);
            continue;
        }

        match verify_checksum(&vib_path, &vib.checksum, &vib.checksum_type).await {
            Ok(true) => {
                info!("Verified: {}", vib_path.display());
            },
            Ok(false) => {
                warn!("Checksum mismatch: {}", vib_path.display());
                report.checksum_mismatches.push((vib_path, vib.checksum));
            },
            Err(e) => {
                error!("Failed to verify {}: {}", vib_path.display(), e);
                report.add_error(vib_path, format!("Verification failed: {}", e));
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
