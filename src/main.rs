use anyhow::Result;
use std::path::PathBuf;
use log::{LevelFilter, warn, info, error};
use simplelog::{WriteLogger, CombinedLogger, TermLogger, Config, TerminalMode, ColorChoice};
use std::fs::File;

mod downloader;
mod parser;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    
    // Check for --verify flag
    let verify = args.contains(&"--verify".to_string());

    if args.len() < 2 || args.len() > 3 {
        println!("Usage: {} <download_path> [--verify]", args[0]);
        std::process::exit(1);
    }

    let download_path = PathBuf::from(&args[1]);
    std::fs::create_dir_all(&download_path)?;
    
    // Set up logging
    let log_file = File::create(download_path.join("download_errors.log"))?;
    CombinedLogger::init(vec![
        TermLogger::new(
            LevelFilter::Info,
            Config::default(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        ),
        WriteLogger::new(
            LevelFilter::Warn,
            Config::default(),
            log_file,
        ),
    ])?;

    let service = downloader::DownloadService::new(download_path.clone());
    
    if verify {
        info!("Starting verification process...");
        match service.verify_downloads().await {
            Ok((checked, missing, wrong_checksum)) => {
                if checked == 0 {
                    warn!("No files were checked. Are you sure the directory contains downloaded VMware files?");
                    warn!("Directory: {}", download_path.display());
                }
                if missing > 0 || wrong_checksum > 0 {
                    warn!("Verification found issues:");
                    warn!("  - {} files missing", missing);
                    warn!("  - {} files with wrong checksum", wrong_checksum);
                    std::process::exit(1);
                } else if checked > 0 {
                    info!("All {} files verified successfully!", checked);
                }
            }
            Err(e) => {
                error!("Verification failed: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        // Process all sources
        if let Err(e) = service.process_sources().await {
            warn!("Errors occurred during initial processing: {}", e);
        }

        // Retry failed downloads
        if let Err(e) = service.retry_failed_downloads().await {
            warn!("Errors occurred during retry attempts: {}", e);
        }
        
        // Summarize files not downloaded
        let failed_downloads = service.get_failed_downloads();
        if !failed_downloads.is_empty() {
            warn!("Summary of files not downloaded:");
            for url in failed_downloads {
                warn!("  {}", url);
            }
        } else {
            info!("All files downloaded successfully.");
        }
    }
    
    Ok(())
}