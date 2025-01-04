#![allow(unused)]
// Version 0.1.1
use anyhow::Result;
use bytes::Bytes;
use http_body_util::Empty;
use hyper_tls::HttpsConnector;
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use log::{error, info, warn, LevelFilter};
use simplelog::{ColorChoice, CombinedLogger, Config, TermLogger, TerminalMode, WriteLogger};
use std::fs::File;
use std::path::PathBuf;

mod config;
mod downloader;
mod parser;
mod process;
mod verify;

use crate::config::AppConfig; // Use renamed import

#[tokio::main]
async fn main() -> Result<()> {
    // Load config first
    let config = AppConfig::load_or_default();

    let args: Vec<String> = std::env::args().collect();

    // Check for --verify flag but exclude it from being treated as a path
    let verify = args.iter().any(|arg| arg == "--verify");

    // Get the download path by finding the first argument that isn't --verify
    let download_path = args
        .iter()
        .skip(1) // Skip program name
        .find(|arg| *arg != "--verify")
        .map(|path| PathBuf::from(path))
        .ok_or_else(|| {
            eprintln!("Usage: {} <download_path> [--verify]", args[0]);
            anyhow::anyhow!("No download path provided")
        })?;

    info!("Using download path: {}", download_path.display());
    std::fs::create_dir_all(&download_path)?;

    // Check for sources.toml instead of sources
    if !PathBuf::from("sources.toml").exists() {
        error!("sources.toml file not found");
        std::process::exit(1);
    }

    // Set up logging
    let log_file = File::create(download_path.join("download_errors.log"))?;
    CombinedLogger::init(vec![
        TermLogger::new(
            LevelFilter::Info,
            Config::default(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        ),
        WriteLogger::new(LevelFilter::Warn, Config::default(), log_file),
    ])?;

    let https = HttpsConnector::new();
    let client =
        Client::builder(hyper_util::rt::TokioExecutor::new()).build::<_, Empty<Bytes>>(https);
    let service = downloader::Downloader::new(
        download_path.clone(),
        client,
        config.clone(), // Pass config to Downloader
    );

    if verify {
        info!("Starting verification process...");
        match verify::verify_directory(&download_path).await {
            Ok(report) => {
                report.print_summary();
                if !report.vib_files_missing.is_empty()
                    || !report.checksum_mismatches.is_empty()
                    || !report.error_files.is_empty()
                {
                    std::process::exit(1);
                }
                info!("\nAll files verified successfully!");
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

        // Get and display file type statistics
        /*let stats = service.get_file_type_stats().await; // Add .await
        info!("\nFile format summary:");
        for (ext, count) in stats.into_iter() {
            // Use into_iter() on the HashMap
            info!("  {}: {} files", ext, count);
        }*/

        // Summarize files not downloaded
        let failed_downloads = service.get_failed_downloads().await; // Add .await
        if !failed_downloads.is_empty() {
            warn!("\nSummary of files not downloaded:");
            for url in failed_downloads.into_iter() {
                // Use into_iter() on the Vec
                warn!("  {}", url);
            }
        } else {
            info!("\nAll files downloaded successfully.");
        }

        // Get and display download report
        let report = service.get_download_report().await;
        report.print_summary();
    }

    Ok(())
}
