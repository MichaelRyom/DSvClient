use anyhow::Result;
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use tokio::fs;
use crate::parser::{DepotParser, VibFile};
use std::collections::{HashSet, HashMap};
use hyper::{Request, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_tls::HttpsConnector;
use http_body_util::{BodyExt, Empty};
use bytes::Bytes;
use log::{info, warn, debug, error};
use tokio::io::AsyncReadExt;
use zip::ZipArchive;
use std::io::Read;
use std::sync::Arc;
use tokio::sync::Mutex;  // Change to tokio's Mutex

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub enum Source {
    Http(String),
    Path(PathBuf),
}

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub source: Source,
    pub relative_path: String,
    pub checksum: Option<String>,
    pub checksum_type: Option<String>,
    pub file_type: FileType,
    pub size: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FileType {
    Xml,
    Zip,
    Vib,
    Other(String),
}

#[async_trait]
pub trait SourceProcessor {
    async fn read_content(&self, source: &Source) -> Result<Vec<u8>>;
    async fn save_content(&self, content: &[u8], dest: &Path) -> Result<()>;
}

#[derive(Clone)]
pub struct HttpProcessor {
    client: Client<HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>, Empty<Bytes>>,
}

#[async_trait]
impl SourceProcessor for HttpProcessor {
    async fn read_content(&self, source: &Source) -> Result<Vec<u8>> {
        if let Source::Http(url) = source {
            debug!("HTTP processor reading from URL: {}", url);
            let req = Request::builder()
                .uri(url)
                .body(Empty::<Bytes>::new())?;

            let mut response = self.client.request(req).await?;
            let mut content = Vec::new();

            while let Some(frame) = response.frame().await {
                let frame = frame?;
                if let Some(data) = frame.data_ref() {
                    content.extend_from_slice(data);
                }
            }
            Ok(content)
        } else {
            error!("Invalid source type for HTTP processor: {:?}", source);
            Err(anyhow::anyhow!("Not an HTTP source"))
        }
    }

    async fn save_content(&self, content: &[u8], dest: &Path) -> Result<()> {
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(dest, content).await?;
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct FileProcessor;

#[async_trait]
impl SourceProcessor for FileProcessor {
    async fn read_content(&self, source: &Source) -> Result<Vec<u8>> {
        if let Source::Path(path) = source {
            debug!("File processor reading from path: {}", path.display());
            Ok(fs::read(path).await?)
        } else {
            error!("Invalid source type for file processor: {:?}", source);
            Err(anyhow::anyhow!("Not a file source"))
        }
    }

    async fn save_content(&self, content: &[u8], dest: &Path) -> Result<()> {
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(dest, content).await?;
        Ok(())
    }
}

// Make ProcessManager cloneable by deriving Clone and using Arc for shared state
#[derive(Clone)]
pub struct ProcessManager {
    http_processor: HttpProcessor,
    file_processor: FileProcessor,
    processed_sources: Arc<Mutex<HashSet<Source>>>,  // Changed to tokio Mutex
    base_path: PathBuf,
}

impl ProcessManager {
    pub fn new(client: Client<HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>, Empty<Bytes>>, base_path: PathBuf) -> Self {
        debug!("Creating new ProcessManager with base path: {}", base_path.display());
        Self {
            http_processor: HttpProcessor { client },
            file_processor: FileProcessor::default(),
            processed_sources: Arc::new(Mutex::new(HashSet::new())),
            base_path,
        }
    }

    pub async fn process_source(&self, source: Source) -> Result<Vec<FileInfo>> {
        // Change to take &self instead of &mut self since we're using Arc<Mutex> now
        debug!("Processing source: {:?}", source);
        self.process_source_inner(source).await
    }

    async fn process_source_inner(&self, source: Source) -> Result<Vec<FileInfo>> {
        {
            let mut processed = self.processed_sources.lock().await;
            if processed.contains(&source) {
                debug!("Source already processed, skipping: {:?}", source);
                return Ok(Vec::new());
            }
            processed.insert(source.clone());
        }

        debug!("Starting processing of source: {:?}", source);

        let content = match &source {
            Source::Http(url) => {
                debug!("Downloading HTTP content from: {}", url);
                self.http_processor.read_content(&source).await?
            }
            Source::Path(path) => {
                debug!("Reading file content from: {}", path.display());
                self.file_processor.read_content(&source).await?
            }
        };
        debug!("Content size: {} bytes", content.len());

        let file_type = self.get_file_type(&source);
        debug!("Detected file type: {:?}", file_type);

        match file_type {
            FileType::Xml => {
                debug!("Processing as XML");
                self.process_xml(&source, &content).await
            }
            FileType::Zip => {
                debug!("Processing as ZIP");
                self.process_zip(&source, &content).await
            }
            _ => {
                debug!("Processing as regular file");
                Ok(vec![self.create_file_info(&source, None, None)?])
            }
        }
    }

    fn get_file_type(&self, source: &Source) -> FileType {
        let path = match source {
            Source::Http(url) => Path::new(url),
            Source::Path(path) => path.as_path(),
        };

        match path.extension().and_then(|ext| ext.to_str()) {
            Some("xml") => FileType::Xml,
            Some("zip") => FileType::Zip,
            Some("vib") => FileType::Vib,
            Some(ext) => FileType::Other(ext.to_string()),
            None => FileType::Other("".to_string()),
        }
    }

    async fn process_xml(&self, source: &Source, content: &[u8]) -> Result<Vec<FileInfo>> {
        debug!("Processing XML content from {:?}", source);
        let mut files = Vec::new();
        let content_str = String::from_utf8_lossy(content);
        let mut parser = DepotParser::new(&content_str);

        // Process vendors (for depot index)
        if let Ok(vendors) = parser.parse_vendors() {
            debug!("Found {} vendors", vendors.len());
            for vendor in vendors {
                debug!("Processing vendor: {} ({})", vendor.name, vendor.code);
                let vendor_source = match source {
                    Source::Http(url) => {
                        let base = url.rsplit_once('/').map(|(dir, _)| dir).unwrap_or(url);
                        Source::Http(format!("{}/{}/{}", base, vendor.relative_path, vendor.index_file))
                    }
                    Source::Path(path) => {
                        Source::Path(path.parent().unwrap_or(path).join(&vendor.relative_path).join(&vendor.index_file))
                    }
                };
                // Box the recursive call
                files.extend(Box::pin(self.process_source_inner(vendor_source)).await?);
            }
        }

        // Process packages
        if let Ok(packages) = parser.parse_packages() {
            debug!("Found {} packages", packages.len());
            for package in packages {
                debug!("Processing package: {} v{}", package.product_id, package.version);
                let package_source = self.create_relative_source(source, &package.url)?;
                // Box the recursive call
                files.extend(Box::pin(self.process_source_inner(package_source)).await?);
            }
        }

        // Process VIB files
        if let Ok(vibs) = parser.parse_vib_files() {
            debug!("Found {} VIB files", vibs.len());
            for vib in vibs {
                debug!("Processing VIB: {}", vib.relative_path);
                files.push(FileInfo {
                    source: self.create_relative_source(source, &vib.relative_path)?,
                    relative_path: vib.relative_path,
                    checksum: Some(vib.checksum),
                    checksum_type: Some(vib.checksum_type),
                    file_type: FileType::Vib,
                    size: None,
                });
            }
        }

        debug!("XML processing complete, found {} total files", files.len());
        Ok(files)
    }

    async fn process_zip(&self, source: &Source, content: &[u8]) -> Result<Vec<FileInfo>> {
        debug!("Processing ZIP content from {:?}", source);
        let mut files = Vec::new();
        let reader = std::io::Cursor::new(content);
        let mut archive = ZipArchive::new(reader)?;
        debug!("ZIP archive contains {} files", archive.len());

        for i in 0..archive.len() {
            if let Ok(mut file) = archive.by_index(i) {
                debug!("Examining ZIP entry: {} ({} bytes)", file.name(), file.size());
                if file.name().ends_with("vmware.xml") {
                    debug!("Found vmware.xml in ZIP: {}", file.name());
                    let mut contents = String::new();
                    file.read_to_string(&mut contents)?;
                    let mut parser = DepotParser::new(&contents);
                    if let Ok(vibs) = parser.parse_vib_files() {
                        debug!("Found {} VIB files in ZIP", vibs.len());
                        for vib in vibs {
                            info!("Processing VIB from ZIP: {} (Checksum: {} {})",
                                vib.relative_path,
                                vib.checksum_type,
                                vib.checksum);
                            files.push(FileInfo {
                                source: self.create_relative_source(source, &vib.relative_path)?,
                                relative_path: vib.relative_path,
                                checksum: Some(vib.checksum),
                                checksum_type: Some(vib.checksum_type),
                                file_type: FileType::Vib,
                                size: None,
                            });
                        }
                    }
                }
            }
        }

        debug!("ZIP processing complete, found {} total files", files.len());
        Ok(files)
    }

    fn create_relative_source(&self, base_source: &Source, relative_path: &str) -> Result<Source> {
        debug!("Creating relative source from {:?} with path {}", base_source, relative_path);
        Ok(match base_source {
            Source::Http(url) => {
                let base = url.rsplit_once('/').map(|(dir, _)| dir).unwrap_or(url);
                Source::Http(format!("{}/{}", base, relative_path))
            }
            Source::Path(path) => {
                Source::Path(path.parent().unwrap_or(path).join(relative_path))
            }
        })
    }

    fn create_file_info(&self, source: &Source, checksum: Option<String>, checksum_type: Option<String>) -> Result<FileInfo> {
        debug!("Creating file info for {:?}", source);
        let relative_path = match source {
            Source::Http(url) => url.split("VUM/PRODUCTION/").nth(1).unwrap_or(url).to_string(),
            Source::Path(path) => path.strip_prefix(&self.base_path)
                .unwrap_or(path)
                .to_string_lossy()
                .to_string(),
        };

        Ok(FileInfo {
            source: source.clone(),
            relative_path,
            checksum,
            checksum_type,
            file_type: self.get_file_type(source),
            size: None,
        })
    }
}
