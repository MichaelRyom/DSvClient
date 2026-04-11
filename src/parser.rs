use quick_xml::events::Event;
use quick_xml::reader::Reader;
use anyhow::Result;  // Add this import
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct VendorList {
    #[serde(rename = "vendor")]
    pub vendors: Vec<Vendor>
}

#[derive(Debug, Clone, Deserialize)]
pub struct Vendor {
    pub name: String,
    pub code: String,
    pub indexfile: String,  // Note: this is the correct field name
    #[serde(rename = "relativePath")]
    pub relative_path: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetadataList {
    pub metadata: Vec<Metadata>
}


#[derive(Debug, Clone, Deserialize)]
pub struct Metadata {
    //pub version: String,
    pub url: String
}

/* #[derive(Debug, Clone, Deserialize)]
pub struct MetadataResponse {
    pub bulletin: Vec<Bulletin>
} */

/* #[derive(Debug, Clone, Deserialize)]
pub struct Bulletin {
    #[serde(rename = "vibList")]
    pub vib_list: VibList
} */

#[derive(Debug, Clone, Deserialize)]
pub struct VibList {
    //pub vib: Vec<VibInfo>
}

/* #[derive(Debug, Clone, Deserialize)]
pub struct VibInfo {
    pub relative_path: String,
    pub checksum: Option<String>,
    pub checksum_type: Option<String>,
    pub url: String,
} */

#[derive(Debug, Clone)]
pub struct VmwarePackage {
    pub name: String,
    pub version: String,
    pub url: String,
    pub checksum: String,
    pub checksum_type: String,
}

/* #[derive(Debug, Clone)]
pub struct AddonPackage {
    pub name: String,
    pub version: String,
    pub url: String,
} */

/* #[derive(Debug, Clone)]
pub struct AddonMetadata {
    pub name: String,
    pub version: String,
    pub url: String,
} */

#[derive(Debug, Clone)]
pub struct VibFile {
    pub relative_path: String,
    pub checksum: String,
    pub checksum_type: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VcsaManifest {
    #[serde(rename = "file")]
    pub files: Vec<VcsaFile>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VcsaFile {
    pub name: String,
    pub size: String,
    pub sha256sum: String,
}

#[derive(Debug)]
pub struct VcsaPackage {
    pub name: String,
    pub location: String,
    pub version: String,
    pub arch: String,
    pub checksum: String,         // SHA1 checksum
    pub checksum256: String,      // SHA256 checksum
}

#[derive(Debug, Clone, Deserialize)]
pub struct VcsaChecksum {
    #[serde(rename = "type")]
    pub checksum_type: String,
    #[serde(rename = "$value")]
    pub value: String,
}

pub struct DepotParser<'a> {
    reader: Reader<&'a [u8]>,
}

impl<'a> DepotParser<'a> {
    pub fn new(content: &'a str) -> Self {
        let reader = Reader::from_str(content);
        DepotParser { reader }
    }

/*     // Add a method to create parser from reader
    pub fn from_reader(reader: &'a [u8]) -> Self {
        let reader = Reader::from_reader(reader);
        DepotParser { reader }
    } */

    pub fn parse_packages(&mut self) -> Result<Vec<VmwarePackage>> {
        let mut packages = Vec::new();
        let mut buf = Vec::new();
        let mut current_package = None;
        let mut current_field = None;

        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => {
                    match e.name().as_ref() {
                        b"metadata" => {
                            current_package = Some(VmwarePackage {
                                name: String::new(),
                                version: String::new(),
                                url: String::new(),
                                checksum: String::new(),
                                checksum_type: String::new(),
                            });
                        }
                        b"name" | b"version" | b"url" | b"checksum" | b"checksumType" => {
                            current_field = Some(String::from_utf8_lossy(e.name().as_ref()).to_string());
                        }
                        _ => {}
                    }
                }
                Ok(Event::Text(e)) => {
                    if let (Some(package), Some(field)) = (&mut current_package, &current_field) {
                        let text = e.unescape()?.trim().to_string();
                        match field.as_str() {
                            "name" => package.name = text,
                            "version" => package.version = text,
                            "url" => package.url = text,
                            "checksum" => package.checksum = text,
                            "checksumType" => package.checksum_type = text,
                            _ => {}
                        }
                    }
                }
                Ok(Event::End(e)) => {
                    match e.name().as_ref() {
                        b"metadata" => {
                            if let Some(package) = current_package.take() {
                                packages.push(package);
                            }
                        }
                        _ => current_field = None,
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(anyhow::anyhow!("Error parsing XML: {}", e)),
                _ => {}
            }
        }
        Ok(packages)
    }

    pub fn parse_vendors(&mut self) -> Result<Vec<Vendor>> {
        let mut vendors = Vec::new();
        let mut buf = Vec::new();
        let mut current_vendor = None;
        let mut current_field = None;

        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => {
                    match e.name().as_ref() {
                        b"vendor" => {
                            current_vendor = Some(Vendor {
                                name: String::new(),
                                code: String::new(),
                                indexfile: String::new(),
                                relative_path: String::new(),
                            });
                        }
                        b"name" | b"code" | b"indexfile" | b"relativePath" => {
                            current_field = Some(String::from_utf8_lossy(e.name().as_ref()).to_string());
                        }
                        _ => {}
                    }
                }
                Ok(Event::Text(e)) => {
                    if let (Some(vendor), Some(field)) = (&mut current_vendor, &current_field) {
                        let text = e.unescape()?.trim().to_string();
                        match field.as_str() {
                            "name" => vendor.name = text,
                            "code" => vendor.code = text,
                            "indexfile" => vendor.indexfile = text,
                            "relativePath" => vendor.relative_path = text,
                            _ => {}
                        }
                    }
                }
                Ok(Event::End(e)) => {
                    match e.name().as_ref() {
                        b"vendor" => {
                            if let Some(vendor) = current_vendor.take() {
                                vendors.push(vendor);
                            }
                        }
                        _ => current_field = None,
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(anyhow::anyhow!("Error parsing XML: {}", e)),
                _ => {}
            }
        }
        Ok(vendors)
    }

/*     pub fn parse_addon_metadata(&mut self) -> Result<Vec<AddonPackage>> {
        let mut packages = Vec::new();
        let mut buf = Vec::new();
        let mut current_package = None;
        let mut current_field = None;

        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => {
                    match e.name().as_ref() {
                        b"metadata" => {
                            current_package = Some(AddonPackage {
                                name: String::new(),
                                version: String::new(),
                                url: String::new(),
                            });
                        }
                        b"name" | b"version" | b"url" => {
                            current_field = Some(String::from_utf8_lossy(e.name().as_ref()).to_string());
                        }
                        _ => {}
                    }
                }
                Ok(Event::Text(e)) => {
                    if let (Some(package), Some(field)) = (&mut current_package, &current_field) {
                        let text = e.unescape()?.trim().to_string();
                        match field.as_str() {
                            "name" => package.name = text,
                            "version" => package.version = text,
                            "url" => package.url = text,
                            _ => {}
                        }
                    }
                }
                Ok(Event::End(e)) => {
                    match e.name().as_ref() {
                        b"metadata" => {
                            if let Some(package) = current_package.take() {
                                packages.push(package);
                            }
                        }
                        _ => current_field = None,
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(anyhow::anyhow!("Error parsing XML: {}", e)),
                _ => {}
            }
        }
        Ok(packages)
    }
 */
/*     pub fn parse_addon_index(&mut self) -> Result<Vec<AddonMetadata>> {
        let mut metadata_list = Vec::new();
        let mut buf = Vec::new();
        let mut current_metadata = None;
        let mut current_field = None;

        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => {
                    match e.name().as_ref() {
                        b"metadata" => {
                            current_metadata = Some(AddonMetadata {
                                name: String::new(),
                                version: String::new(),
                                url: String::new(),
                            });
                        }
                        b"name" | b"version" | b"url" => {
                            current_field = Some(String::from_utf8_lossy(e.name().as_ref()).to_string());
                        }
                        _ => {}
                    }
                }
                Ok(Event::Text(e)) => {
                    if let (Some(metadata), Some(field)) = (&mut current_metadata, &current_field) {
                        let text = e.unescape()?.trim().to_string();
                        match field.as_str() {
                            "name" => metadata.name = text,
                            "version" => metadata.version = text,
                            "url" => metadata.url = text,
                            _ => {}
                        }
                    }
                }
                Ok(Event::End(e)) => {
                    match e.name().as_ref() {
                        b"metadata" => {
                            if let Some(metadata) = current_metadata.take() {
                                // Only add if we have all required fields
                                if !metadata.url.is_empty() && !metadata.name.is_empty() {
                                    metadata_list.push(metadata);
                                }
                            }
                        }
                        _ => current_field = None,
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(anyhow::anyhow!("Error parsing XML: {}", e)),
                _ => {}
            }
        }

        // Validate we found some metadata
        if metadata_list.is_empty() {
            return Err(anyhow::anyhow!("No valid metadata entries found in XML"));
        }
        
        Ok(metadata_list)
    }
 */
    pub fn parse_vib_files(&mut self) -> Result<Vec<VibFile>> {
        let mut vib_files = Vec::new();
        let mut buf = Vec::new();
        let mut current_vib = None;
        let mut in_vib_file = false;
        let mut current_field = None;

        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => {
                    match e.name().as_ref() {
                        b"vibFile" => {
                            in_vib_file = true;
                            current_vib = Some(VibFile {
                                relative_path: String::new(),
                                checksum: String::new(),
                                checksum_type: String::new(),
                            });
                        }
                        b"relativePath" | b"checksum" | b"checksumType" if in_vib_file => {
                            current_field = Some(String::from_utf8_lossy(e.name().as_ref()).to_string());
                        }
                        _ => {}
                    }
                }
                Ok(Event::Text(e)) => {
                    if let (Some(vib), Some(field)) = (&mut current_vib, &current_field) {
                        let text = e.unescape()?.trim().to_string();
                        match field.as_str() {
                            "relativePath" => vib.relative_path = text,
                            "checksum" => vib.checksum = text,
                            "checksumType" => vib.checksum_type = text,
                            _ => {}
                        }
                    }
                }
                Ok(Event::End(e)) => {
                    match e.name().as_ref() {
                        b"vibFile" => {
                            in_vib_file = false;
                            if let Some(vib) = current_vib.take() {
                                if !vib.relative_path.is_empty() {
                                    vib_files.push(vib);
                                }
                            }
                        }
                        _ => current_field = None,
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(anyhow::anyhow!("Error parsing XML: {}", e)),
                _ => {}
            }
        }
        Ok(vib_files)
    }

/*     // Add new method to parse VIB URLs
    pub fn parse_vib_urls(&mut self) -> Result<Vec<String>> {
        let mut urls = Vec::new();
        let mut buf = Vec::new();
        let mut in_url = false;

        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => {
                    if e.name().as_ref() == b"url" {
                        in_url = true;
                    }
                }
                Ok(Event::Text(e)) if in_url => {
                    let url = e.unescape()?.to_string();
                    if url.ends_with(".vib") {
                        urls.push(url);
                    }
                }
                Ok(Event::End(e)) => {
                    if e.name().as_ref() == b"url" {
                        in_url = false;
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(anyhow::anyhow!("Error parsing XML: {}", e)),
                _ => {}
            }
        }
        Ok(urls)
    }
 */}

pub struct XmlParser {}

impl XmlParser {
    pub fn new() -> Self {
        Self {}
    }
    
    pub fn parse_vendor_list(&self, content: &str) -> Result<Vec<Vendor>> {
        let vendor_list: VendorList = quick_xml::de::from_str(content)?;
        Ok(vendor_list.vendors)
    }

    pub fn parse_metadata_list(&self, content: &str) -> Result<Vec<Metadata>> {
        let metadata_list: MetadataList = quick_xml::de::from_str(content)?;
        Ok(metadata_list.metadata)
    }
    
/*     pub fn parse_metadata_response(&self, content: &str) -> Result<MetadataResponse> {
        Ok(quick_xml::de::from_str(content)?)
    } */

    pub fn parse_vcsa_manifest(&self, content: &str) -> Result<VcsaManifest> {
        Ok(quick_xml::de::from_str(content)?)
    }

    /// Parse the VCSA rpm-manifest.json file.
    ///
    /// This file is a JSON dict keyed by SHA256, with entries like:
    ///
    /// ```json
    /// {
    ///   "files": {
    ///     "015dc347...c72565": {
    ///       "relativepath": "015dc347...c72565.blob",
    ///       "sha256val":    "015dc347...c72565",
    ///       "type":         "blob"
    ///     },
    ///     "bba8b3bf...1aab5": {
    ///       "relativepath": "Linux-PAM-1.5.3-6.ph4.x86_64.rpm",
    ///       "sha256val":    "bba8b3bf...1aab5",
    ///       "type":         "rpm"
    ///     }
    ///   }
    /// }
    /// ```
    ///
    /// rpm-manifest.json lists extra files that are NOT in manifest-latest.xml -
    /// notably container image layers (`.blob`) and container manifests
    /// (`.manifest`). These files are required for `software-packages stage --iso`
    /// to succeed on the VCSA during appliance patching.
    ///
    /// The `relativepath` values in this file are bare filenames (no
    /// `package-pool/` prefix), so we prepend `package-pool/` to match the URL
    /// layout the downloader already uses for entries from the XML manifest.
    pub fn parse_vcsa_rpm_manifest_json(&self, content: &str) -> Result<Vec<VcsaPackage>> {
        let v: serde_json::Value = serde_json::from_str(content)?;
        let files = v.get("files")
            .and_then(|f| f.as_object())
            .ok_or_else(|| anyhow::anyhow!("rpm-manifest.json is missing a top-level 'files' object"))?;

        let mut packages = Vec::with_capacity(files.len());
        for (_hash, entry) in files {
            let relpath = entry.get("relativepath")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if relpath.is_empty() {
                continue;
            }
            let sha256 = entry.get("sha256val")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            packages.push(VcsaPackage {
                name:        relpath.clone(),
                // Prepend package-pool/ so existing URL-building in downloader.rs
                // (format!("{base}/{version}/{location}")) resolves to the right URL.
                location:    format!("package-pool/{}", relpath),
                version:     String::new(),
                arch:        String::new(),
                checksum:    String::new(),
                checksum256: sha256,
            });
        }

        Ok(packages)
    }

    pub fn parse_vcsa_packages(&self, content: &str) -> Result<Vec<VcsaPackage>> {
        let mut reader = quick_xml::Reader::from_str(content);
        let mut buf = Vec::new();
        let mut packages = Vec::new();
        let mut current_package: Option<VcsaPackage> = None;
        let mut in_location = false;
        let mut in_checksum = false;
        let mut in_checksum256 = false;

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(quick_xml::events::Event::Start(e)) => {
                    match e.name().as_ref() {
                        b"package" => {
                            current_package = Some(VcsaPackage {
                                name: e.try_get_attribute("name")?.map(|a| a.unescape_value().unwrap().to_string()).unwrap_or_default(),
                                version: e.try_get_attribute("version")?.map(|a| a.unescape_value().unwrap().to_string()).unwrap_or_default(),
                                arch: e.try_get_attribute("arch")?.map(|a| a.unescape_value().unwrap().to_string()).unwrap_or_default(),
                                location: String::new(),
                                checksum: String::new(),
                                checksum256: String::new(),
                            });
                        }
                        b"location" => in_location = true,
                        b"checksum" => in_checksum = true,
                        b"checksum256" => in_checksum256 = true,
                        _ => (),
                    }
                }
                Ok(quick_xml::events::Event::Text(e)) if current_package.is_some() => {
                    let pkg = current_package.as_mut().unwrap();
                    let text = e.unescape().unwrap().to_string();
                    if in_location {
                        pkg.location = text;
                    } else if in_checksum {
                        pkg.checksum = text;
                    } else if in_checksum256 {
                        pkg.checksum256 = text;
                    }
                }
                Ok(quick_xml::events::Event::End(e)) => {
                    match e.name().as_ref() {
                        b"package" => {
                            if let Some(pkg) = current_package.take() {
                                packages.push(pkg);
                            }
                        }
                        b"location" => in_location = false,
                        b"checksum" => in_checksum = false,
                        b"checksum256" => in_checksum256 = false,
                        _ => (),
                    }
                }
                Ok(quick_xml::events::Event::Eof) => break,
                Err(e) => return Err(anyhow::anyhow!("Error parsing XML: {}", e)),
                _ => (),
            }
        }

        Ok(packages)
    }
}