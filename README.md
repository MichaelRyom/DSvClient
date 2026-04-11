# DSvClient

Check out [https://michaelryom.dk/dsvclient-new-patch-downloading-tool-for-vcenter](https://michaelryom.dk/dsvclient-new-patch-downloading-tool-for-vcenter)

## 📁 Version 0.8.0
**Released:** April 11, 2026

### On-Disk Layout Now Mirrors the Broadcom CDN (VCSA)
Previous releases downloaded every VCSA patch file (RPMs, container image
blobs, container manifests, patch-scripts zips, root metadata) into a single
flat directory at `valm/<version>/`. That worked for local inspection and for
ISO repackaging, but it did **not** match the layout the appliance expects: the
real Broadcom CDN, a real VCSA patch ISO, and `software-packages stage --url`
all put everything except the top-level manifests under `package-pool/`.

As a result, the downloaded tree could not be served over HTTP as a drop-in
replacement for the CDN, and the ISO-building helper had to reconstruct
`package-pool/` by hand — which broke in 0.7.0 once blobs, `.manifest`
container files, and the two `*-patch-scripts.zip` script bundles entered the
picture (they ended up at the ISO root instead of in `package-pool/`, causing
`software-packages stage --iso` to fail with *"Staging failed. Retry to resume
from the current state."*).

**What changed in 0.8.0:**
- **`get_vcsa_file_path` preserves the relative path from each package's
  `<location>` / `relativepath` entry** instead of stripping it to the
  basename. Files whose location has a `package-pool/` prefix now land at
  `valm/<version>/package-pool/<file>`; bare root metadata files
  (`manifest-latest.xml`, `rpm-manifest.json`, and their `.sha256` / `.sign`
  sidecars) still land at `valm/<version>/<file>`.
- **`..` and `.` path segments are stripped defensively** so a hostile
  manifest can't write outside the version directory.
- **`download_file` already creates missing parent directories**, so the
  `package-pool/` subdir is created on demand — no schema migration needed
  inside DSvClient itself.

**Result:** a freshly-downloaded `valm/<version>/` tree can be:
1. Served directly over HTTP as a local mirror of the Broadcom CDN for
   `software-packages stage --url http://host/valm/<version>/`.
2. Turned into a VCSA patch ISO by straight recursive copy — no need to
   partition files between root and `package-pool/` at ISO-build time.
3. Inspected locally the same way as before — only the layout has changed.

**Migrating an existing flat repo (no re-download):**
```sh
cd /path/to/VMware-repo/valm/<version>
mkdir -p package-pool
for f in *; do
  case "$f" in
    package-pool|manifest-latest.xml|manifest-latest.xml.sha256|manifest-latest.xml.sign|\
    rpm-manifest.json|rpm-manifest.json.sha256|rpm-manifest.json.sign) ;;
    *) mv -- "$f" package-pool/ ;;
  esac
done
```
PowerShell equivalent:
```powershell
$src = 'C:\VMware-repo\valm\8.0.3.00800'
$keep = @('manifest-latest.xml','manifest-latest.xml.sha256','manifest-latest.xml.sign',
          'rpm-manifest.json','rpm-manifest.json.sha256','rpm-manifest.json.sign')
New-Item -ItemType Directory -Path (Join-Path $src 'package-pool') -Force | Out-Null
Get-ChildItem -Path $src -File |
  Where-Object { $keep -notcontains $_.Name } |
  Move-Item -Destination (Join-Path $src 'package-pool')
```

## 📦 Version 0.7.0
**Released:** April 11, 2026

### Full VCSA Patch Downloads (Bug Fix)
Earlier releases downloaded only the files referenced in `manifest-latest.xml`
for VCSA sources. That manifest lists RPMs and the two `*-patch-scripts.zip`
files, but it does **not** include the 24+ content-addressed container image
layers (`.blob`) and container manifests (`.manifest`) that a VCSA patch ships
with. Without those files, `software-packages stage --iso` on the appliance
fails with `"The CD drive does not have a valid patch ISO or has an unsupported
version"` because the appliance can't find the container layers referenced in
the patch metadata.

**What changed in 0.7.0:**
- **New parser** — `XmlParser::parse_vcsa_rpm_manifest_json()` reads
  `package-pool/rpm-manifest.json` and returns the complete file list that the
  appliance's own patching code consumes.
- **New downloader hook** — after `rpm-manifest.json` is downloaded, the VCSA
  source loop now also parses it and queues every extra file it references
  (blobs + container manifests + any additional RPMs) using the same URL layout
  and dedup logic already used for the XML manifest entries.
- **Shared queue back-end** — the per-file queueing logic in
  `process_vcsa_manifest` was extracted into a shared helper
  (`queue_vcsa_packages`) so the XML and JSON paths both go through the same
  verification / dedup / concurrency machinery.
- **Log output** — VCSA log lines now preserve the file extension for non-RPM
  entries (`.blob`, `.manifest`) so they're readable in `DSvClient.log`.

**No configuration changes required.** Existing `sources.toml` VCSA entries
work as-is — the fix activates automatically whenever `rpm-manifest.json` is in
the `files = [...]` list (which it already is in the default config).

Result: a VCSA patch folder downloaded with 0.7.0 contains everything
`software-packages stage --iso --acceptEulas` needs to validate and stage the
patch on the appliance.

## 🔐 Version 0.6.0
**Released:** August 27, 2025

### New OAuth 2.0 Authentication System
- **Added OAuth 2.0 Client Credentials Flow** - Full implementation for VMware VVS (vSphere Vulnerabilities & Security) API access
- **New OAuth Source Type** - Dedicated source configuration with:
  - `client_id` - OAuth application identifier  
  - `client_secret` - OAuth application secret
  - `auth_url` - Authentication endpoint URL
  - `output_filename` - Configurable output file names

### Advanced HTTP Handling Improvements
- **Automatic Redirect Handling** - Up to 5 redirects automatically followed for OAuth requests
- **Proper VVS API Headers** - Includes required headers:
  - `X-Vmw-Esp-Client` - OAuth token authentication
- **Enhanced Error Handling** - Detailed OAuth-specific error messages and status reporting

### File Handling Enhancements
- **Raw File Downloads** - OAuth downloads preserve original format without automatic decompression
- **Smart Content Handling** - Files downloaded in their native format (e.g., .gz files stay compressed)
- **Configurable Output Names** - Custom filenames for downloaded OAuth resources

### Configuration Updates
**New OAuth Example** - Complete working example in sources.toml:

```toml
[[sources]]
url = "https://vvs.broadcom.com/v1/compatible/vcg/bundles/all?format=gz"
type = "OAuth"
client_id = "vsphere_hcllib"
client_secret = "cb65f5ac-cb38-4326-a48f-c0e8b23a2d38"
auth_url = "https://auth.esp.vmware.com/api/auth/v1/tokens"
output_filename = "vvs_data.gz"
```

Each platform release now includes:
- Main executable (DSvClient or DSvClient.exe)
- Complete configuration files:
  - `config.toml` - Main application configuration
  - `sources.toml` - Source definitions with OAuth examples
- Platform-specific README files with detailed setup instructions
- Ready-to-use packages - No additional configuration required (But a Broadcom Token)

## 🎯 Version 0.5.0
**Released:** August 27, 2025

### Complete Command-Line Interface Overhaul (Major User Experience Update)
- **Version information system** - `--version` / `-V` flags display version from Cargo.toml
- **Comprehensive help system** - `--help` / `-h` flags show detailed usage information
- **Auto-help display** - Shows help when no arguments provided
- **Professional CLI behavior** - Matches industry standards for command-line tools

### Enhanced Package Metadata
- **Complete package information** - Description, author, license, repository in Cargo.toml
- **Runtime version access** - Uses CARGO_PKG_VERSION at compile time
- **Professional presentation** - Clean, informative output format

### Improved Argument Parsing
- **Flag handling system** - Proper parsing of command-line flags
- **Multiple flag formats** - Support for both `--flag` and `-f` formats
- **Error handling** - Graceful handling of invalid arguments

## 🚨 Version 0.4.0
**Released:** August 20, 2025

### HTTP Error Response Enhancement (Major Feature)
- **Detailed HTTP error messages** - Captures actual server response bodies instead of generic status codes
- **Enhanced DownloadReport structure** - Stores HTTP error messages for 403/404 errors
- **Better debugging information** - Real server error responses help troubleshoot authentication and access issues

### Advanced Timeout Management System
- **Exponential backoff timeout tracking** - Dynamic timeout adjustment based on network conditions
- **Timeout escalation** - Base 30s → Max 5min with doubling on failures
- **Smart timeout reduction** - Automatic timeout reduction after 5min of stability
- **Connection health monitoring** - Per-connection timeout tracking

### Configuration System Improvements
- **Enhanced config parsing** - Better error handling for malformed configurations
- **Expanded configuration options** - More granular control over download behavior
- **JSON export enhancements** - HTTP error messages included in detailed reports

### Retry Logic Overhaul
- **Fixed retry tuple handling** - Proper handling of error messages in retry mechanisms
- **Improved failure recovery** - Better logic for determining when to retry vs. fail

### Dependency Management
- **Major dependency updates** - Updated to latest compatible versions of core libraries
- **Build stabilization** - All tests passing and build successful
- **Removed problematic Cargo.lock** - Better dependency resolution

## 🗂️ Directory Structure Fixes
### ESX_HOST Path Bug Resolution
- **Added ESX_HOST to skip list** - Prevents creation of unwanted ESX_HOST directories in file paths
- **Fixed path extraction logic** - Files no longer incorrectly saved under ESX_HOST directories
- **Cleaner output structure** - More intuitive directory organization

## 🛣️ Version 0.3.2 - Path Structure Overhaul
**Released:** August 18, 2025

### Complete Path Extraction Rewrite
- **Fixed malformed paths** - Eliminated paths like `/tmp/vmware/https:/dl.broadcom.com/...`
- **Enhanced extract_meaningful_path() function** - Smart URL parsing that:
  - Filters out domain names and tokens
  - Skips generic segments (PROD, COMP)
  - Extracts meaningful paths like ESX_HOST/main, VCENTER/version
- **Clean directory structures** - Files now saved in logical paths like `/tmp/vmware/ESX_HOST/main/`

### Documentation and Configuration Updates
- **Added comprehensive README.md** - 44 lines of documentation
- **Enhanced sources.toml** - Better examples and comments
- **Improved logging** - Better path extraction debugging

## 🔧 Version 0.3.1 - Critical URL Fixes
**Released:** August 18, 2025

### vCenter Download URL Corrections
- **Fixed .latest suffix bug** - Removed incorrect .latest from vCenter version URLs
- **URL generation fix** - URLs now correctly generate as version/file instead of version.latest/file
- **Resolves download failures** - vCenter packages now download successfully
- **Affected functions:** `process_vcsa_url()` and `process_vcsa_manifest()`

---

## Changes Since Version 0.3.0

### Major Features Added

#### 🔐 OAuth 2.0 Support (v0.6.0)
- Added OAuth 2.0 authentication for VMware VVS API access
- Enhanced authentication capabilities for secure API interactions

#### 📋 Command-Line Interface Improvements (v0.5.0)
- Added comprehensive version information display
- Improved command-line argument handling and user experience

#### 🌐 Enhanced HTTP Error Handling (v0.4.0)
- Added detailed HTTP response error messages
- Better error reporting for debugging network issues
- Improved user experience with more informative error messages

### Bug Fixes & Improvements

#### 🔧 URL and Path Handling Fixes
- **Fixed Broadcom URL path extraction** - Eliminated malformed paths like 'https:/' in file directories
- **Fixed vCenter download URLs** - Removed incorrect .latest suffixes that were breaking downloads
- **Improved URL path extraction** - Cleaner directory structures with better path sanitization

#### 🏗️ Build System Improvements
- **Fixed Windows build issues** - Removed sccache installation that was causing windows_sys compatibility problems
- **Stabilized dependencies** - Pinned problematic dependencies to stable versions
- **Fixed ESX_HOST directory bug** - Resolved directory creation issues

#### 📦 GitHub Actions & Release Process
- **Fixed double compression in artifacts** - Eliminated nested zip files in release downloads
- **Removed duplicate publish steps** - Streamlined the release workflow
- **Enhanced release packages** - Now include config files (config.toml, sources.toml) with platform-specific READMEs
- **Upgraded artifact uploads** - Updated to actions/upload-artifact@v4

#### 🗂️ Configuration & File Management
- **Added exclusions.json to gitignore** - Better repository cleanliness
- **Improved config file handling** - Better integration with release packages

### Version Progression
- v0.3.0 → v0.3.1 → v0.4.0 → v0.5.0 → v0.6.0 → v0.7.0 (current)
