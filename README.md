Check out https://codeberg.org/MichaelRyom/DSvClient or https://michaelryom.dk/dsvclient-new-patch-downloading-tool-for-vcenter

## ⚠️ Broadcom Migration Update (2024)

Broadcom has migrated VMware's update infrastructure and introduced a new download token authentication system. DSvClient now supports:

- **Legacy VMware URLs**: Still supported for existing repositories
- **New Broadcom URLs**: Requires download tokens from your Broadcom support account

### Setting up Broadcom Download Tokens

1. Obtain your download token from the Broadcom Support Portal
2. Update your `sources.toml` file with your global download token:

```toml
# Global download token used by all Broadcom sources
downloadToken = "YOUR_ACTUAL_TOKEN_HERE"

[[sources]]
url = "https://dl.broadcom.com/<downloadToken>/PROD/COMP/ESX_HOST/main/vmw-depot-index.xml"
enabled = true
vendor = "Broadcom"
type = "Host"
```

#### Per-Source Token Override (Optional)

If you need different tokens for specific sources, you can override the global token:

```toml
# This source uses its own token instead of the global one
[[sources]]
url = "https://dl.broadcom.com/<downloadToken>/PROD/COMP/ESX_HOST/special/vmw-depot-index.xml"
downloadToken = "SPECIAL_TOKEN_FOR_THIS_SOURCE"
enabled = true
vendor = "Broadcom"
type = "Host"
```

### URL Format Changes

- **Old VMware URLs**: `https://hostupdate.vmware.com/software/VUM/PRODUCTION/...`
- **New Broadcom URLs**: `https://dl.broadcom.com/<downloadToken>/PROD/COMP/ESX_HOST/...`

The `<downloadToken>` placeholder in URLs will be automatically replaced with your actual token during downloads.
