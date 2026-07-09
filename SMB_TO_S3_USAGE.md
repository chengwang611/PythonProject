# SMB to S3 Download Usage Guide

## Overview
The `SMBTEST2.py` script downloads files from an SMB share and uploads them to Amazon S3, preserving the folder structure.

## Refactored Structure

### Main Function: `download_smb_to_s3()`
A reusable function that can be imported and called from other scripts.

**Parameters:**
- `server` (str): SMB server IP or hostname
- `share` (str): SMB share name
- `subfolder` (str): Subfolder path (use backslashes, e.g., `r"report\daily"`)
- `username` (str): SMB username
- `password` (str): SMB password
- `s3_bucket` (str): S3 bucket name
- `s3_prefix` (str, optional): S3 prefix/folder path (default: "smb-downloads")
- `download_dir` (str, optional): Local temp directory (default: "./smb_download")
- `chunk_size` (int, optional): Read chunk size in bytes (default: 1MB)
- `port` (int, optional): SMB port (default: 445)

**Returns:**
```python
{
    "downloaded": 3,  # Number of files downloaded from SMB
    "uploaded": 3     # Number of files uploaded to S3
}
```

## Usage Examples

### 1. Using Environment Variables (Recommended)
```bash
export SMB_SERVER="192.168.1.7"
export SMB_SHARE="smbtest"
export SMB_SUBFOLDER="report\daily"
export SMB_USERNAME="CHENGWANG2019"
export SMB_PASSWORD="Password2019"
export S3_BUCKET_NAME="my-bucket"
export S3_PREFIX="smb-downloads"
export SMB_DOWNLOAD_DIR="/tmp/smb_download"

python src/dailyvaule.py
```

### 2. Importing and Using in Another Script

```python
from src.dailyvaule import download_smb_to_s3

# Download from SMB and upload to S3
result = download_smb_to_s3(
    server="192.168.1.7",
    share="smbtest",
    subfolder=r"report\daily",
    username="CHENGWANG2019",
    password="Password2019",
    s3_bucket="my-bucket",
    s3_prefix="smb-downloads",
    download_dir="/tmp/smb_download"
)

print(f"Downloaded: {result['downloaded']}, Uploaded: {result['uploaded']}")
```

### 3. Using with Different Subfolders
```python
# Download from report/daily
download_smb_to_s3(
    server="192.168.1.7",
    share="smbtest",
    subfolder=r"report\daily",
    username="user",
    password="pass",
    s3_bucket="my-bucket"
)

# Download from report/monthly
download_smb_to_s3(
    server="192.168.1.7",
    share="smbtest",
    subfolder=r"report\monthly",
    username="user",
    password="pass",
    s3_bucket="my-bucket"
)
```

## S3 Path Structure
Files are uploaded to S3 preserving the SMB folder structure:

```
SMB: \\192.168.1.7\smbtest\report\daily\file.txt
  ↓
S3:  s3://my-bucket/smb-downloads/report/daily/file.txt
```

## Features
- ✅ Chunked reading for large files (1MB chunks by default)
- ✅ Handles empty files and EOF conditions gracefully
- ✅ Proper cleanup of SMB connections
- ✅ Preserves folder structure in S3
- ✅ Configurable via environment variables or parameters
- ✅ Returns statistics (downloaded/uploaded counts)
- ✅ Can be imported and reused in other scripts

## Error Handling
- Files that fail to download are logged but don't stop the process
- Connection errors are raised and should be caught by the caller
- Always closes SMB connections properly, even on errors

## AWS Credentials
The script uses boto3, which reads AWS credentials from:
1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. AWS credentials file (`~/.aws/credentials`)
3. IAM role (if running on EC2/ECS/Lambda)

