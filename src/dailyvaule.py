import os
import pathlib
import uuid
from typing import Optional, Dict

import boto3

from smbprotocol.connection import Connection
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect
from smbprotocol.open import (
    Open,
    ImpersonationLevel,
    DirectoryAccessMask,
    FileAttributes,
    ShareAccess,
    CreateDisposition,
    CreateOptions,
)
from smbprotocol.file_info import FileInformationClass


def download_smb_to_s3(
    server: str,
    share: str,
    subfolder: str,
    username: str,
    password: str,
    s3_bucket: str,
    s3_prefix: str = "smb-downloads",
    download_dir: Optional[str] = None,
    chunk_size: int = 1024 * 1024,
    port: int = 445,
    s3_access_key: Optional[str] = None,
    s3_secret_key: Optional[str] = None,
    s3_endpoint_url: Optional[str] = None,
) -> Dict[str, int]:
    """
    Download files from SMB share and upload to S3.

    Args:
        server: SMB server IP or hostname
        share: SMB share name
        subfolder: Subfolder path within the share (use backslashes for SMB paths)
        username: SMB username
        password: SMB password
        s3_bucket: S3 bucket name
        s3_prefix: S3 prefix/folder path (default: "smb-downloads")
        download_dir: Local directory for temporary downloads (default: ./smb_download)
        chunk_size: Size of chunks to read (default: 1MB)
        port: SMB port (default: 445)
        s3_access_key: AWS access key ID (optional, uses credentials from environment/config if not provided)
        s3_secret_key: AWS secret access key (optional, uses credentials from environment/config if not provided)
        s3_endpoint_url: Custom S3 endpoint URL (optional, for S3-compatible services like MinIO)

    Returns:
        dict: Statistics with 'downloaded' and 'uploaded' counts
    """
    if download_dir is None:
        download_dir = os.path.join(os.getcwd(), "smb_download")

    pathlib.Path(download_dir).mkdir(parents=True, exist_ok=True)
    s3_prefix = s3_prefix.strip("/")

    # Initialize S3 client with optional credentials and endpoint
    s3_client_kwargs = {}
    if s3_access_key and s3_secret_key:
        s3_client_kwargs["aws_access_key_id"] = s3_access_key
        s3_client_kwargs["aws_secret_access_key"] = s3_secret_key
    if s3_endpoint_url:
        s3_client_kwargs["endpoint_url"] = s3_endpoint_url

    s3_client = boto3.client("s3", **s3_client_kwargs)

    def upload_to_s3(local_file_path: str, file_name: str, smb_subfolder: str) -> bool:
        """Upload a file to S3 bucket preserving the SMB folder structure."""
        try:
            # Convert SMB path to S3 path: report\daily -> report/daily
            s3_folder_path = smb_subfolder.replace("\\", "/")
            s3_key = f"{s3_prefix}/{s3_folder_path}/{file_name}"

            s3_client.upload_file(local_file_path, s3_bucket, s3_key)
            print(f"    ✓ Uploaded to s3://{s3_bucket}/{s3_key}")
            return True
        except Exception as e:
            print(f"    ✗ Failed to upload to S3: {e}")
            return False

    try:
        conn = Connection(uuid.uuid4(), server, port)
        conn.connect()
        print(f"✓ Connected to server: {server}:{port}")

        session = Session(conn, username, password)
        session.connect()
        print("✓ Authenticated")

        tree = TreeConnect(session, fr"\\{server}\{share}")
        tree.connect()
        print(f"✓ Connected to share: {share}")

        directory = Open(tree, subfolder)
        directory.create(
            ImpersonationLevel.Impersonation,
            DirectoryAccessMask.FILE_LIST_DIRECTORY | DirectoryAccessMask.SYNCHRONIZE,
            FileAttributes.FILE_ATTRIBUTE_DIRECTORY,
            ShareAccess.FILE_SHARE_READ | ShareAccess.FILE_SHARE_WRITE,
            CreateDisposition.FILE_OPEN,
            CreateOptions.FILE_DIRECTORY_FILE,
        )
        print(f"✓ Opened directory: {subfolder}")

        files = directory.query_directory("*", FileInformationClass.FILE_NAMES_INFORMATION)
        print(f"\nFiles in {share}/{subfolder}:")

        downloaded_count = 0
        uploaded_count = 0
        for f in files:
            file_name_bytes = f["file_name"].get_value()
            if isinstance(file_name_bytes, bytes):
                file_name = file_name_bytes.decode("utf-16-le")
            else:
                file_name = str(file_name_bytes)

            if file_name in (".", ".."):  # skip current/parent directory entries
                continue

            print(f"  Downloading: {file_name}")

            file_obj = None
            try:
                remote_path = f"{subfolder}\\{file_name}"
                file_obj = Open(tree, remote_path)
                file_obj.create(
                    ImpersonationLevel.Impersonation,
                    0x00120089,  # Generic read + sync + file read data
                    FileAttributes.FILE_ATTRIBUTE_NORMAL,
                    ShareAccess.FILE_SHARE_READ,
                    CreateDisposition.FILE_OPEN,
                    CreateOptions.FILE_NON_DIRECTORY_FILE,
                )

                local_path = os.path.join(download_dir, file_name)
                with open(local_path, "wb") as output_file:
                    offset = 0
                    while True:
                        try:
                            data = file_obj.read(offset, chunk_size)
                            if not data:
                                break
                            output_file.write(data)
                            offset += len(data)
                        except Exception as read_err:
                            # Handle EOF or empty files gracefully
                            if "STATUS_END_OF_FILE" in str(read_err):
                                break
                            raise

                downloaded_count += 1
                print(f"    ✓ Downloaded to {local_path}")

                # Upload to S3
                if upload_to_s3(local_path, file_name, subfolder):
                    uploaded_count += 1

            except Exception as e:
                print(f"    ✗ Failed to download: {e}")
            finally:
                # Always close the file handle if it was opened
                if file_obj is not None:
                    try:
                        file_obj.close()
                    except Exception:
                        pass  # Ignore errors during cleanup

        directory.close()
        tree.disconnect()
        session.disconnect()
        conn.disconnect()

        print(f"\n✓ Done! Downloaded {downloaded_count} files to {download_dir}")
        print(f"✓ Uploaded {uploaded_count} files to s3://{s3_bucket}/{s3_prefix}/{subfolder.replace(chr(92), '/')}")

        return {
            "downloaded": downloaded_count,
            "uploaded": uploaded_count,
        }

    except Exception as e:
        print(f"✗ Error: {e}")
        raise


def main() -> Dict[str, int]:
    """Main entry point with configuration from environment variables."""
    server = os.environ.get("SMB_SERVER", "192.168.1.7")
    share = os.environ.get("SMB_SHARE", "smbtest")
    subfolder = os.environ.get("SMB_SUBFOLDER", r"report\daily")
    username = os.environ.get("SMB_USERNAME", "CHENGWANG2019")
    password = os.environ.get("SMB_PASSWORD", "Password2019")

    s3_bucket = os.environ.get("S3_BUCKET_NAME", "your-bucket-name")
    s3_prefix = os.environ.get("S3_PREFIX", "smb-downloads")
    download_dir = os.environ.get(
        "SMB_DOWNLOAD_DIR",
        "/Users/chengwang/PycharmProjects/PythonProject/smb_download",
    )

    # S3 Credentials (optional)
    s3_access_key = os.environ.get("S3_ACCESS_KEY")
    s3_secret_key = os.environ.get("S3_SECRET_KEY")
    s3_endpoint_url = os.environ.get("S3_ENDPOINT_URL")

    if s3_bucket == "your-bucket-name":
        print("⚠ Warning: Please set S3_BUCKET_NAME environment variable")

    result = download_smb_to_s3(
        server=server,
        share=share,
        subfolder=subfolder,
        username=username,
        password=password,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        download_dir=download_dir,
        s3_access_key=s3_access_key,
        s3_secret_key=s3_secret_key,
        s3_endpoint_url=s3_endpoint_url,
    )

    return result


if __name__ == "__main__":
    main()

