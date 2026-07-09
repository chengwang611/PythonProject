"""Helpers for writing encoded text, encrypting files, and FTPS upload.

These utilities are intentionally small and side-effect focused so that
higher-level pipelines can orchestrate them.
"""
from typing import Iterable
import subprocess
from ftplib import FTP_TLS
import os


def write_recordlines_cp1047(recordlines: Iterable[str], output_path: str) -> None:
    """Write lines to a single text file encoded with cp1047.

    Each element in ``recordlines`` is expected to be a Python ``str``; newlines
    are appended between records.
    """
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "wb") as f:
        for line in recordlines:
            if line is None:
                line = ""
            if not isinstance(line, str):
                line = str(line)
            f.write(line.encode("cp1047"))
            f.write(b"\n")


def encrypt_file_with_pgp(input_path: str, output_path: str, public_key_path: str) -> None:
    """Encrypt ``input_path`` to ``output_path`` using gpg and a given public key.

    This assumes the ``gpg`` binary is available on the system.
    """
    # Import the public key (idempotent) and then encrypt.
    # Many setups instead configure a recipient id; here we import from file and
    # let gpg pick it up.
    subprocess.run(["gpg", "--batch", "--yes", "--import", public_key_path], check=True)
    subprocess.run(
        [
            "gpg",
            "--batch",
            "--yes",
            "--trust-model",
            "always",
            "-o",
            output_path,
            "-e",
            input_path,
        ],
        check=True,
    )


def encrypt_file_with_openssl(input_path: str, output_path: str, public_key_path: str) -> None:
    """Encrypt ``input_path`` to ``output_path`` using openssl rsautl.

    This is a simple example; in production you'd likely use CMS/PKCS#7.
    """
    subprocess.run(
        [
            "openssl",
            "rsautl",
            "-encrypt",
            "-inkey",
            public_key_path,
            "-pubin",
            "-in",
            input_path,
            "-out",
            output_path,
        ],
        check=True,
    )


def ftps_upload_file(
    host: str,
    port: int,
    username: str,
    password: str,
    local_path: str,
    remote_path: str,
    use_tls_explicit: bool = True,
    passive: bool = True,
) -> None:
    """Upload ``local_path`` to ``remote_path`` via FTPS to an MVS/mainframe.

    ``remote_path`` may include directories, e.g. ``"/inbound/myfile.dat"``.
    """
    ftps = FTP_TLS()
    ftps.connect(host, port)
    ftps.login(username, password)
    if use_tls_explicit:
        ftps.prot_p()  # secure data connection
    ftps.set_pasv(passive)

    # Change directory if remote_path contains dirs
    dir_name, file_name = os.path.split(remote_path)
    if dir_name:
        ftps.cwd(dir_name)

    with open(local_path, "rb") as f:
        ftps.storbinary(f"STOR {file_name}", f)

    ftps.quit()

