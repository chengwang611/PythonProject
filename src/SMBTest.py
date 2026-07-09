import uuid
from smbprotocol.connection import Connection
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect
from smbprotocol.open import Open
from smbprotocol.file_info import FileInformationClass


def list_smb_files(server, share, username, password, domain=None):
    """
    List files in an SMB share

    Args:
        server: SMB server IP (e.g., "192.168.1.4")
        share: Share name (e.g., "LENOVO-P50")
        username: SMB username
        password: SMB password
        domain: Domain/Workgroup name (optional, e.g., "WORKGROUP")

    Returns:
        List of file names
    """
    try:
        # Connect to server
        conn = Connection(uuid.uuid4(), server, 445)
        conn.connect()

        # Login with domain if provided
        if domain:
            username_with_domain = f"{domain}\\{username}"
            session = Session(conn, username_with_domain, password)
        else:
            session = Session(conn, username, password)

        session.connect()

        # Connect to share
        tree = TreeConnect(session, fr"\\{server}\{share}")
        tree.connect()

        # List files
        from smbprotocol.open import (
            ImpersonationLevel,
            DirectoryAccessMask,
            FileAttributes,
            ShareAccess,
            CreateDisposition,
            CreateOptions
        )

        directory = Open(tree, "")
        directory.create(
            ImpersonationLevel.Impersonation,
            DirectoryAccessMask.FILE_LIST_DIRECTORY | DirectoryAccessMask.SYNCHRONIZE,
            FileAttributes.FILE_ATTRIBUTE_DIRECTORY,
            ShareAccess.FILE_SHARE_READ | ShareAccess.FILE_SHARE_WRITE,
            CreateDisposition.FILE_OPEN,
            CreateOptions.FILE_DIRECTORY_FILE
        )

        files = []
        file_info = directory.query_directory("*", FileInformationClass.FILE_NAMES_INFORMATION)

        for f in file_info:
            file_name = f["file_name"].get_value()
            files.append(file_name)
            print(f"  {file_name}")

        # Cleanup
        directory.close()
        tree.disconnect()
        session.disconnect()
        conn.disconnect()

        return files

    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    files = list_smb_files("192.168.1.4", "smbtest", "chengwang2019", "Password2019", domain="")
    print(f"\nFound {len(files)} files")



