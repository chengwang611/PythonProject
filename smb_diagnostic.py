#!/usr/bin/env python3
"""
Simple SMB Diagnostic Tool
"""
import socket
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.SMBTest import list_smb_files


def main():
    print("\n" + "=" * 60)
    print("SMB DIAGNOSTIC")
    print("=" * 60)

    server = "192.168.1.4"
    share = "mao-h"
    username = "chengwang2019"
    password = "Password2019"

    # Step 1: Check network
    print("\n[1] Network Check")
    try:
        socket.create_connection((server, 445), timeout=3)
        print(f"✓ Can reach {server}:445")
    except Exception as e:
        print(f"✗ Cannot reach {server}:445")
        print(f"  Error: {e}")
        return False

    # Step 2: Try to access share
    print(f"\n[2] Access Share: {share}")
    try:
        files = list_smb_files(server, share, username, password, domain="WORKGROUP")
        print(f"✓ Success! Found {len(files)} files")
        return True
    except Exception as e:
        print(f"✗ Failed to access {share}")
        print(f"  Error: {e}")
        print("\nCheck:")
        print(f"  - Share name: {share}")
        print(f"  - Username: {username}")
        print(f"  - Password: correct?")
        print(f"  - Share exists on {server}?")
        return False


if __name__ == "__main__":
    success = main()
    print("\n" + "=" * 60)
    if success:
        print("STATUS: ✓ PASS")
    else:
        print("STATUS: ✗ FAIL")
    print("=" * 60 + "\n")


