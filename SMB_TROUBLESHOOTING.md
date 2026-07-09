# SMB Connection Troubleshooting Guide

## Error: STATUS_BAD_NETWORK_NAME

This error means the SMB server cannot find the share name you specified.

### Root Causes

1. **Share Name is Incorrect**
   - Wrong spelling or capitalization
   - Share doesn't exist on the server
   
2. **Domain/Workgroup Issue**
   - Not using the correct domain name for authentication
   - Workgroup vs Domain authentication mismatch
   
3. **Access Permissions**
   - User account doesn't have access to this share
   - Share is restricted to specific users/groups
   
4. **Network Issues**
   - Server is not reachable
   - Firewall blocking SMB (port 445)

---

## Troubleshooting Steps

### Step 1: Verify Share Exists

On the SMB server (Windows machine), open Command Prompt and run:
```cmd
net share
```

This lists all available shares. Look for your share name.

Expected output:
```
Share name    LENOVO-P50
Remote path   C:\Users\chengwang\Documents\LENOVO-P50
Type          Disk
Users
Comment
Path          C:\Users\chengwang\Documents\LENOVO-P50
Maximum users No limit
```

### Step 2: Check Share Name Capitalization

Share names are case-insensitive but some systems are picky. Try:
- Original: `LENOVO-P50`
- Uppercase: `LENOVO-P50`
- Lowercase: `lenovo-p50`

Example:
```python
# Try uppercase
list_smb_files("192.168.1.4", "LENOVO-P50", username, password)

# Or lowercase
list_smb_files("192.168.1.4", "lenovo-p50", username, password)
```

### Step 3: Identify Your Domain/Workgroup

On the SMB server, check the domain/workgroup:

**Windows Command Prompt:**
```cmd
wmic os get name,version
systeminfo | findstr /I "domain"
```

Or check Settings:
- Windows 11/10: Settings > System > About
- Look for "Domain:" or "Workgroup:" field

Common values:
- `WORKGROUP` (default for non-domain systems)
- `DOMAIN` (if joined to corporate domain)
- Your actual domain name (e.g., `CORP.EXAMPLE.COM`)

### Step 4: Update Your Code

```python
from src.SMBTest import list_smb_files
import os

# Set environment variables
os.environ["SMB_USERNAME"] = "chengwang2019"
os.environ["SMB_PASSWORD"] = "Password2019"

# Option A: Without domain (for WORKGROUP)
try:
    files = list_smb_files("192.168.1.4", "LENOVO-P50")
    print(f"Success! Found {len(files)} files")
except Exception as e:
    print(f"Failed: {e}")

# Option B: With WORKGROUP explicitly
try:
    files = list_smb_files("192.168.1.4", "LENOVO-P50", domain="WORKGROUP")
    print(f"Success! Found {len(files)} files")
except Exception as e:
    print(f"Failed: {e}")

# Option C: With corporate domain
try:
    files = list_smb_files("192.168.1.4", "LENOVO-P50", domain="YOURDOMAIN")
    print(f"Success! Found {len(files)} files")
except Exception as e:
    print(f"Failed: {e}")
```

### Step 5: Test Network Connectivity

Test if the server is reachable:

**macOS/Linux Terminal:**
```bash
# Test if host is reachable
ping 192.168.1.4

# Test if SMB port is open
nc -zv 192.168.1.4 445

# Or use telnet
telnet 192.168.1.4 445
```

**Expected output:**
```
Connected to 192.168.1.4
Escape character is ']'.
```

If connection is refused or times out, the server is not reachable or firewall is blocking it.

### Step 6: Verify Credentials

Make sure your username and password are correct:

```bash
export SMB_USERNAME="chengwang2019"
export SMB_PASSWORD="Password2019"
```

Run the test:
```bash
python -c "from src.SMBTest import list_smb_files; list_smb_files('192.168.1.4', 'LENOVO-P50')"
```

---

## Common Solutions

### Solution 1: Workgroup System (Most Common)

If the server is not on a domain:

```python
files = list_smb_files(
    server="192.168.1.4",
    share="LENOVO-P50",
    domain="WORKGROUP"  # Add this
)
```

### Solution 2: Domain-Joined System

If the server is on a corporate domain:

```python
files = list_smb_files(
    server="192.168.1.4",
    share="LENOVO-P50",
    domain="CORP"  # Your actual domain name
)
```

### Solution 3: Username Format for Domain

If using a domain, try different username formats:

```python
# Option A: Simple username
files = list_smb_files("192.168.1.4", "LENOVO-P50", "chengwang2019", "Password2019", domain="CORP")

# Option B: DOMAIN\username format (it gets auto-formatted)
# Don't add it yourself, just use simple username with domain parameter
```

### Solution 4: Try IPC$ Share First

The IPC$ share should be available on all systems. Use this to test basic connectivity:

```python
from src.SMBTest import get_available_shares

try:
    shares = get_available_shares("192.168.1.4")
    print(f"Available shares: {shares}")
except Exception as e:
    print(f"Cannot access IPC$: {e}")
    print("This means basic SMB connectivity is broken")
```

---

## Diagnostic Checklist

- [ ] Server IP address is correct: `192.168.1.4`
- [ ] Share name exists on server (verified with `net share`)
- [ ] Share name spelling is correct
- [ ] Domain/Workgroup name is correct
- [ ] Username is correct
- [ ] Password is correct
- [ ] Credentials are set in environment variables
- [ ] Network can reach the server (ping succeeds)
- [ ] Port 445 is open (firewall allows SMB)
- [ ] User has read access to the share

---

## Full Diagnostic Run

```python
import os
import socket
from src.SMBTest import list_smb_files, get_available_shares

print("=" * 70)
print("SMB DIAGNOSTIC REPORT")
print("=" * 70)

server = "192.168.1.4"
share = "LENOVO-P50"

# 1. Check environment
print("\n📋 ENVIRONMENT CHECK:")
print(f"  SMB_USERNAME set: {'✓' if os.getenv('SMB_USERNAME') else '✗'}")
print(f"  SMB_PASSWORD set: {'✓' if os.getenv('SMB_PASSWORD') else '✗'}")

# 2. Check network
print("\n🌐 NETWORK CHECK:")
try:
    socket.create_connection((server, 445), timeout=2)
    print(f"  ✓ Can connect to {server}:445")
except Exception as e:
    print(f"  ✗ Cannot reach {server}:445 - {e}")

# 3. Try IPC$
print("\n📁 IPC$ SHARE TEST:")
try:
    get_available_shares(server)
    print(f"  ✓ Can access IPC$ (basic connectivity works)")
except Exception as e:
    print(f"  ✗ Cannot access IPC$ - {e}")

# 4. Try actual share (no domain)
print(f"\n📂 SHARE TEST (no domain): {share}")
try:
    list_smb_files(server, share)
    print(f"  ✓ SUCCESS: Can access {share}")
except Exception as e:
    print(f"  ✗ Failed (no domain) - {e}")

# 5. Try with WORKGROUP
print(f"\n📂 SHARE TEST (WORKGROUP): {share}")
try:
    list_smb_files(server, share, domain="WORKGROUP")
    print(f"  ✓ SUCCESS: Can access {share} with WORKGROUP")
except Exception as e:
    print(f"  ✗ Failed (WORKGROUP) - {e}")

print("\n" + "=" * 70)
```

---

## Additional Resources

- [smbprotocol Documentation](https://github.com/jborean93/smbprotocol)
- [SMB/CIFS Protocol](https://en.wikipedia.org/wiki/Server_Message_Block)
- [Windows SMB Shares Documentation](https://docs.microsoft.com/en-us/windows/win32/fileio/smb-overview)

