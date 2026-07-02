"""
Restore .FBK backup to a new .FDB with default credentials.
Run: python restore-backup.py C:\path\to\backup.FBK C:\path\to\output.FDB
"""
import sys
import os
import subprocess

# Find bundled gbak
script_dir = os.path.dirname(os.path.abspath(__file__))
gbak = os.path.join(script_dir, "firebird", "bin", "gbak.exe")
if not os.path.isfile(gbak):
    print(f"ERROR: gbak.exe not found at {gbak}")
    sys.exit(1)

if len(sys.argv) < 3:
    print("Usage: python restore-backup.py <backup.FBK> <output.FDB>")
    print("Example: python restore-backup.py C:\\backup.FBK C:\\restored.FDB")
    sys.exit(1)

fbk_path = sys.argv[1]
fdb_path = sys.argv[2]

if not os.path.isfile(fbk_path):
    print(f"ERROR: Backup file not found: {fbk_path}")
    sys.exit(1)

# Set Firebird env for gbak
os.environ["FIREBIRD"] = os.path.join(script_dir, "firebird")

print(f"Restoring backup...")
print(f"  From: {fbk_path}")
print(f"  To:   {fdb_path}")
print(f"  User: SYSDBA / masterkey")
print()

# gbak -c (create) -user SYSDBA -pass masterkey backup.FBK output.FDB
result = subprocess.run(
    [gbak, "-c", "-user", "SYSDBA", "-pass", "masterkey", fbk_path, fdb_path],
    capture_output=True, text=True,
)

if result.returncode == 0:
    size = os.path.getsize(fdb_path) / 1024 / 1024
    print(f"SUCCESS! Restored to: {fdb_path} ({size:.1f} MB)")
    print()
    print("Use this path in .env:")
    print(f"  FIREBIRD_DSN={fdb_path}")
    print(f"  FIREBIRD_USER=SYSDBA")
    print(f"  FIREBIRD_PASSWORD=masterkey")
else:
    print(f"FAILED!")
    print(f"  {result.stderr.strip()}")
    sys.exit(1)
