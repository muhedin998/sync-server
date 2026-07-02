"""
Quick database connection test.
Run: python test-db.py [path_to_fdb]
"""
import sys
import os

# Load .env
from dotenv import load_dotenv
load_dotenv()

# Check for command-line argument
if len(sys.argv) > 1:
    dsn = sys.argv[1]
else:
    dsn = os.getenv("FIREBIRD_DSN", "").strip()

user = os.getenv("FIREBIRD_USER", "SYSDBA")
password = os.getenv("FIREBIRD_PASSWORD", "masterkey")

if not dsn:
    print("ERROR: No database path provided.")
    print("Usage: python test-db.py C:\\path\\to\\database.FDB")
    sys.exit(1)

if not os.path.isfile(dsn):
    print(f"ERROR: File not found: {dsn}")
    sys.exit(1)

print(f"Testing connection...")
print(f"  DSN:  {dsn}")
print(f"  User: {user}")
print()

# Set bundled Firebird
bundled = os.path.join(os.path.dirname(os.path.abspath(__file__)), "firebird", "bin")
if os.path.isdir(bundled):
    os.environ["FIREBIRD"] = bundled
    print(f"  Firebird: {bundled}")
else:
    print(f"  Firebird: system")

print()

try:
    import fdb
    conn = fdb.connect(dsn=dsn, user=user, password=password, charset="UTF8")
    cur = conn.cursor()

    # Count products
    cur.execute("SELECT COUNT(*) FROM ARTIKAL WHERE NE_KORISTI_SE IS NULL OR NE_KORISTI_SE = 0")
    count = cur.fetchone()[0]

    # Count barcodes
    try:
        cur.execute("SELECT COUNT(*) FROM ARTIKAL_BARKOD WHERE BARKOD IS NOT NULL AND BARKOD != ''")
        barcodes = cur.fetchone()[0]
    except:
        barcodes = "N/A (table not found)"

    # Count prices
    try:
        cur.execute("SELECT COUNT(*) FROM RM_TRENUTNO_STANJE WHERE COALESCE(PROD_CENA_SA_P, PROD_CENA_BEZ_P) IS NOT NULL")
        prices = cur.fetchone()[0]
    except:
        prices = "N/A (table not found)"

    conn.close()

    print("SUCCESS!")
    print(f"  Active products: {count}")
    print(f"  Barcodes:        {barcodes}")
    print(f"  Prices:          {prices}")

except Exception as e:
    print(f"FAILED!")
    print(f"  Error: {e}")
    sys.exit(1)
