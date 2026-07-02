"""
ACIS Sync Server — Firebird / File → REST API

Memory-lean: streams products in batches, never loads full dataset.
One sync at a time (concurrent requests get 503).

Source mode:
    FIREBIRD_DSN set  → Firebird server or embedded
    FILE_SOURCE set   → CSV or JSON file
    (if both set, Firebird takes priority)

Usage:
    start.bat                   # recommended
    python main.py              # uses .env config
"""

import os
import gc
import csv
import zlib
import json
import logging
import time
import socket
import struct
import threading
from datetime import datetime, timezone

from starlette.requests import Request
from dotenv import load_dotenv
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse

load_dotenv()

# ── Config ──────────────────────────────────────────────────────────

_BUNDLED_FIREBIRD = os.path.join(os.path.dirname(os.path.abspath(__file__)), "firebird", "bin")
if os.path.isdir(_BUNDLED_FIREBIRD):
    os.environ.setdefault("FIREBIRD", _BUNDLED_FIREBIRD)

FIREBIRD_DSN          = os.getenv("FIREBIRD_DSN", "").strip()
FIREBIRD_DSN_FALLBACK = os.getenv("FIREBIRD_DSN_FALLBACK", "").strip()
FIREBIRD_USER         = os.getenv("FIREBIRD_USER", "SYSDBA")
FIREBIRD_PASSWORD     = os.getenv("FIREBIRD_PASSWORD", "masterkey")
FIREBIRD_READONLY     = os.getenv("FIREBIRD_READONLY", "false").lower() == "true"

FILE_SOURCE       = os.getenv("FILE_SOURCE", "").strip()
FILE_ENCODING     = os.getenv("FILE_ENCODING", "utf-8")
FILE_DELIMITER    = os.getenv("FILE_DELIMITER", ";")

# Column mapping for CSV/JSON (env or defaults)
COL_ID            = os.getenv("COL_ID", "SIFRA")
COL_SIFRA         = os.getenv("COL_SIFRA", "SIFRA")
COL_BARCODE       = os.getenv("COL_BARCODE", "BARCODE")
COL_NAZIV         = os.getenv("COL_NAZIV", "NAZIV")
COL_CENA          = os.getenv("COL_CENA", "CENA")
COL_GRUPA         = os.getenv("COL_GRUPA", "GRUPA")
COL_JEDINICA      = os.getenv("COL_JEDINICA", "JEDINICA_MERE")
COL_AKTIVAN       = os.getenv("COL_AKTIVAN", "AKTIVAN")

SYNC_HOST         = os.getenv("SYNC_HOST", "0.0.0.0")
SYNC_PORT         = int(os.getenv("SYNC_PORT", "8765"))
STREAM_BATCH      = int(os.getenv("STREAM_BATCH", "1000"))
BROADCAST_PORT    = int(os.getenv("BROADCAST_PORT", "8766"))

# Determine source mode
USE_FILE = not FIREBIRD_DSN and bool(FILE_SOURCE)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("sync-server")

# ── File source ─────────────────────────────────────────────────────

_file_products = []       # list of compact dicts
_file_loaded_at = None

def _load_file_products():
    """Load products from CSV or JSON file into memory."""
    global _file_products, _file_loaded_at
    if not FILE_SOURCE:
        raise RuntimeError("FILE_SOURCE not set")
    if not os.path.isfile(FILE_SOURCE):
        raise FileNotFoundError(f"File not found: {FILE_SOURCE}")

    ext = os.path.splitext(FILE_SOURCE)[1].lower()
    t0 = time.time()

    if ext == '.csv':
        with open(FILE_SOURCE, 'r', encoding=FILE_ENCODING) as f:
            reader = csv.DictReader(f, delimiter=FILE_DELIMITER)
            rows = list(reader)
    elif ext == '.json':
        with open(FILE_SOURCE, 'r', encoding=FILE_ENCODING) as f:
            rows = json.load(f)
    else:
        raise ValueError(f"Unsupported file format: {ext}. Use .csv or .json")

    products = []
    for i, row in enumerate(rows):
        def get(col_env, fallback=''):
            val = row.get(col_env, '')
            return str(val).strip() if val else fallback

        raw_id = get(COL_ID)
        pid = int(raw_id) if raw_id.isdigit() else i + 1

        products.append({
            "id":  pid,
            "s":   get(COL_SIFRA, str(pid)),
            "b":   get(COL_BARCODE),
            "n":   get(COL_NAZIV, 'BEZ NAZIVA'),
            "c":   _to_float(row.get(COL_CENA)),
            "g":   get(COL_GRUPA) or None,
            "j":   get(COL_JEDINICA) or None,
            "src": "ACIS",
            "a":   1 if str(row.get(COL_AKTIVAN, '1')).strip() not in ('0', 'false', 'False') else 0,
            "ca":  "2000-01-01T00:00:00.000000",
            "ua":  "2000-01-01T00:00:00.000000",
        })

    _file_products = products
    _file_loaded_at = datetime.now(timezone.utc)
    log.info(f"[file] Loaded {len(products)} products from {FILE_SOURCE} in {time.time()-t0:.1f}s")
    return products

# ── Sync guard — only one sync at a time, auto-expires if client disconnects ──

_sync_active   = False
_sync_started  = 0.0
_sync_guard    = threading.Lock()   # protects the two flags above
_SYNC_MAX_SECS = 120                # force-release stale lock after 2 min

def _acquire_sync() -> bool:
    global _sync_active, _sync_started
    with _sync_guard:
        if _sync_active and (time.time() - _sync_started) < _SYNC_MAX_SECS:
            return False
        if _sync_active:
            log.warning("Stale sync lock — force-releasing after timeout")
        _sync_active  = True
        _sync_started = time.time()
        return True

def _release_sync():
    global _sync_active
    with _sync_guard:
        _sync_active = False

# ── Firebird (only imported/used when DSN is set) ───────────────────

_active_dsn = None  # resolved after first successful connect

def _connect(dsn: str):
    """Connect to Firebird."""
    import fdb
    return fdb.connect(dsn=dsn, user=FIREBIRD_USER,
                       password=FIREBIRD_PASSWORD, charset="UTF8")

def _connection():
    global _active_dsn
    if not FIREBIRD_DSN:
        raise RuntimeError("FIREBIRD_DSN not set — running in file mode")

    # Already resolved — use cached DSN
    if _active_dsn:
        return _connect(_active_dsn)

    # Try main DSN first
    try:
        conn = _connect(FIREBIRD_DSN)
        _active_dsn = FIREBIRD_DSN
        log.info(f"Connected to: {FIREBIRD_DSN}")
        return conn
    except Exception as e:
        if not FIREBIRD_DSN_FALLBACK:
            raise
        log.warning(f"Main DB failed ({e}), trying fallback...")

    # Try fallback DSN
    conn = _connect(FIREBIRD_DSN_FALLBACK)
    _active_dsn = FIREBIRD_DSN_FALLBACK
    log.info(f"Connected to fallback: {FIREBIRD_DSN_FALLBACK}")
    return conn

def _fetch_one(sql: str, params: tuple = ()):
    conn = _connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        conn.close()

def _iter_batches(sql: str, params: tuple = (), batch_size: int = STREAM_BATCH):
    """
    Execute SQL and yield batches of (columns, rows) tuples.
    Opens one connection, streams through cursor — never loads all rows at once.
    """
    conn = _connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        while True:
            batch = cur.fetchmany(batch_size)
            if not batch:
                break
            yield cols, batch
    finally:
        conn.close()

# ── Product SQL ─────────────────────────────────────────────────────

# Prices and barcodes are fetched separately into dicts and merged in Python.
# This keeps the main query simple (no heavy JOIN on RM_TRENUTNO_STANJE)
# and fast on large catalogs.
_PRODUCT_SELECT = """
    SELECT
        a.ID,
        a.SIFRA,
        a.NAZIV,
        COALESCE(jm.OZNAKA, '') as JEDINICA_MERE,
        COALESCE(ag.NAZIV, '') as GRUPA,
        COALESCE(a.DATUM_RADA || 'T' || a.VREME_RADA,
                 '2000-01-01T00:00:00.000000') as DATUM_VREME
    FROM ARTIKAL a
    LEFT JOIN JEDINICA_MERE jm ON a.JEDINICA_MERE_ID = jm.ID
    LEFT JOIN ARTIKAL_GRUPA ag ON a.ARTIKAL_GRUPA_ID = ag.ID
    WHERE (a.NE_KORISTI_SE IS NULL OR a.NE_KORISTI_SE = 0)
"""

FULL_SQL = _PRODUCT_SELECT + " ORDER BY a.ID"
DELTA_SQL = _PRODUCT_SELECT + """
    AND COALESCE(a.DATUM_RADA || 'T' || a.VREME_RADA,
                 '2000-01-01T00:00:00.000000') > ?
    ORDER BY a.ID
"""

# ── Lookup caches (barcodes + prices) ───────────────────────────────

_barcode_cache = None
_barcode_lock  = threading.Lock()

_price_cache = None
_price_lock  = threading.Lock()

def _load_all_barcodes():
    """Load all (ARTIKAL_ID, BARKOD) pairs into a dict: {artikal_id: 'barcode1 barcode2 ...'}."""
    global _barcode_cache
    with _barcode_lock:
        if _barcode_cache is not None:
            return _barcode_cache
        log.info("Loading barcode index...")
        t0 = time.time()
        conn = _connection()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT ARTIKAL_ID, TRIM(BARKOD) as BARKOD
                FROM ARTIKAL_BARKOD
                WHERE (PODRAZUMVENA_VREDNOST = 1 OR PODRAZUMVENA_VREDNOST IS NULL)
                  AND (NE_KORISTI_SE IS NULL OR NE_KORISTI_SE = 0)
                  AND BARKOD IS NOT NULL
                  AND BARKOD != ''
                ORDER BY ARTIKAL_ID, PODRAZUMVENA_VREDNOST DESC NULLS LAST
            """)
            by_id = {}
            for art_id, barcode in cur:
                b = barcode.strip()
                if art_id not in by_id:
                    by_id[art_id] = b
                elif b not in by_id[art_id]:
                    by_id[art_id] += ' ' + b
            _barcode_cache = by_id
        finally:
            conn.close()
        log.info(f"Barcode index: {len(_barcode_cache)} products with barcodes in {time.time()-t0:.1f}s")
        return _barcode_cache

def _load_all_prices():
    """Load all (ARTIKAL_ID → price) into a dict from RM_TRENUTNO_STANJE."""
    global _price_cache
    with _price_lock:
        if _price_cache is not None:
            return _price_cache
        log.info("Loading price index...")
        t0 = time.time()
        conn = _connection()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT ARTIKAL_ID,
                       COALESCE(PROD_CENA_SA_P, PROD_CENA_BEZ_P) as CENA
                FROM RM_TRENUTNO_STANJE
                WHERE COALESCE(PROD_CENA_SA_P, PROD_CENA_BEZ_P) IS NOT NULL
            """)
            _price_cache = {row[0]: row[1] for row in cur}
        finally:
            conn.close()
        log.info(f"Price index: {len(_price_cache)} products with prices in {time.time()-t0:.1f}s")
        return _price_cache

def _row_to_compact(r: dict, barcodes: dict, prices: dict) -> dict:
    return {
        "id":  r["ID"],
        "s":   (r["SIFRA"] or "").strip(),
        "b":   barcodes.get(r["ID"], ""),
        "n":   (r["NAZIV"] or "BEZ NAZIVA").strip(),
        "c":   _to_float(prices.get(r["ID"])),
        "g":   r["GRUPA"] or None,
        "j":   r["JEDINICA_MERE"] or None,
        "src": "ACIS",
        "a":   1,
        "ca":  r["DATUM_VREME"],
        "ua":  r["DATUM_VREME"],
    }

def _to_float(val):
    if val is None: return None
    try: return float(val)
    except (ValueError, TypeError): return None

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")

def _make_product_stream(sql: str, params: tuple, barcodes: dict, prices: dict, count_ref: list):
    """
    Generator that yields gzip-compressed JSON chunks of the product array.

    Uses zlib streaming (Z_SYNC_FLUSH after each batch) so the HTTP response
    headers are sent immediately and data flows to the client as it is produced,
    instead of buffering the entire body before sending a single byte.
    count_ref is a one-element list used to report the final count back to the caller.
    """
    cobj = zlib.compressobj(level=6, method=zlib.DEFLATED, wbits=31)  # wbits=31 → gzip
    first = True

    yield cobj.compress(b'[')

    for cols, batch in _iter_batches(sql, params):
        for row in batch:
            d = dict(zip(cols, row))
            item = _row_to_compact(d, barcodes, prices)
            chunk = json.dumps(item, ensure_ascii=False).encode("utf-8")
            if not first:
                chunk = b',' + chunk
            else:
                first = False
            data = cobj.compress(chunk)
            if data:
                yield data
            count_ref[0] += 1
        # Flush after each batch so the client receives data progressively
        flushed = cobj.flush(zlib.Z_SYNC_FLUSH)
        if flushed:
            yield flushed
        del batch

    yield cobj.compress(b']')
    yield cobj.flush(zlib.Z_FINISH)

# ── FastAPI app ─────────────────────────────────────────────────────

app = FastAPI(title="ACIS Sync Server", version="1.4.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routes ──────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    try:
        if USE_FILE:
            if not _file_products:
                _load_file_products()
            count = len(_file_products)
            source = "file"
        else:
            count = _fetch_one(
                "SELECT COUNT(*) FROM ARTIKAL WHERE NE_KORISTI_SE IS NULL OR NE_KORISTI_SE = 0"
            ) or 0
            source = "firebird"
        return {
            "status": "ok", "source": source,
            "productCount": count, "serverVersion": "1.4.0",
            "serverTime": now_iso(),
        }
    except Exception as e:
        log.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=f"Error: {e}")


@app.get("/api/sync/products/count")
def product_count():
    try:
        if USE_FILE:
            if not _file_products:
                _load_file_products()
            return {"count": len([p for p in _file_products if p["a"] == 1])}
        count = _fetch_one(
            "SELECT COUNT(*) FROM ARTIKAL WHERE NE_KORISTI_SE IS NULL OR NE_KORISTI_SE = 0"
        )
        return {"count": count or 0}
    except Exception as e:
        log.error(f"Count failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sync/products")
def sync_products(
    request: Request,
    since: str = Query("", description="ISO timestamp — only products updated after this"),
):
    mode = "DELTA" if since else "FULL"
    client_ip = request.client.host if request.client else "?"
    t_start = time.time()

    if not _acquire_sync():
        log.warning(f"[{client_ip}] {mode} sync REJECTED — another sync already in progress")
        raise HTTPException(status_code=503, detail="Sync already in progress, retry shortly")

    # ── File mode: stream directly from memory ───────────────────────
    if USE_FILE:
        if not _file_products:
            try:
                _load_file_products()
            except Exception as e:
                _release_sync()
                log.error(f"[{client_ip}] File load FAILED: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        server_time = now_iso()
        active = [p for p in _file_products if p["a"] == 1]
        log.info(f"[{client_ip}] {mode} sync started (file, {len(active)} products)")

        def generate():
            try:
                cobj = zlib.compressobj(level=6, method=zlib.DEFLATED, wbits=31)
                yield cobj.compress(b'[')
                first = True
                count = 0
                for item in active:
                    chunk = json.dumps(item, ensure_ascii=False).encode("utf-8")
                    if not first:
                        chunk = b',' + chunk
                    else:
                        first = False
                    data = cobj.compress(chunk)
                    if data:
                        yield data
                    count += 1
                yield cobj.compress(b']')
                yield cobj.flush(zlib.Z_FINISH)
                elapsed = time.time() - t_start
                log.info(f"[{client_ip}] {mode} sync done: {count} products | total={elapsed:.1f}s")
            finally:
                _release_sync()

        return StreamingResponse(generate(), media_type="application/octet-stream", headers={
            "X-Server-Time": server_time,
            "X-Deactivated-Count": "0",
        })

    # ── Firebird mode ───────────────────────────────────────────────
    try:
        barcodes = _load_all_barcodes()
        prices   = _load_all_prices()

        deactivated_ids = []
        if since:
            conn = _connection()
            try:
                cur = conn.cursor()
                cur.execute("""
                    SELECT a.ID FROM ARTIKAL a
                    WHERE a.NE_KORISTI_SE = 1
                      AND COALESCE(a.DATUM_RADA || 'T' || a.VREME_RADA,
                                   '2000-01-01T00:00:00.000000') > ?
                """, (since,))
                deactivated_ids = [r[0] for r in cur.fetchall()]
            finally:
                conn.close()
    except Exception as e:
        _release_sync()
        log.error(f"[{client_ip}] {mode} sync pre-fetch FAILED: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    sql    = DELTA_SQL if since else FULL_SQL
    params = (since,) if since else ()
    server_time = now_iso()
    count_ref   = [0]

    log.info(f"[{client_ip}] {mode} sync started" + (f" since={since}" if since else ""))

    def generate():
        try:
            yield from _make_product_stream(sql, params, barcodes, prices, count_ref)
            gc.collect()
            elapsed = time.time() - t_start
            deact_info = f", {len(deactivated_ids)} deactivated" if deactivated_ids else ""
            log.info(
                f"[{client_ip}] {mode} sync done: {count_ref[0]} products{deact_info}"
                f" | total={elapsed:.1f}s"
            )
        except Exception as e:
            log.error(f"[{client_ip}] {mode} sync stream FAILED: {e}")
            raise
        finally:
            _release_sync()

    headers = {
        "X-Deactivated-Count": str(len(deactivated_ids)),
        "X-Server-Time": server_time,
    }
    if deactivated_ids:
        headers["X-Deactivated-IDs"] = ",".join(str(i) for i in deactivated_ids)

    return StreamingResponse(generate(), media_type="application/octet-stream", headers=headers)


# ── UDP Broadcast (auto-discovery) ──────────────────────────────────

def _get_local_ip():
    """Get the local LAN IP address."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"

def _udp_broadcast_loop():
    """Broadcast server presence every 3 seconds on UDP."""
    local_ip = _get_local_ip()
    msg = json.dumps({
        "name": "latko-sync",
        "ip": local_ip,
        "port": SYNC_PORT,
        "version": "1.5.0",
    }).encode("utf-8")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.settimeout(1)

    while True:
        try:
            sock.sendto(msg, ("255.255.255.255", BROADCAST_PORT))
            sock.sendto(msg, (local_ip, BROADCAST_PORT))
        except Exception:
            pass
        time.sleep(3)

@app.get("/api/discover")
def discover():
    """HTTP fallback for discovery — app can probe this endpoint."""
    return {
        "name": "latko-sync",
        "ip": _get_local_ip(),
        "port": SYNC_PORT,
        "version": "1.5.0",
    }


# ── Startup ─────────────────────────────────────────────────────────

@app.on_event("startup")
def startup():
    mode = "FILE" if USE_FILE else "FIREBIRD"
    log.info(f"ACIS Sync Server v1.5.0 (mode={mode})")
    log.info(f"Listening on {SYNC_HOST}:{SYNC_PORT}")
    log.info(f"UDP broadcast on port {BROADCAST_PORT}")

    # Start UDP broadcast thread
    t = threading.Thread(target=_udp_broadcast_loop, daemon=True)
    t.start()

    if USE_FILE:
        log.info(f"File source: {FILE_SOURCE}")
        try:
            _load_file_products()
        except Exception as e:
            log.warning(f"File load failed: {e}")
    else:
        log.info(f"Firebird DSN: {FIREBIRD_DSN}")
        log.info(f"Bundled Firebird: {os.environ.get('FIREBIRD', 'not set')}")
        if FIREBIRD_READONLY:
            log.info("READ-ONLY mode — no writes will be attempted")
        try:
            count = _fetch_one(
                "SELECT COUNT(*) FROM ARTIKAL WHERE NE_KORISTI_SE IS NULL OR NE_KORISTI_SE = 0"
            )
            log.info(f"Firebird connected — {count} active products")
            _load_all_barcodes()
            _load_all_prices()
        except Exception as e:
            log.warning(f"Firebird connection failed: {e}")


# ── Main ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=SYNC_HOST, port=SYNC_PORT, log_level="info")
