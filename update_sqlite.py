#!/usr/bin/env python3
# update_sqlite.py — Incremental update Raw_State from Base API (keeping original schema)
import argparse, os, sys, time, json, sqlite3
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
import requests

# ---------- Config via env/args ----------
DEFAULT_TYPES = [
    "intemp","inhumid","inco2","inpm25","inpm10","intvoc",
    "inlight","inmotion","inpressure","current","occcount","battery"
]

ICT = timezone(timedelta(hours=7))  # Bangkok time
DEFAULT_START = "2025-03-26T00:00:00+07:00"  # first-available as per project notes

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", dest="db_path", required=True)
    ap.add_argument("--api-base", dest="api_base", required=True,
                    help="e.g., https://archcu-digitaltwin.mamgistics.com/arc01/api")
    ap.add_argument("--types", default=",".join(DEFAULT_TYPES))
    ap.add_argument("--chunk-minutes-default", type=int, default=180)
    ap.add_argument("--chunk-minutes-occcount", type=int, default=60)
    ap.add_argument("--retries", type=int, default=3)
    return ap.parse_args()

# ---------- Helpers ----------
def iso_min(dt: datetime) -> str:
    # API expects minute precision; timestamp is already in ICT
    return dt.astimezone(ICT).strftime("%Y-%m-%dT%H:%M")

def parse_any_iso(s: str) -> datetime:
    # Python 3.11 supports fromisoformat with TZ offsets like +07:00
    try:
        return datetime.fromisoformat(s)
    except Exception:
        # try without seconds
        try:
            return datetime.fromisoformat(s + ":00")
        except Exception:
            raise

def http_get_json_or_ndjson(url: str, retries: int = 3):
    last_err = None
    for _ in range(max(1, retries)):
        try:
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            text = r.text.strip()
            if not text:
                return []
            if text.startswith("["):
                return r.json()
            # NDJSON
            return [json.loads(line) for line in text.splitlines() if line.strip()]
        except Exception as e:
            last_err = e
            time.sleep(1.0)
    raise last_err

def ensure_raw_state_exists(conn: sqlite3.Connection):
    # Keep original schema names/cases
    cur = conn.cursor()
    cur.execute("""
        SELECT name FROM sqlite_master WHERE type='table' AND lower(name)=lower('Raw_State');
    """)
    row = cur.fetchone()
    if not row:
        # Create with your original columns
        cur.execute("""
            CREATE TABLE Raw_State (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                measurementLabel TEXT,
                state REAL
            );
        """)
        conn.commit()

def get_last_ts_for_type(conn: sqlite3.Connection, measurement_type: str) -> datetime:
    """
    Use MIM mapping to find last timestamp PER TYPE.
    If no rows found, return DEFAULT_START in ICT.
    """
    q = """
    SELECT MAX(rs.timestamp)
    FROM Raw_State rs
    JOIN mim m ON rs.measurementLabel = m.measurementLabel
    WHERE m.measurementType = ?
    """
    cur = conn.execute(q, (measurement_type,))
    val = cur.fetchone()[0] if cur else None
    if val:
        return parse_any_iso(val)
    return datetime.fromisoformat(DEFAULT_START)

def insert_rows(conn: sqlite3.Connection, rows):
    """
    rows: list of (timestamp:str, measurementLabel:str, state:float)
    """
    if not rows:
        return 0
    cur = conn.cursor()
    cur.executemany(
        "INSERT INTO Raw_State(timestamp, measurementLabel, state) VALUES (?, ?, ?)",
        rows
    )
    conn.commit()
    return len(rows)

# ---------- Main incremental ----------
def run():
    args = parse_args()
    api_base = args.api_base.rstrip("/")
    types = [t.strip() for t in args.types.split(",") if t.strip()]

    conn = sqlite3.connect(args.db_path)
    ensure_raw_state_exists(conn)

    now_ict = datetime.now(ICT)

    total_inserted = 0
    for t in types:
        # 1) last-ts per type via MIM
        last_dt = get_last_ts_for_type(conn, t)

        # ถ้าข้อมูลใน DB ล้ำหน้า now (ไม่ควรเกิด) ให้ถอยกลับเล็กน้อย
        if last_dt > now_ict:
            last_dt = now_ict - timedelta(minutes=5)

        # Start strictly after last timestamp to avoid duplicates
        start_dt = last_dt + timedelta(seconds=1)
        end_dt   = now_ict

        if start_dt >= end_dt:
            print(f"[{t}] up-to-date (<= {iso_min(end_dt)})")
            continue

        chunk_minutes = args.chunk_minutes_occcount if t == "occcount" else args.chunk_minutes_default
        print(f"[{t}] range {iso_min(start_dt)} → {iso_min(end_dt)} (chunk={chunk_minutes}m)")

        t0 = start_dt
        type_inserts = 0
        while t0 < end_dt:
            t1 = min(t0 + timedelta(minutes=chunk_minutes), end_dt)
            qs = urlencode({"from": iso_min(t0), "to": iso_min(t1)})
            url = f"{api_base}/range/{t}?{qs}"

            try:
                items = http_get_json_or_ndjson(url, retries=args.retries)
                if items:
                    batch = []
                    for it in items:
                        try:
                            ts = str(it["timestamp"]).strip()
                            label = str(it["measurementLabel"]).strip()
                            state = float(it["state"])
                            # guard: keep only rows strictly > last_dt
                            if parse_any_iso(ts) > last_dt:
                                batch.append((ts, label, state))
                        except Exception:
                            # skip malformed
                            continue
                    if batch:
                        n = insert_rows(conn, batch)
                        type_inserts += n
                        print(f"  + {iso_min(t0)}–{iso_min(t1)} → {n} rows")
                else:
                    print(f"  . {iso_min(t0)}–{iso_min(t1)} → 0 rows")
            except Exception as e:
                # fail the whole run (as requested)
                print(f"  ! {iso_min(t0)}–{iso_min(t1)} FAILED: {e}", file=sys.stderr)
                sys.exit(1)

            t0 = t1
            time.sleep(0.15)

        print(f"[{t}] done: inserted {type_inserts} rows")
        total_inserted += type_inserts

    # Summary
    print(f"[summary] inserted total {total_inserted} rows into {args.db_path}")

if __name__ == "__main__":
    run()
