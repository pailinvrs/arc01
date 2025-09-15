#!/usr/bin/env python3
# update_sqlite.py — Incremental update Raw_State from Base API (start strictly after last in-DB per type)
# - Keeps original schema (Raw_State id/timestamp/measurementLabel/state)
# - No schema/index changes
# - If a type has no rows in DB yet -> backfill only a small recent window (default 24h), not months

import argparse, os, sys, time, json, sqlite3
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
import requests

# -------- settings --------
DEFAULT_TYPES = [
    "intemp","inhumid","inco2","inpm25","inpm10","intvoc",
    "inlight","inmotion","inpressure","current","occcount","battery"
]
ICT = timezone(timedelta(hours=7))
# when a type has no rows in DB, fetch only this many hours back (NOT months)
BACKFILL_HOURS_ON_EMPTY = int(os.getenv("BACKFILL_HOURS_ON_EMPTY", "24"))

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", dest="db_path", required=True)
    ap.add_argument("--api-base", dest="api_base", required=True,
                    help="e.g., https://archcu-digitaltwin.mamgistics.com/arc01/api")
    ap.add_argument("--types", default=",".join(DEFAULT_TYPES))
    ap.add_argument("--chunk-minutes-default", type=int, default=1440)   # 1 day
    ap.add_argument("--chunk-minutes-occcount", type=int, default=60)
    ap.add_argument("--retries", type=int, default=3)
    return ap.parse_args()

# -------- helpers --------
def iso_min(dt: datetime) -> str:
    return dt.astimezone(ICT).strftime("%Y-%m-%dT%H:%M")

def parse_any_iso(s: str) -> datetime:
    # Python 3.11 handles tz offsets like "+07:00"
    return datetime.fromisoformat(s)

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
            # NDJSON (one object per line)
            return [json.loads(line) for line in text.splitlines() if line.strip()]
        except Exception as e:
            last_err = e
            time.sleep(1.0)
    raise last_err

def ensure_raw_state_exists(conn: sqlite3.Connection):
    # Keep original schema; create only if truly missing
    cur = conn.cursor()
    cur.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND lower(name)=lower('Raw_State');
    """)
    if not cur.fetchone():
        cur.execute("""
            CREATE TABLE Raw_State (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                measurementLabel TEXT,
                state REAL
            );
        """)
        conn.commit()

def get_last_ts_for_type(conn: sqlite3.Connection, measurement_type: str) -> datetime | None:
    """
    Return the latest timestamp in DB for the given measurement_type.
    If no rows exist for that type, return None.
    """
    q = """
    SELECT MAX(rs.timestamp)
    FROM Raw_State rs
    JOIN mim m ON rs.measurementLabel = m.measurementLabel
    WHERE lower(m.measurementType) = lower(?)
    """
    cur = conn.execute(q, (measurement_type,))
    val = cur.fetchone()[0] if cur else None
    return parse_any_iso(val) if val else None

def insert_rows_dedup(conn: sqlite3.Connection, rows):
    """
    Insert with per-row NOT EXISTS guard (no schema change required).
    rows: list of (timestamp:str, measurementLabel:str, state:float)
    """
    if not rows:
        return 0
    cur = conn.cursor()
    cur.executemany(
        """
        INSERT INTO Raw_State(timestamp, measurementLabel, state)
        SELECT ?, ?, ?
        WHERE NOT EXISTS (
          SELECT 1 FROM Raw_State
          WHERE timestamp = ? AND measurementLabel = ?
        )
        """,
        [(ts, lab, st, ts, lab) for (ts, lab, st) in rows]
    )
    conn.commit()
    # rowcount on executemany may not reflect total inserted; re-count via changes()
    return conn.total_changes

# -------- main incremental --------
def run():
    args = parse_args()
    api_base = args.api_base.rstrip("/")
    types = [t.strip() for t in args.types.split(",") if t.strip()]

    conn = sqlite3.connect(args.db_path)
    ensure_raw_state_exists(conn)

    now_ict = datetime.now(ICT)
    total_inserted = 0

    for t in types:
        last_dt = get_last_ts_for_type(conn, t)

        if last_dt is None:
            # No rows of this type in DB yet -> start only a small recent window (avoid months of backfill)
            start_dt = now_ict - timedelta(hours=BACKFILL_HOURS_ON_EMPTY)
            mode = f"INIT (no rows in DB; start last {BACKFILL_HOURS_ON_EMPTY}h)"
        else:
            # Start strictly after last timestamp to avoid duplicates
            start_dt = last_dt + timedelta(seconds=1)
            mode = f"APPEND (after {iso_min(last_dt)})"

        end_dt = now_ict
        if start_dt >= end_dt:
            print(f"[{t}] up-to-date (no fetch). {mode}")
            continue

        chunk_minutes = args.chunk_minutes_occcount if t == "occcount" else args.chunk_minutes_default
        print(f"[{t}] {mode} → range {iso_min(start_dt)} → {iso_min(end_dt)} (chunk={chunk_minutes}m)")

        t0 = start_dt
        type_inserts_before = total_inserted

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
                            lab = str(it["measurementLabel"]).strip()
                            st  = float(it["state"])
                            # keep only rows strictly after last_dt (if last_dt exists)
                            if (last_dt is None) or (parse_any_iso(ts) > last_dt):
                                batch.append((ts, lab, st))
                        except Exception:
                            continue
                    if batch:
                        # per-chunk dedup against DB
                        before = total_inserted
                        inserted_now = insert_rows_dedup(conn, batch)
                        total_inserted += inserted_now
                        print(f"  + {iso_min(t0)}–{iso_min(t1)} → ask:{len(batch)} ins:{inserted_now}")
                else:
                    print(f"  . {iso_min(t0)}–{iso_min(t1)} → 0 rows")
            except Exception as e:
                print(f"  ! {iso_min(t0)}–{iso_min(t1)} FAILED: {e}", file=sys.stderr)
                sys.exit(1)

            t0 = t1
            time.sleep(0.1)

        print(f"[{t}] done: inserted {total_inserted - type_inserts_before} rows")

    print(f"[summary] inserted total {total_inserted} rows into {args.db_path}")

    # Let workflow know whether to upload or skip
    out = os.getenv("INSERTED_FILE")
    if out:
        os.makedirs(os.path.dirname(out), exist_ok=True)
        with open(out, "w", encoding="utf-8") as f:
            f.write(str(total_inserted))

if __name__ == "__main__":
    run()
