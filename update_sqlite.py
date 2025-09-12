#!/usr/bin/env python3
# update_sqlite.py — sync ARC01 SQLite (AIM/MIM full upsert + Raw_State incremental)
import os, time, requests, sqlite3
from datetime import datetime, timedelta, timezone

API_BASE = os.getenv("API_BASE", "https://archcu-digitaltwin.mamgistics.com")
DB_PATH  = os.getenv("DB_PATH", "arc01.db")

ICT = timezone(timedelta(hours=7))  # Bangkok

# ทั้งหมดตามเอกสาร + ที่คุณเพิ่ม
BASE_TYPES = [
    "intemp","inhumid","inco2","inpm25","inpm10","intvoc",
    "inlight","inmotion","inpressure",
    "current","occcount","battery"
]

# ลองทั้ง 2 เส้นทาง (บางระบบตั้ง base path ต่างกัน)
API_PREFIXES = ["/arc01/api", ""]

def ensure_schema(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS Raw_State(
        timestamp TEXT NOT NULL,
        measurementLabel TEXT NOT NULL,
        state REAL NOT NULL,
        PRIMARY KEY (timestamp, measurementLabel)
    );""")
    cur.execute("""CREATE TABLE IF NOT EXISTS MIM(
        measurementLabel TEXT PRIMARY KEY,
        measurementType TEXT,
        zoneName TEXT,
        deviceFriendlyName TEXT
    );""")
    cur.execute("""CREATE TABLE IF NOT EXISTS AIM(
        deviceFriendlyName TEXT PRIMARY KEY,
        building TEXT, floor TEXT, room TEXT,
        deviceClass TEXT, macAddress TEXT, ipAddress TEXT
    );""")
    cur.execute("""CREATE TABLE IF NOT EXISTS Meta(
        source_key TEXT PRIMARY KEY,
        last_ts TEXT
    );""")
    conn.commit()

# ---------- helpers ----------
def fmt_min(dt: datetime) -> str:
    return dt.astimezone(ICT).strftime("%Y-%m-%dT%H:%M")

def parse_dt_guess(s: str) -> datetime:
    s = (s or "").strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S%z","%Y-%m-%dT%H:%M%z","%Y-%m-%dT%H:%M"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=ICT)
        except ValueError:
            continue
    return datetime.now(ICT) - timedelta(hours=36)

def get_json(path_no_prefix: str, params=None):
    last_err = None
    for p in API_PREFIXES:
        url = f"{API_BASE}{p}{path_no_prefix}"
        try:
            r = requests.get(url, params=params or {}, timeout=60)
            if r.status_code == 404:
                last_err = f"404 {url}"
                continue
            r.raise_for_status()
            data = r.json()
            return data, url
        except Exception as e:
            last_err = f"{type(e).__name__}: {e} @ {url}"
            continue
    raise RuntimeError(last_err or "all prefixes failed")

# ---------- sync AIM/MIM (เต็มแทนที่แบบ upsert) ----------
def sync_mim(conn: sqlite3.Connection):
    data, used_url = get_json("/mim")
    if not isinstance(data, list): return 0, used_url
    cur = conn.cursor()
    cur.executemany(
        "INSERT INTO MIM(measurementLabel,measurementType,zoneName,deviceFriendlyName) "
        "VALUES(?,?,?,?) "
        "ON CONFLICT(measurementLabel) DO UPDATE SET "
        "measurementType=excluded.measurementType, "
        "zoneName=excluded.zoneName, "
        "deviceFriendlyName=excluded.deviceFriendlyName",
        [
            (
                d.get("measurementLabel"),
                d.get("measurementType"),
                d.get("zoneName"),
                d.get("deviceFriendlyName"),
            )
            for d in data
            if d.get("measurementLabel")
        ],
    )
    conn.commit()
    return len(data), used_url

def sync_aim(conn: sqlite3.Connection):
    data, used_url = get_json("/aim")
    if not isinstance(data, list): return 0, used_url
    cur = conn.cursor()
    cur.executemany(
        "INSERT INTO AIM(deviceFriendlyName,building,floor,room,deviceClass,macAddress,ipAddress) "
        "VALUES(?,?,?,?,?,?,?) "
        "ON CONFLICT(deviceFriendlyName) DO UPDATE SET "
        "building=excluded.building, floor=excluded.floor, room=excluded.room, "
        "deviceClass=excluded.deviceClass, macAddress=excluded.macAddress, ipAddress=excluded.ipAddress",
        [
            (
                d.get("deviceFriendlyName"),
                d.get("building"), d.get("floor"), d.get("room"),
                d.get("deviceClass"), d.get("macAddress"), d.get("ipAddress"),
            )
            for d in data
            if d.get("deviceFriendlyName")
        ],
    )
    conn.commit()
    return len(data), used_url

# ---------- Raw_State incremental ----------
def meta_get(conn, key, default_ts=None):
    row = conn.execute("SELECT last_ts FROM Meta WHERE source_key=?", (key,)).fetchone()
    return row[0] if row and row[0] else default_ts

def meta_set(conn, key, ts_str):
    conn.execute(
        "INSERT INTO Meta(source_key,last_ts) VALUES(?,?) "
        "ON CONFLICT(source_key) DO UPDATE SET last_ts=excluded.last_ts",
        (key, ts_str),
    )
    conn.commit()

def fetch_range(base_type: str, start_dt: datetime, end_dt: datetime):
    return get_json(f"/range/{base_type}", params={"from": fmt_min(start_dt), "to": fmt_min(end_dt)})

def upsert_raw(conn: sqlite3.Connection, rows):
    conn.executemany(
        "INSERT INTO Raw_State(timestamp,measurementLabel,state) VALUES(?,?,?) "
        "ON CONFLICT(timestamp,measurementLabel) DO UPDATE SET state=excluded.state",
        rows
    )
    conn.commit()

def main():
    conn = sqlite3.connect(DB_PATH)
    ensure_schema(conn)

    # 1) อัปเดต AIM/MIM ทุกครั้ง (ข้อมูลเล็ก)
    try:
        n_mim, url_mim = sync_mim(conn)
        print(f"[MIM] upsert {n_mim} rows via {url_mim}")
    except Exception as e:
        print(f"[MIM] skip: {e}")
    try:
        n_aim, url_aim = sync_aim(conn)
        print(f"[AIM] upsert {n_aim} rows via {url_aim}")
    except Exception as e:
        print(f"[AIM] skip: {e}")

    # 2) อัปเดต Raw_State แบบ incremental
    now = datetime.now(ICT)
    backfill_hours = int(os.getenv("BACKFILL_HOURS", "72"))  # รอบแรกดึงย้อนไปกี่ชั่วโมง

    total_all = 0
    for t in BASE_TYPES:
        key = f"base:{t}"
        last_ts = meta_get(conn, key, default_ts=(now - timedelta(hours=backfill_hours)).strftime("%Y-%m-%dT%H:%M"))
        start_dt = parse_dt_guess(last_ts) - timedelta(minutes=5)  # overlap 5 นาที
        end_dt   = now
        chunk = 60 if t == "occcount" else 180

        print(f"[sync] {t}: {fmt_min(start_dt)} → {fmt_min(end_dt)} (chunk={chunk}m)")
        t0 = start_dt
        type_total = 0
        used_url = None
        while t0 < end_dt:
            t1 = min(t0 + timedelta(minutes=chunk), end_dt)
            try:
                items, used_url = fetch_range(t, t0, t1)
                n = len(items or [])
                if n:
                    rows = []
                    for it in items:
                        try:
                            rows.append((str(it["timestamp"]).strip(),
                                         str(it["measurementLabel"]).strip(),
                                         float(it["state"])))
                        except Exception:
                            continue
                    if rows:
                        upsert_raw(conn, rows)
                        type_total += len(rows)
                print(f"  + {fmt_min(t0)}–{fmt_min(t1)} → {n} rec")
            except Exception as e:
                print(f"  ! {fmt_min(t0)}–{fmt_min(t1)} FAILED: {e}")
            t0 = t1
            time.sleep(0.2)

        meta_set(conn, key, now.strftime("%Y-%m-%dT%H:%M"))
        print(f"[done] {t}: {type_total} rows via {used_url}")
        total_all += type_total

    # summary
    try:
        size = os.path.getsize(DB_PATH)
    except Exception:
        size = -1
    print(f"[summary] total rows changed ≈ {total_all}, db={DB_PATH}, size={size} bytes")

if __name__ == "__main__":
    main()
