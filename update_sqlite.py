#!/usr/bin/env python3
# scripts/update_sqlite.py
# อัปเดตเฉพาะ Raw_State จาก Base API
import os, time, requests, sqlite3
from datetime import datetime, timedelta, timezone

API_BASE = os.getenv("API_BASE", "https://archcu-digitaltwin.mamgistics.com")
DB_PATH  = os.getenv("DB_PATH", "arc01.db")
TZ = timezone(timedelta(hours=7))  # ICT

# ชนิดข้อมูลจาก Base API (ปรับได้ตามจริงของระบบคุณ)
BASE_TYPES = ["intemp","inhumid","inco2","inpm25","intvoc","current","occcount","battery"]

# mapping สำหรับค้นหา last_ts จาก Raw_State เดิม
LABEL_PATTERNS = {
    "intemp":   ["inTempMeas%"],
    "inhumid":  ["inHumidMeas%"],
    "inco2":    ["inCO2Meas%"],
    "inpm25":   ["inPM25Meas%"],
    "intvoc":   ["inTVOCMeas%"],
    "current":  ["current%"],
    "occcount": ["occCount%"],
    "battery":  ["battery%"],
}

def ensure_raw_state(conn: sqlite3.Connection):
    cur = conn.cursor()
    # สร้างเฉพาะ Raw_State ถ้ายังไม่มี (ไม่แตะ MIM/AIM)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Raw_State(
            timestamp TEXT NOT NULL,
            measurementLabel TEXT NOT NULL,
            state REAL NOT NULL,
            PRIMARY KEY (timestamp, measurementLabel)
        );
    """)
    conn.commit()

def parse_dt_ict(s: str) -> datetime:
    s = s.strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M%z"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    # ถ้า parse ไม่ได้ ให้ fallback เป็นตอนนี้ใน ICT
    return datetime.now(TZ)

def fmt_minute_ict(dt: datetime) -> str:
    return dt.astimezone(TZ).strftime("%Y-%m-%dT%H:%M")

def get_last_ts_for_type(conn: sqlite3.Connection, base_type: str):
    patterns = LABEL_PATTERNS.get(base_type, [])
    if not patterns:
        return None
    where = " OR ".join(["measurementLabel LIKE ?"] * len(patterns))
    sql = f"SELECT MAX(timestamp) FROM Raw_State WHERE {where}"
    cur = conn.cursor()
    cur.execute(sql, patterns)
    row = cur.fetchone()
    return row[0] if row and row[0] else None

def fetch_range(base_type: str, start_dt: datetime, end_dt: datetime):
    url = f"{API_BASE}/arc01/api/range/{base_type}"
    params = {"from": fmt_minute_ict(start_dt), "to": fmt_minute_ict(end_dt)}
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict):
        data = [data]
    return data

def upsert_raw(conn: sqlite3.Connection, rows):
    cur = conn.cursor()
    cur.executemany(
        "INSERT INTO Raw_State(timestamp,measurementLabel,state) VALUES(?,?,?) "
        "ON CONFLICT(timestamp,measurementLabel) DO UPDATE SET state=excluded.state",
        rows
    )
    conn.commit()

def main():
    conn = sqlite3.connect(DB_PATH)
    ensure_raw_state(conn)

    now = datetime.now(TZ)
    # ดึงย้อนหลัง 36 ชม. เผื่อกรณีหลุดรอบ + overlap 5 นาที
    fallback_start = now - timedelta(hours=36)

    for base_type in BASE_TYPES:
        last_ts = get_last_ts_for_type(conn, base_type)
        start_dt = parse_dt_ict(last_ts) - timedelta(minutes=5) if last_ts else fallback_start
        end_dt   = now

        # แบ่งช่วงเพื่อความปลอดภัย (occcount หนาแน่น ใช้ชิ้นเล็กลง)
        chunk_minutes = 60 if base_type == "occcount" else 180

        print(f"[sync] {base_type}: {fmt_minute_ict(start_dt)} -> {fmt_minute_ict(end_dt)} (chunk {chunk_minutes}m)")
        t0 = start_dt
        total = 0
        while t0 < end_dt:
            t1 = min(t0 + timedelta(minutes=chunk_minutes), end_dt)
            try:
                items = fetch_range(base_type, t0, t1)
            except Exception as e:
                print(f"[warn] fetch {base_type} {fmt_minute_ict(t0)}-{fmt_minute_ict(t1)} failed: {e}")
                t0 = t1
                time.sleep(0.3)
                continue

            rows = []
            for it in (items or []):
                try:
                    ts  = str(it["timestamp"]).strip()
                    lab = str(it["measurementLabel"]).strip()
                    val = float(it["state"])
                    rows.append((ts, lab, val))
                except Exception:
                    continue

            if rows:
                upsert_raw(conn, rows)
                total += len(rows)

            t0 = t1
            time.sleep(0.2)  # ถนอม API

        print(f"[done] {base_type}: upserted {total} rows")

    conn.close()

if __name__ == "__main__":
    main()
