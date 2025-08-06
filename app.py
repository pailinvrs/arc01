import os
import sqlite3
import requests
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

DB_URL = "https://github.com/pailinvrs/arc01/releases/download/v1.0/arc01.sqlite"
DB_PATH = "arc01.sqlite"
EXPECTED_SIZE_MB = 300  # ขนาดขั้นต่ำเพื่อเช็กไฟล์ (ตั้งน้อยกว่าของจริงเล็กน้อย)

def download_db():
    if not os.path.exists(DB_PATH) or os.path.getsize(DB_PATH) < EXPECTED_SIZE_MB * 1024 * 1024:
        print("Downloading database from GitHub Releases...")
        r = requests.get(DB_URL, stream=True)
        r.raise_for_status()
        with open(DB_PATH, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
        print(f"Database downloaded successfully. Size: {size_mb:.2f} MB")
    else:
        print("Database already exists and size is OK.")

# โหลด DB ตอนเริ่ม
download_db()

# ตั้งค่า CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def run_sql(sql: str):
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()
    return rows

@app.get("/query")
def query_database(sql: str = Query(..., description="SQL query string")):
    try:
        result = run_sql(sql)
        return {"status": "ok", "data": result}
    except Exception as e:
        return {"status": "error", "message": str(e)}


