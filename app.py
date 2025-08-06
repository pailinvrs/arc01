import os
import sqlite3
import requests
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# ลิงก์ Google Drive (Direct download)
DB_URL = "https://drive.google.com/uc?export=download&id=1d1iwFTPsVWffVw5KflnRc58_7Tj-4299"
DB_PATH = "arc01.sqlite"  # ตั้งชื่อตรงกับไฟล์จริง

def download_db():
    if not os.path.exists(DB_PATH):
        print("Downloading database from Google Drive...")
        r = requests.get(DB_URL)
        with open(DB_PATH, 'wb') as f:
            f.write(r.content)
        print("Database downloaded successfully.")

# เรียกโหลด DB ตอนเริ่ม
download_db()

# ตั้งค่า CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def run_sql(sql: str):
    conn = sqlite3.connect(DB_PATH)
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

