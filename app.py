import sqlite3
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# อนุญาตให้เรียก API จากทุกที่ (รวม ChatGPT)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = "arc01.db"  # path ไป SQLite ของคุณ

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
