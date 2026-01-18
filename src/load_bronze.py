import os
import hashlib
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv("config/.env")

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise RuntimeError("DB_URL missing. Check config/.env")

engine = create_engine(DB_URL)

DATASETS = [
    ("hotels_raw", "bronze_inputs/hotels_raw.csv"),
    ("room_types_raw", "bronze_inputs/room_types_raw.csv"),
    ("bookings_raw", "bronze_inputs/bookings_raw.csv"),
    ("payments_raw", "bronze_inputs/payments_raw.csv"),
    ("room_inventory_raw", "bronze_inputs/room_inventory_raw.csv"),
]

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def log_run(run_id, step, dataset, status, row_count=None, checksum=None, message=""):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO audit.etl_run_log
            (run_id, step, dataset, started_at, ended_at, status, row_count, checksum, message)
            VALUES (:run_id, :step, :dataset, :started_at, :ended_at, :status, :row_count, :checksum, :message)
        """), {
            "run_id": run_id,
            "step": step,
            "dataset": dataset,
            "started_at": datetime.utcnow(),
            "ended_at": datetime.utcnow(),
            "status": status,
            "row_count": row_count,
            "checksum": checksum,
            "message": (message or "")[:1000]
        })

def load_one(table: str, csv_path: str, run_id: str):
    checksum = sha256_file(csv_path)
    df = pd.read_csv(csv_path)

    # Idempotent load: truncate then append
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE bronze.{table};"))

    df.to_sql(
        table,
        engine,
        schema="bronze",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000
    )

    row_count = len(df)
    log_run(
        run_id, "load_bronze", table, "success",
        row_count=row_count, checksum=checksum,
        message=f"Loaded {row_count} rows into bronze.{table} from {csv_path}"
    )

def main():
    run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    for table, path in DATASETS:
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing file: {path}")
        try:
            load_one(table, path, run_id)
        except Exception as e:
            log_run(run_id, "load_bronze", table, "failed", checksum=sha256_file(path), message=str(e))
            raise
    print("✅ Bronze load complete")

if __name__ == "__main__":
    main()
