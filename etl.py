import os
import sys
import subprocess
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv("config/.env")

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise RuntimeError("DB_URL missing. Check config/.env")

ENGINE = create_engine(DB_URL)

PSQL_CONN = "postgresql://hotel_user:hotel_pass@localhost:5432/hotel_db"


def utc_run_id() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def log_run(run_id, step, dataset, status, row_count=None, checksum=None, message=""):
    with ENGINE.begin() as conn:
        conn.execute(text("""
            INSERT INTO audit.etl_run_log
            (run_id, step, dataset, started_at, ended_at, status, row_count, checksum, message)
            VALUES (:run_id, :step, :dataset, now(), now(), :status, :row_count, :checksum, :message)
        """), {
            "run_id": run_id,
            "step": step,
            "dataset": dataset,
            "status": status,
            "row_count": row_count,
            "checksum": checksum,
            "message": (message or "")[:1000]
        })


def run_psql_file(sql_path: str, run_id: str):
    cmd = [
        "psql", PSQL_CONN,
        "-v", f"run_id='{run_id}'",
        "-f", sql_path
    ]
    subprocess.run(cmd, check=True)


def load_bronze():
    run_id = utc_run_id()
    try:
        subprocess.run([sys.executable, "src/load_bronze.py"], check=True)
        log_run(run_id, "load_bronze", "all_bronze_tables", "success",
                message="Loaded bronze from CSVs")
        print("✅ Bronze load complete")
    except Exception as e:
        log_run(run_id, "load_bronze", "all_bronze_tables", "failed", message=str(e))
        raise


def build_silver():
    run_id = utc_run_id()
    try:
        run_psql_file("sql/02_audit_rejected_rows.sql", run_id)
        run_psql_file("sql/03_build_silver.sql", run_id)

        with ENGINE.begin() as conn:
            counts = conn.execute(text("""
                SELECT 'hotels' t, count(*) c FROM silver.hotels
                UNION ALL SELECT 'room_types', count(*) FROM silver.room_types
                UNION ALL SELECT 'bookings', count(*) FROM silver.bookings
                UNION ALL SELECT 'payments', count(*) FROM silver.payments
                UNION ALL SELECT 'room_inventory', count(*) FROM silver.room_inventory
                UNION ALL SELECT 'rejected_rows_this_run', count(*) FROM audit.rejected_rows WHERE run_id = :run_id
            """), {"run_id": run_id}).fetchall()

        msg = "; ".join([f"{r[0]}={r[1]}" for r in counts])
        log_run(run_id, "build_silver", "silver_layer", "success", message=msg)
        print("✅ build_silver completed")
        print(msg)
    except Exception as e:
        log_run(run_id, "build_silver", "silver_layer", "failed", message=str(e))
        raise


def build_gold():
    run_id = utc_run_id()
    try:
        run_psql_file("sql/04_build_gold.sql", run_id)
        # Save reconciliation output to logs (also runs the checks)
        os.makedirs("logs", exist_ok=True)
        with open("logs/day3_reconciliation.txt", "w", encoding="utf-8") as f:
            subprocess.run(
                ["psql", PSQL_CONN, "-f", "sql/05_reconciliation.sql"],
                check=True,
                stdout=f
            )

        log_run(run_id, "build_gold", "gold_layer", "success",
                message="Gold aggregates + dashboard table built; reconciliation saved")
        print("✅ build_gold completed")
        print("✅ reconciliation saved to logs/day3_reconciliation.txt")
    except Exception as e:
        log_run(run_id, "build_gold", "gold_layer", "failed", message=str(e))
        raise


def run_all():
    load_bronze()
    build_silver()
    build_gold()
    print("✅ all pipeline completed (bronze → silver → gold)")


def main():
    if len(sys.argv) < 2:
        print("Usage: python etl.py [load_bronze|build_silver|build_gold|all]")
        sys.exit(1)

    cmd = sys.argv[1].strip().lower()

    if cmd == "load_bronze":
        load_bronze()
    elif cmd == "build_silver":
        build_silver()
    elif cmd == "build_gold":
        build_gold()
    elif cmd == "all":
        run_all()
    else:
        raise SystemExit(f"Unknown command: {cmd}")


if __name__ == "__main__":
    main()
