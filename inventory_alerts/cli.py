import argparse
import os
from pathlib import Path

from .db import connect_db, run_sql_file
from .worker import run_worker


ROOT = Path(__file__).resolve().parent
SQL_DIR = ROOT / "sql"


def init_db() -> None:
    with connect_db(autocommit=True) as conn:
        run_sql_file(conn, str(SQL_DIR / "001_schema.sql"))
    print("✅ Database initialized.")


def seed_demo() -> None:
    with connect_db(autocommit=True) as conn:
        run_sql_file(conn, str(SQL_DIR / "002_seed_demo.sql"))
    print("✅ Seeded demo data and triggered a low-stock alert.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Postgres inventory low-stock alerts (email via SMTP).")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init-db", help="Create schema/tables/triggers/views.")
    sub.add_parser("seed-demo", help="Insert demo rows and trigger a low-stock alert.")

    p_run = sub.add_parser("run-worker", help="Run LISTEN/NOTIFY worker that emails employees.")
    p_run.add_argument("--poll-fallback-seconds", type=int, default=60)

    args = parser.parse_args()

    if args.cmd == "init-db":
        init_db()
    elif args.cmd == "seed-demo":
        seed_demo()
    elif args.cmd == "run-worker":
        run_worker(poll_fallback_seconds=args.poll_fallback_seconds)
    else:
        raise SystemExit("Unknown command")


if __name__ == "__main__":
    main()
