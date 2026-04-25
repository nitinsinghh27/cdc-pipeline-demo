#!/usr/bin/env python3
"""
monitor.py — Live pipeline stats: MySQL vs MinIO vs PostgreSQL row/file counts.

Usage:
    python monitor.py          # refresh every 3s
    python monitor.py --interval 5
"""

import argparse
import os
import time

import boto3
import psycopg2
import pymysql
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

TABLES = ["stores", "products", "inventory", "orders"]

def mysql_counts():
    try:
        conn = pymysql.connect(
            host=os.environ.get("MYSQL_HOST", "localhost"),
            port=int(os.environ.get("MYSQL_PORT", 3306)),
            db=os.environ.get("MYSQL_DB", "demo_db"),
            user=os.environ.get("MYSQL_USER", "app_user"),
            password=os.environ.get("MYSQL_PASSWORD", "app_pass"),
            connect_timeout=3,
        )
        cur = conn.cursor()
        counts = {}
        for t in TABLES:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            counts[t] = cur.fetchone()[0]
        conn.close()
        return counts, None
    except Exception as e:
        return {}, str(e)


def minio_counts():
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
            aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        paginator = s3.get_paginator("list_objects_v2")
        by_table = {}
        total = 0
        for page in paginator.paginate(Bucket="cdc-events"):
            for obj in page.get("Contents", []):
                table = obj["Key"].split("/")[0]
                by_table[table] = by_table.get(table, 0) + 1
                total += 1
        return by_table, total, None
    except Exception as e:
        return {}, 0, str(e)


def postgres_counts():
    try:
        conn = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=int(os.environ.get("POSTGRES_PORT", 5433)),
            dbname=os.environ.get("POSTGRES_DB", "staging_db"),
            user=os.environ.get("POSTGRES_USER", "postgres"),
            password=os.environ.get("POSTGRES_PASSWORD", "postgres_pass"),
            connect_timeout=3,
        )
        cur = conn.cursor()
        counts = {}
        processed = 0
        for t in TABLES:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                counts[t] = cur.fetchone()[0]
            except Exception:
                conn.rollback()
                counts[t] = "?"
        try:
            cur.execute("SELECT COUNT(*) FROM _cdc_loader_state")
            processed = cur.fetchone()[0]
        except Exception:
            conn.rollback()
        conn.close()
        return counts, processed, None
    except Exception as e:
        return {}, 0, str(e)


def render(mysql, minio_by_table, minio_total, minio_processed, postgres):
    os.system("clear")
    now = time.strftime("%H:%M:%S")
    print(f"╔══════════════════════════════════════════════════════════╗")
    print(f"║          CDC Pipeline Live Monitor  [{now}]         ║")
    print(f"╚══════════════════════════════════════════════════════════╝")
    print()
    print(f"  {'TABLE':<12}  {'MySQL':>8}  {'MinIO files':>12}  {'Postgres':>10}  {'Lag':>6}")
    print(f"  {'─'*12}  {'─'*8}  {'─'*12}  {'─'*10}  {'─'*6}")

    for t in TABLES:
        my  = mysql.get(t, "✗")
        mn  = minio_by_table.get(t, 0)
        pg  = postgres.get(t, "✗")
        lag = (my - pg) if isinstance(my, int) and isinstance(pg, int) else "?"
        lag_str = f"{'🔴 ' if isinstance(lag, int) and lag > 5 else ''}{lag}"
        print(f"  {t:<12}  {str(my):>8}  {str(mn):>12}  {str(pg):>10}  {lag_str:>6}")

    print()
    print(f"  MinIO total files : {minio_total}")
    print(f"  Loader processed  : {minio_processed}")
    pending = minio_total - minio_processed
    status = "✅ caught up" if pending == 0 else f"⚠️  {pending} pending"
    print(f"  Loader status     : {status}")
    print()
    print(f"  Press Ctrl+C to stop.")


def main(interval: float):
    print("Starting monitor... (Ctrl+C to stop)")
    while True:
        mysql, mysql_err         = mysql_counts()
        minio_by, minio_tot, _   = minio_counts()
        pg, processed, pg_err    = postgres_counts()

        render(mysql, minio_by, minio_tot, processed, pg)

        if mysql_err:
            print(f"  ⚠️  MySQL error: {mysql_err}")
        if pg_err:
            print(f"  ⚠️  Postgres error: {pg_err}")

        time.sleep(interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=float, default=3.0)
    args = parser.parse_args()
    try:
        main(args.interval)
    except KeyboardInterrupt:
        print("\nMonitor stopped.")
