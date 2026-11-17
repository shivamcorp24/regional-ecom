"""Creates the Airflow metadata database in the shared Postgres server if it does not exist.

Postgres init scripts only run on a fresh volume, so this runs from airflow-init instead and
works against an existing volume too.
"""

import os

import psycopg2

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    port=os.getenv("POSTGRES_PORT", "5432"),
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
    dbname=os.environ["POSTGRES_DB"],
)
conn.autocommit = True
with conn.cursor() as cur:
    cur.execute("SELECT 1 FROM pg_database WHERE datname = 'airflow'")
    if cur.fetchone():
        print("Database 'airflow' already exists.")
    else:
        cur.execute("CREATE DATABASE airflow")
        print("Created database 'airflow'.")
conn.close()
