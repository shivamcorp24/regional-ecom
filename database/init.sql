-- Runs once, when the Postgres data volume is first created.
-- Airflow keeps its metadata in its own database, separate from the warehouse.
CREATE DATABASE airflow;
