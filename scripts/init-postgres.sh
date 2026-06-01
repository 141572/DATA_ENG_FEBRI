#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE airflow;
    CREATE DATABASE olist_dwh;
EOSQL

# Execute the DWH schema DDL in the newly created olist_dwh database
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "olist_dwh" -f /docker-entrypoint-initdb.d/init_dwh.sql
