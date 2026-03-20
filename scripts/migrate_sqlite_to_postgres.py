#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""One-shot migration from the local SQLite payment database to PostgreSQL.

Usage:
    python scripts/migrate_sqlite_to_postgres.py \
        --sqlite-path invoice_payment.db \
        --postgres-dsn postgresql://user:pass@host:5432/dbname
"""

import argparse
import os
import sqlite3
from typing import Iterable, Sequence

import psycopg
from psycopg.rows import dict_row

from brazil_tool.db.postgres_payment_manager import PostgresPaymentManager


TABLES_IN_ORDER = [
    "accounts",
    "payment_invoices",
    "account_transactions",
    "payment_installments",
    "customer_advances",
]

TABLE_COLUMNS = {
    "accounts": ["id", "name", "bank_info", "currency", "initial_balance", "current_balance", "note", "is_active"],
    "payment_invoices": [
        "id",
        "invoice_number",
        "issuer_name",
        "issue_date",
        "total_amount",
        "terms_count",
        "status",
        "file_name",
        "created_at",
        "natureza_operacao",
        "destinatario_name",
        "destinatario_cnpj",
        "issuer_cnpj",
        "description",
    ],
    "account_transactions": [
        "id",
        "account_id",
        "date",
        "type",
        "amount",
        "description",
        "related_invoice_id",
        "related_installment_id",
        "created_at",
    ],
    "payment_installments": [
        "id",
        "invoice_id",
        "term_number",
        "due_date",
        "amount",
        "paid_amount",
        "penalty",
        "status",
        "paid_date",
        "note",
        "account_id",
        "transaction_id",
        "created_at",
    ],
    "customer_advances": [
        "id",
        "customer_name",
        "customer_cnpj",
        "amount",
        "remaining_amount",
        "date",
        "description",
        "account_id",
        "transaction_id",
        "created_at",
        "status",
    ],
}

TABLE_CONFLICT_UPDATE = {
    table: [col for col in cols if col != "id"]
    for table, cols in TABLE_COLUMNS.items()
}


def parse_args():
    parser = argparse.ArgumentParser(description="Migrate Brazil Tool payment data from SQLite to PostgreSQL")
    parser.add_argument("--sqlite-path", default=os.getenv("BRAZIL_TOOL_DB_PATH", "invoice_payment.db"))
    parser.add_argument("--postgres-dsn", default=os.getenv("BRAZIL_TOOL_DATABASE_URL", ""))
    return parser.parse_args()


def iter_sqlite_rows(conn: sqlite3.Connection, table: str, columns: Sequence[str]) -> Iterable[sqlite3.Row]:
    col_sql = ", ".join(columns)
    query = f"SELECT {col_sql} FROM {table} ORDER BY id"
    cur = conn.cursor()
    cur.execute(query)
    while True:
        batch = cur.fetchmany(500)
        if not batch:
            break
        for row in batch:
            yield row


def insert_rows(pg_conn: psycopg.Connection, table: str, columns: Sequence[str], rows: Iterable[sqlite3.Row]):
    cols_sql = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    update_cols = TABLE_CONFLICT_UPDATE[table]
    updates_sql = ", ".join([f"{col}=EXCLUDED.{col}" for col in update_cols])
    sql = f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders}) ON CONFLICT (id) DO UPDATE SET {updates_sql}"
    count = 0
    with pg_conn.cursor() as cur:
        for row in rows:
            values = [row[col] for col in columns]
            cur.execute(sql, values)
            count += 1
    return count


def reset_sequence(pg_conn: psycopg.Connection, table: str):
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT setval(pg_get_serial_sequence(%s, 'id'), COALESCE((SELECT MAX(id) FROM " + table + "), 1), true)",
            (table,),
        )


def main():
    args = parse_args()
    if not args.postgres_dsn:
        raise SystemExit("Missing PostgreSQL DSN. Set --postgres-dsn or BRAZIL_TOOL_DATABASE_URL.")

    # Ensure target schema exists.
    PostgresPaymentManager(args.postgres_dsn)

    sqlite_conn = sqlite3.connect(args.sqlite_path)
    sqlite_conn.row_factory = sqlite3.Row
    pg_conn = psycopg.connect(args.postgres_dsn, row_factory=dict_row)

    try:
        for table in TABLES_IN_ORDER:
            columns = TABLE_COLUMNS[table]
            copied = insert_rows(pg_conn, table, columns, iter_sqlite_rows(sqlite_conn, table, columns))
            print(f"[{table}] copied {copied} rows")
        for table in TABLES_IN_ORDER:
            reset_sequence(pg_conn, table)
        pg_conn.commit()
        print("Migration completed successfully.")
    finally:
        sqlite_conn.close()
        pg_conn.close()


if __name__ == "__main__":
    main()
