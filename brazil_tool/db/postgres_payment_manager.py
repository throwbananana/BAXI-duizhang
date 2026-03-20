# -*- coding: utf-8 -*-
import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import psycopg
from psycopg.rows import dict_row

from brazil_tool.core.utils import br_to_float


class PostgresPaymentManager:
    """PostgreSQL-backed payment database manager.

    This class mirrors the public methods used by `brazil_tool.server` so the
    existing API layer can switch backends through environment variables with
    minimal changes.
    """

    backend = "postgres"
    _INSTALLMENT_ALLOWED_FIELDS = {
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
    }
    _INSTALLMENT_NUMERIC_FIELDS = {"amount", "paid_amount", "penalty"}
    _INSTALLMENT_STATUS_VALUES = {"Pending", "Partial", "Paid"}

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.target = dsn
        self.init_db()

    def _connect(self):
        return psycopg.connect(self.dsn, row_factory=dict_row)

    @staticmethod
    def _now_str() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def ping(self) -> bool:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            return True
        except Exception:
            return False

    def _parse_date(self, date_str):
        if not date_str or not isinstance(date_str, str):
            return None
        date_str = date_str.strip()
        if not date_str:
            return None
        d_part = date_str.split()[0]
        for fmt in ["%d/%m/%Y", "%Y/%m/%d", "%Y-%m-%d", "%d-%m-%Y"]:
            try:
                return datetime.strptime(d_part, fmt)
            except ValueError:
                continue
        return None

    def _to_float(self, value):
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        parsed = br_to_float(str(value))
        if parsed is not None:
            return parsed
        try:
            return float(str(value))
        except Exception:
            return None

    def _normalize_positive_amount(self, value):
        amount = self._to_float(value)
        if amount is None or amount <= 0:
            return None
        return round(amount, 2)

    def init_db(self):
        ddl = [
            """
            CREATE TABLE IF NOT EXISTS payment_invoices (
                id BIGSERIAL PRIMARY KEY,
                invoice_number TEXT UNIQUE,
                issuer_name TEXT,
                issue_date TEXT,
                total_amount DOUBLE PRECISION,
                terms_count INTEGER DEFAULT 1,
                status TEXT DEFAULT 'Unpaid',
                file_name TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                natureza_operacao TEXT,
                destinatario_name TEXT,
                destinatario_cnpj TEXT,
                issuer_cnpj TEXT,
                description TEXT DEFAULT ''
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS payment_installments (
                id BIGSERIAL PRIMARY KEY,
                invoice_id BIGINT REFERENCES payment_invoices(id) ON DELETE CASCADE,
                term_number INTEGER,
                due_date TEXT,
                amount DOUBLE PRECISION,
                paid_amount DOUBLE PRECISION DEFAULT 0,
                penalty DOUBLE PRECISION DEFAULT 0,
                status TEXT DEFAULT 'Pending',
                paid_date TEXT,
                note TEXT,
                account_id BIGINT,
                transaction_id BIGINT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS accounts (
                id BIGSERIAL PRIMARY KEY,
                name TEXT UNIQUE,
                bank_info TEXT,
                currency TEXT DEFAULT 'BRL',
                initial_balance DOUBLE PRECISION DEFAULT 0,
                current_balance DOUBLE PRECISION DEFAULT 0,
                note TEXT,
                is_active INTEGER DEFAULT 1
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS account_transactions (
                id BIGSERIAL PRIMARY KEY,
                account_id BIGINT REFERENCES accounts(id),
                date TEXT,
                type TEXT,
                amount DOUBLE PRECISION,
                description TEXT,
                related_invoice_id BIGINT,
                related_installment_id BIGINT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS customer_advances (
                id BIGSERIAL PRIMARY KEY,
                customer_name TEXT,
                customer_cnpj TEXT,
                amount DOUBLE PRECISION,
                remaining_amount DOUBLE PRECISION,
                date TEXT,
                description TEXT,
                account_id BIGINT REFERENCES accounts(id),
                transaction_id BIGINT,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                status TEXT DEFAULT 'Open'
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_payment_installments_invoice_id ON payment_installments(invoice_id)",
            "CREATE INDEX IF NOT EXISTS idx_payment_installments_status ON payment_installments(status)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_payment_installments_invoice_term ON payment_installments(invoice_id, term_number)",
            "CREATE INDEX IF NOT EXISTS idx_payment_invoices_created_at ON payment_invoices(created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_account_transactions_account_date ON account_transactions(account_id, date, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_account_transactions_invoice ON account_transactions(related_invoice_id)",
        ]
        with self._connect() as conn, conn.cursor() as cur:
            for stmt in ddl:
                cur.execute(stmt)
            conn.commit()

    def find_invoice_id_by_number(self, number: str):
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT id FROM payment_invoices WHERE invoice_number = %s", (number,))
            row = cur.fetchone()
            return row["id"] if row else None

    def get_all_existing_invoice_numbers(self) -> set:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT invoice_number FROM payment_invoices")
            return {row["invoice_number"] for row in cur.fetchall() if row and row.get("invoice_number")}

    def get_account_id_for_invoice(self, invoice_id: int):
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT account_id FROM account_transactions WHERE related_invoice_id=%s ORDER BY id DESC LIMIT 1",
                (invoice_id,),
            )
            row = cur.fetchone()
            return row["account_id"] if row else None

    def get_all_installments_extended(self):
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    i.issuer_name, i.destinatario_name, i.destinatario_cnpj, i.issuer_cnpj,
                    p.due_date, p.amount, p.paid_amount, p.status, i.invoice_number
                FROM payment_installments p
                JOIN payment_invoices i ON p.invoice_id = i.id
                """
            )
            return cur.fetchall()

    def search_pending_installments(self, patterns: List[str], term_number: int = None):
        if not patterns:
            return []
        clauses = []
        params: List[Any] = []
        for pat in patterns:
            clauses.append("i.invoice_number LIKE %s")
            params.append(pat)
        query = (
            """
            SELECT i.id as invoice_id, p.id as installment_id, p.amount,
                   i.destinatario_name, i.destinatario_cnpj, p.due_date, i.invoice_number,
                   p.paid_amount, p.penalty, p.term_number
            FROM payment_installments p
            JOIN payment_invoices i ON p.invoice_id = i.id
            WHERE p.status != 'Paid' AND (
            """
            + " OR ".join(clauses)
            + ")"
        )
        if term_number is not None:
            query += " AND p.term_number = %s"
            params.append(term_number)
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def register_payment(self, installment_id: int, amount: float, date: str, account_id: int, description: str) -> bool:
        amount = self._normalize_positive_amount(amount)
        if amount is None:
            return False
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, invoice_id, paid_amount, amount, penalty
                    FROM payment_installments
                    WHERE id=%s
                    FOR UPDATE
                    """,
                    (installment_id,),
                )
                inst = cur.fetchone()
                if not inst:
                    return False
                inv_id = inst["invoice_id"]
                current_paid = inst.get("paid_amount") or 0.0
                total_amount = inst.get("amount") or 0.0
                penalty = inst.get("penalty") or 0.0
                due_total = total_amount + penalty
                due_remaining = max(0.0, due_total - current_paid)
                if amount > due_remaining + 0.01:
                    return False

                cur.execute(
                    """
                    INSERT INTO account_transactions
                    (account_id, date, type, amount, description, related_invoice_id, related_installment_id, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    RETURNING id
                    """,
                    (account_id, date, 'Income', amount, description, inv_id, installment_id),
                )
                trans_id = cur.fetchone()["id"]
                cur.execute("UPDATE accounts SET current_balance = current_balance + %s WHERE id=%s", (amount, account_id))

                new_paid = current_paid + amount
                new_status = 'Paid' if new_paid >= due_total - 0.01 else 'Partial'
                cur.execute(
                    """
                    UPDATE payment_installments
                    SET paid_amount=%s, status=%s, paid_date=%s, transaction_id=%s, account_id=%s
                    WHERE id=%s
                    """,
                    (new_paid, new_status, date, trans_id, account_id, installment_id),
                )
                self._refresh_invoice_status(cur, inv_id)
                conn.commit()
                return True
        except Exception as e:
            logging.error("Error registering payment (installment_id=%s, account_id=%s): %s", installment_id, account_id, e)
            return False

    def add_account(self, name, bank_info="", currency="BRL", initial_balance=0.0, note=""):
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO accounts (name, bank_info, currency, initial_balance, current_balance, note)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (name, bank_info, currency, initial_balance, initial_balance, note),
                )
                row_id = cur.fetchone()["id"]
                conn.commit()
                return row_id
        except Exception:
            return None

    def update_account(self, account_id, name, bank_info, currency, note, is_active=1) -> bool:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE accounts SET name=%s, bank_info=%s, currency=%s, note=%s, is_active=%s
                    WHERE id=%s
                    """,
                    (name, bank_info, currency, note, is_active, account_id),
                )
                ok = cur.rowcount > 0
                conn.commit()
                return ok
        except Exception as e:
            logging.error("Error updating account (%s): %s", account_id, e)
            return False

    def delete_account(self, account_id) -> bool:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute("SELECT id FROM accounts WHERE id=%s", (account_id,))
                if not cur.fetchone():
                    return False
                cur.execute("SELECT count(*) AS cnt FROM account_transactions WHERE account_id=%s", (account_id,))
                used = cur.fetchone()["cnt"]
                if used > 0:
                    cur.execute("UPDATE accounts SET is_active=0 WHERE id=%s", (account_id,))
                else:
                    cur.execute("DELETE FROM accounts WHERE id=%s", (account_id,))
                conn.commit()
                return True
        except Exception as e:
            logging.error("Error deleting account (%s): %s", account_id, e)
            return False

    def get_accounts(self, active_only=True):
        query = "SELECT * FROM accounts"
        if active_only:
            query += " WHERE is_active=1"
        query += " ORDER BY name"
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()

    def get_account_balance(self, account_id):
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT current_balance FROM accounts WHERE id=%s", (account_id,))
            row = cur.fetchone()
            return row["current_balance"] if row else 0.0

    def add_transaction(self, account_id, date, trans_type, amount, description, related_invoice_id=None, related_installment_id=None):
        amount = self._normalize_positive_amount(amount)
        if amount is None:
            return None
        trans_type = str(trans_type or "").strip().title()
        if trans_type not in {"Income", "Expense"}:
            return None
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO account_transactions
                    (account_id, date, type, amount, description, related_invoice_id, related_installment_id, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    RETURNING id
                    """,
                    (account_id, date, trans_type, amount, description, related_invoice_id, related_installment_id),
                )
                trans_id = cur.fetchone()["id"]
                if trans_type == 'Income':
                    cur.execute("UPDATE accounts SET current_balance = current_balance + %s WHERE id=%s", (amount, account_id))
                else:
                    cur.execute("UPDATE accounts SET current_balance = current_balance - %s WHERE id=%s", (amount, account_id))
                conn.commit()
                return trans_id
        except Exception as e:
            logging.error("Error adding transaction (account_id=%s): %s", account_id, e)
            return None

    def delete_transaction(self, trans_id) -> bool:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute("SELECT * FROM account_transactions WHERE id=%s", (trans_id,))
                trans = cur.fetchone()
                if not trans:
                    return False
                if trans['type'] == 'Income':
                    cur.execute("UPDATE accounts SET current_balance = current_balance - %s WHERE id=%s", (trans['amount'], trans['account_id']))
                else:
                    cur.execute("UPDATE accounts SET current_balance = current_balance + %s WHERE id=%s", (trans['amount'], trans['account_id']))

                if trans.get('related_installment_id'):
                    inst_id = trans['related_installment_id']
                    cur.execute(
                        "SELECT paid_amount, invoice_id FROM payment_installments WHERE id=%s FOR UPDATE",
                        (inst_id,),
                    )
                    inst_row = cur.fetchone()
                    if inst_row:
                        curr_paid = inst_row.get('paid_amount') or 0.0
                        new_paid = max(0.0, curr_paid - trans['amount'])
                        new_status = 'Partial' if new_paid > 0.01 else 'Pending'
                        cur.execute(
                            """
                            UPDATE payment_installments
                            SET transaction_id = CASE WHEN transaction_id=%s THEN NULL ELSE transaction_id END,
                                paid_amount=%s,
                                status=%s,
                                paid_date=CASE WHEN %s <= 0.01 THEN NULL ELSE paid_date END
                            WHERE id=%s
                            """,
                            (trans_id, new_paid, new_status, new_paid, inst_id),
                        )
                        self._refresh_invoice_status(cur, inst_row['invoice_id'])

                cur.execute("DELETE FROM account_transactions WHERE id=%s", (trans_id,))
                conn.commit()
                return True
        except Exception as e:
            logging.error("Error deleting transaction (%s): %s", trans_id, e)
            return False

    def get_transactions(self, account_id, limit=100):
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT t.*, i.invoice_number, i.destinatario_name, i.issuer_name
                FROM account_transactions t
                LEFT JOIN payment_invoices i ON t.related_invoice_id = i.id
                WHERE t.account_id=%s
                ORDER BY t.date DESC, t.created_at DESC
                LIMIT %s
                """,
                (account_id, limit),
            )
            return cur.fetchall()

    def get_need_pdf_invoices(self):
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, invoice_number
                FROM payment_invoices
                WHERE natureza_operacao LIKE '%%NEED PDF%%'
                   OR description LIKE '%%NEED PDF%%'
                """
            )
            return cur.fetchall()

    def upsert_invoice(self, invoice_data: dict):
        if not isinstance(invoice_data, dict):
            return None
        required_fields = ("invoice_number", "issuer_name", "issue_date", "total_amount", "file_name")
        if any(invoice_data.get(k) is None for k in required_fields):
            return None
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO payment_invoices
                    (invoice_number, issuer_name, issue_date, total_amount, file_name, created_at, status,
                     natureza_operacao, destinatario_name, destinatario_cnpj, issuer_cnpj, description)
                    VALUES (%s, %s, %s, %s, %s, NOW(), 'Unpaid', %s, %s, %s, %s, %s)
                    ON CONFLICT (invoice_number) DO UPDATE SET
                        issuer_name = EXCLUDED.issuer_name,
                        issue_date = EXCLUDED.issue_date,
                        total_amount = EXCLUDED.total_amount,
                        file_name = EXCLUDED.file_name,
                        natureza_operacao = EXCLUDED.natureza_operacao,
                        destinatario_name = EXCLUDED.destinatario_name,
                        destinatario_cnpj = EXCLUDED.destinatario_cnpj,
                        issuer_cnpj = EXCLUDED.issuer_cnpj,
                        description = COALESCE(EXCLUDED.description, payment_invoices.description)
                    RETURNING id
                    """,
                    (
                        invoice_data['invoice_number'],
                        invoice_data['issuer_name'],
                        invoice_data['issue_date'],
                        invoice_data['total_amount'],
                        invoice_data['file_name'],
                        invoice_data.get('natureza_operacao', ''),
                        invoice_data.get('destinatario_name', ''),
                        invoice_data.get('destinatario_cnpj', ''),
                        invoice_data.get('issuer_cnpj', ''),
                        invoice_data.get('description', ''),
                    ),
                )
                inv_id = cur.fetchone()["id"]
                conn.commit()
                return inv_id
        except Exception as e:
            logging.error("Error upserting invoice (%s): %s", invoice_data.get("invoice_number"), e)
            return None

    def update_invoice_number(self, invoice_id: int, new_number: str) -> bool:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute("UPDATE payment_invoices SET invoice_number=%s WHERE id=%s", (new_number, invoice_id))
                ok = cur.rowcount > 0
                conn.commit()
                return ok
        except Exception:
            return False

    def delete_invoice_by_number(self, invoice_number: str):
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute("SELECT id FROM payment_invoices WHERE invoice_number=%s", (invoice_number,))
                row = cur.fetchone()
                if not row:
                    return False
                inv_id = row['id']

                cur.execute("SELECT * FROM account_transactions WHERE related_invoice_id=%s ORDER BY id DESC", (inv_id,))
                trans_rows = cur.fetchall()
                for trans in trans_rows:
                    if trans['type'] == 'Income':
                        cur.execute("UPDATE accounts SET current_balance = current_balance - %s WHERE id=%s", (trans['amount'], trans['account_id']))
                    else:
                        cur.execute("UPDATE accounts SET current_balance = current_balance + %s WHERE id=%s", (trans['amount'], trans['account_id']))

                    if trans.get('related_installment_id'):
                        inst_id = trans['related_installment_id']
                        cur.execute(
                            "SELECT paid_amount, amount, penalty, invoice_id FROM payment_installments WHERE id=%s FOR UPDATE",
                            (inst_id,),
                        )
                        inst_row = cur.fetchone()
                        if inst_row:
                            curr_paid = inst_row.get('paid_amount') or 0.0
                            total_due = (inst_row.get('amount') or 0.0) + (inst_row.get('penalty') or 0.0)
                            new_paid = max(0.0, curr_paid - (trans.get('amount') or 0.0))
                            if new_paid >= total_due - 0.01:
                                new_status = 'Paid'
                            elif new_paid > 0.01:
                                new_status = 'Partial'
                            else:
                                new_status = 'Pending'
                            cur.execute(
                                """
                                UPDATE payment_installments
                                SET transaction_id = CASE WHEN transaction_id=%s THEN NULL ELSE transaction_id END,
                                    paid_amount=%s,
                                    status=%s,
                                    paid_date=CASE WHEN %s <= 0.01 THEN NULL ELSE paid_date END
                                WHERE id=%s
                                """,
                                (trans['id'], new_paid, new_status, new_paid, inst_id),
                            )
                            self._refresh_invoice_status(cur, inst_row['invoice_id'])

                cur.execute("DELETE FROM account_transactions WHERE related_invoice_id=%s", (inv_id,))
                cur.execute("DELETE FROM payment_installments WHERE invoice_id=%s", (inv_id,))
                cur.execute("DELETE FROM payment_invoices WHERE id=%s", (inv_id,))
                conn.commit()
                return True
        except Exception as e:
            logging.error("Error deleting invoice (%s): %s", invoice_number, e)
            return False

    def generate_payment_plan(self, invoice_id: int, terms: int, start_date: datetime = None, interval_days: int = 30) -> bool:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute("SELECT total_amount, issue_date FROM payment_invoices WHERE id=%s", (invoice_id,))
                row = cur.fetchone()
                if not row:
                    return False
                total_amount = row['total_amount']
                issue_date_str = row['issue_date']
                base_date = start_date or self._parse_date(issue_date_str) or datetime.now()

                cur.execute("SELECT COALESCE(SUM(paid_amount), 0) AS total_paid FROM payment_installments WHERE invoice_id=%s", (invoice_id,))
                total_paid_so_far = cur.fetchone()['total_paid'] or 0.0
                remaining_principal = round(total_amount - total_paid_so_far, 2)
                if remaining_principal <= 0.01:
                    cur.execute("UPDATE payment_invoices SET terms_count=%s WHERE id=%s", (terms, invoice_id))
                    conn.commit()
                    return True

                cur.execute(
                    """
                    DELETE FROM payment_installments
                    WHERE invoice_id=%s AND status='Pending' AND COALESCE(paid_amount,0)=0 AND COALESCE(note,'')=''
                    """,
                    (invoice_id,),
                )
                cur.execute("SELECT count(*) AS kept_count FROM payment_installments WHERE invoice_id=%s", (invoice_id,))
                kept_count = cur.fetchone()['kept_count']
                new_terms_to_gen = terms - kept_count
                if new_terms_to_gen <= 0:
                    cur.execute("UPDATE payment_invoices SET terms_count=%s WHERE id=%s", (terms, invoice_id))
                    conn.commit()
                    return True

                term_amount = round(remaining_principal / new_terms_to_gen, 2)
                for i in range(1, new_terms_to_gen + 1):
                    actual_term_num = kept_count + i
                    if start_date:
                        due_date = start_date if i == 1 else start_date + timedelta(days=interval_days * (i - 1))
                    else:
                        due_date = base_date + timedelta(days=interval_days * (actual_term_num - 1))
                    current_amount = term_amount if i < new_terms_to_gen else round(remaining_principal - term_amount * (i - 1), 2)
                    cur.execute(
                        """
                        INSERT INTO payment_installments
                        (invoice_id, term_number, due_date, amount, paid_amount, penalty, status)
                        VALUES (%s, %s, %s, %s, 0, 0, 'Pending')
                        ON CONFLICT (invoice_id, term_number) DO UPDATE SET
                            due_date = EXCLUDED.due_date,
                            amount = EXCLUDED.amount
                        """,
                        (invoice_id, actual_term_num, due_date.strftime("%Y-%m-%d"), current_amount),
                    )
                cur.execute("UPDATE payment_invoices SET terms_count=%s WHERE id=%s", (terms, invoice_id))
                conn.commit()
                return True
        except Exception as e:
            logging.error("Error generating payment plan (invoice_id=%s): %s", invoice_id, e)
            return False

    def get_all_installments_for_export(self, invoice_number: str) -> List[dict]:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT pi.*
                FROM payment_installments pi
                JOIN payment_invoices i ON pi.invoice_id = i.id
                WHERE i.invoice_number = %s
                ORDER BY pi.term_number
                """,
                (invoice_number,),
            )
            return cur.fetchall()

    def restore_installments_from_import(self, invoice_number: str, installments: List[dict]) -> bool:
        if not installments:
            return True
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute("SELECT id FROM payment_invoices WHERE invoice_number=%s", (invoice_number,))
                row = cur.fetchone()
                if not row:
                    return False
                inv_id = row['id']
                cur.execute("DELETE FROM payment_installments WHERE invoice_id=%s", (inv_id,))
                for inst in installments:
                    cur.execute(
                        """
                        INSERT INTO payment_installments
                        (invoice_id, term_number, due_date, amount, paid_amount, penalty, status, paid_date, note)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            inv_id,
                            inst.get('term_number'),
                            inst.get('due_date'),
                            inst.get('amount'),
                            inst.get('paid_amount'),
                            inst.get('penalty'),
                            inst.get('status'),
                            inst.get('paid_date'),
                            inst.get('note'),
                        ),
                    )
                self._refresh_invoice_status(cur, inv_id)
                conn.commit()
                return True
        except Exception as e:
            logging.error("Error restoring installments for invoice %s: %s", invoice_number, e)
            return False

    def get_invoices(self):
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    i.*,
                    COALESCE(COUNT(pi.id), 0) as total_terms_count,
                    COALESCE(SUM(CASE WHEN pi.status = 'Paid' THEN 1 ELSE 0 END), 0) as paid_terms_count,
                    COALESCE(SUM(pi.paid_amount), 0) as total_paid_amount
                FROM payment_invoices i
                LEFT JOIN payment_installments pi ON pi.invoice_id = i.id
                GROUP BY i.id
                ORDER BY i.created_at DESC
                """
            )
            return cur.fetchall()

    def get_installments(self, invoice_id):
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT * FROM payment_installments WHERE invoice_id=%s ORDER BY term_number", (invoice_id,))
            return cur.fetchall()

    def update_installment_field(self, installment_id: int, field: str, value: Any) -> bool:
        if field not in self._INSTALLMENT_ALLOWED_FIELDS:
            raise ValueError(f"Invalid installment field: {field}")
        if field in self._INSTALLMENT_NUMERIC_FIELDS:
            parsed = self._to_float(value)
            if parsed is None:
                raise ValueError(f"Invalid numeric value for {field}")
            value = round(parsed, 2)
        elif field == "status":
            normalized_status = str(value).strip().title()
            if normalized_status not in self._INSTALLMENT_STATUS_VALUES:
                raise ValueError(f"Invalid status: {value}")
            value = normalized_status
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(f"UPDATE payment_installments SET {field}=%s WHERE id=%s", (value, installment_id))
                cur.execute("SELECT invoice_id, amount, penalty, paid_amount FROM payment_installments WHERE id=%s", (installment_id,))
                row = cur.fetchone()
                if not row:
                    return False
                inv_id = row['invoice_id']
                amount = row.get('amount') or 0.0
                penalty = row.get('penalty') or 0.0
                paid_amount = row.get('paid_amount') or 0.0
                total_due = amount + penalty
                if field in self._INSTALLMENT_NUMERIC_FIELDS:
                    if paid_amount >= total_due - 0.01:
                        auto_status = 'Paid'
                    elif paid_amount > 0.01:
                        auto_status = 'Partial'
                    else:
                        auto_status = 'Pending'
                    cur.execute("UPDATE payment_installments SET status=%s WHERE id=%s", (auto_status, installment_id))
                self._refresh_invoice_status(cur, inv_id)
                conn.commit()
                return True
        except Exception as e:
            logging.error("Error updating installment field (%s, %s): %s", installment_id, field, e)
            return False

    def _refresh_invoice_status(self, cursor, invoice_id):
        cursor.execute("SELECT count(*) AS cnt FROM payment_installments WHERE invoice_id=%s AND status='Pending'", (invoice_id,))
        pending_count = cursor.fetchone()['cnt']
        cursor.execute("SELECT count(*) AS cnt FROM payment_installments WHERE invoice_id=%s AND status='Paid'", (invoice_id,))
        paid_count = cursor.fetchone()['cnt']
        cursor.execute("SELECT count(*) AS cnt FROM payment_installments WHERE invoice_id=%s AND status='Partial'", (invoice_id,))
        partial_count = cursor.fetchone()['cnt']
        if pending_count == 0 and partial_count == 0 and paid_count > 0:
            inv_status = 'Paid'
        elif paid_count > 0 or partial_count > 0:
            inv_status = 'Partial'
        else:
            inv_status = 'Unpaid'
        cursor.execute("UPDATE payment_invoices SET status=%s WHERE id=%s", (inv_status, invoice_id))

    def refresh_invoice_status(self, invoice_id: int) -> bool:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute("SELECT id FROM payment_invoices WHERE id=%s", (invoice_id,))
                if not cur.fetchone():
                    return False
                self._refresh_invoice_status(cur, invoice_id)
                conn.commit()
                return True
        except Exception as e:
            logging.error("Error refreshing invoice status (invoice_id=%s): %s", invoice_id, e)
            return False

    def add_advance(self, customer_name, customer_cnpj, amount, date, description, account_id, transaction_id):
        amount = self._normalize_positive_amount(amount)
        if amount is None:
            return None
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO customer_advances
                    (customer_name, customer_cnpj, amount, remaining_amount, date, description, account_id, transaction_id, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    RETURNING id
                    """,
                    (customer_name, customer_cnpj, amount, amount, date, description, account_id, transaction_id),
                )
                adv_id = cur.fetchone()['id']
                conn.commit()
                return adv_id
        except Exception as e:
            logging.error("Error adding advance (account_id=%s, transaction_id=%s): %s", account_id, transaction_id, e)
            return None

    def get_advances_by_customer(self, customer_cnpj=None, customer_name=None):
        query = "SELECT * FROM customer_advances WHERE remaining_amount > 0 AND status = 'Open'"
        params: List[Any] = []
        if customer_cnpj:
            clean_cnpj = re.sub(r'\D', '', customer_cnpj)
            query += " AND regexp_replace(COALESCE(customer_cnpj,''), '[^0-9]', '', 'g') = %s"
            params.append(clean_cnpj)
        elif customer_name:
            query += " AND customer_name = %s"
            params.append(customer_name)
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def apply_advance_to_installment(self, advance_id, installment_id, amount_to_apply):
        amount_to_apply = self._normalize_positive_amount(amount_to_apply)
        if amount_to_apply is None:
            return False
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT remaining_amount FROM customer_advances WHERE id=%s AND status='Open' FOR UPDATE",
                    (advance_id,),
                )
                adv = cur.fetchone()
                if not adv:
                    return False
                if amount_to_apply > (adv.get('remaining_amount') or 0.0) + 0.01:
                    return False

                cur.execute(
                    "SELECT paid_amount, amount, penalty, invoice_id FROM payment_installments WHERE id=%s FOR UPDATE",
                    (installment_id,),
                )
                inst = cur.fetchone()
                if not inst:
                    return False
                curr_paid = inst.get('paid_amount') or 0.0
                total_amt = (inst.get('amount') or 0.0) + (inst.get('penalty') or 0.0)
                inv_id = inst['invoice_id']
                due_remaining = max(0.0, total_amt - curr_paid)
                if amount_to_apply > due_remaining + 0.01:
                    return False

                cur.execute("UPDATE customer_advances SET remaining_amount = remaining_amount - %s WHERE id=%s", (amount_to_apply, advance_id))
                cur.execute("UPDATE customer_advances SET status='Used' WHERE id=%s AND remaining_amount < 0.01", (advance_id,))

                new_paid = curr_paid + amount_to_apply
                new_status = 'Paid' if new_paid >= total_amt - 0.01 else 'Partial'
                cur.execute(
                    """
                    UPDATE payment_installments
                    SET paid_amount=%s, status=%s, paid_date=%s, note=%s
                    WHERE id=%s
                    """,
                    (new_paid, new_status, datetime.now().strftime("%Y-%m-%d"), f"Written off from advance ID {advance_id}", installment_id),
                )
                self._refresh_invoice_status(cur, inv_id)
                conn.commit()
                return True
        except Exception as e:
            logging.error("Error applying advance (advance_id=%s, installment_id=%s): %s", advance_id, installment_id, e)
            return False
