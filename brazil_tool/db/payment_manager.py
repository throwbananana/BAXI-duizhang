# -*- coding: utf-8 -*-
import logging
import os
import sqlite3
import re
from datetime import datetime, timedelta
from typing import List, Dict
from brazil_tool.core.utils import br_to_float

class PaymentManager:
    """Payment System Database Manager."""
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

    def __init__(self, db_path="invoice_payment.db"):
        self.db_path = db_path
        self.init_db()

    def _connect(self):
        timeout_sec = float(os.getenv("BRAZIL_TOOL_SQLITE_TIMEOUT", "20"))
        conn = sqlite3.connect(self.db_path, timeout=timeout_sec)
        try:
            conn.execute(f"PRAGMA busy_timeout = {int(timeout_sec * 1000)}")
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA journal_mode=WAL")
        except sqlite3.DatabaseError:
            # Keep compatibility for restricted environments or read-only cases.
            pass
        return conn

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
        conn = self._connect()
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS payment_invoices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_number TEXT UNIQUE,
                issuer_name TEXT,
                issue_date TEXT,
                total_amount REAL,
                terms_count INTEGER DEFAULT 1,
                status TEXT DEFAULT 'Unpaid',
                file_name TEXT,
                created_at TEXT,
                natureza_operacao TEXT,
                destinatario_name TEXT,
                destinatario_cnpj TEXT,
                issuer_cnpj TEXT,
                description TEXT DEFAULT ''
            )
        ''')
        
        # Migrations
        cursor.execute("PRAGMA table_info(payment_invoices)")
        columns = [info[1] for info in cursor.fetchall()]
        if "natureza_operacao" not in columns:
            cursor.execute("ALTER TABLE payment_invoices ADD COLUMN natureza_operacao TEXT")
        if "destinatario_name" not in columns:
            cursor.execute("ALTER TABLE payment_invoices ADD COLUMN destinatario_name TEXT")
        if "destinatario_cnpj" not in columns:
            cursor.execute("ALTER TABLE payment_invoices ADD COLUMN destinatario_cnpj TEXT")
        if "issuer_cnpj" not in columns:
            cursor.execute("ALTER TABLE payment_invoices ADD COLUMN issuer_cnpj TEXT")
        if "created_at" not in columns:
            cursor.execute("ALTER TABLE payment_invoices ADD COLUMN created_at TEXT")
        if "description" not in columns:
            cursor.execute("ALTER TABLE payment_invoices ADD COLUMN description TEXT DEFAULT ''")

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS payment_installments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_id INTEGER,
                term_number INTEGER,
                due_date TEXT,
                amount REAL,
                paid_amount REAL DEFAULT 0,
                penalty REAL DEFAULT 0,
                status TEXT DEFAULT 'Pending',
                paid_date TEXT,
                note TEXT,
                account_id INTEGER,
                transaction_id INTEGER,
                created_at TEXT,
                FOREIGN KEY(invoice_id) REFERENCES payment_invoices(id)
            )
        ''')
        
        cursor.execute("PRAGMA table_info(payment_installments)")
        columns = [info[1] for info in cursor.fetchall()]
        for col, dtype in [("paid_amount", "REAL DEFAULT 0"), ("note", "TEXT"), ("penalty", "REAL DEFAULT 0"), ("account_id", "INTEGER"), ("transaction_id", "INTEGER"), ("created_at", "TEXT")]:
            if col not in columns:
                cursor.execute(f"ALTER TABLE payment_installments ADD COLUMN {col} {dtype}")

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE,
                bank_info TEXT,
                currency TEXT DEFAULT 'BRL',
                initial_balance REAL DEFAULT 0,
                current_balance REAL DEFAULT 0,
                note TEXT,
                is_active INTEGER DEFAULT 1
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS account_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER,
                date TEXT,
                type TEXT, 
                amount REAL,
                description TEXT,
                related_invoice_id INTEGER,
                related_installment_id INTEGER,
                created_at TEXT,
                FOREIGN KEY(account_id) REFERENCES accounts(id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS customer_advances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_name TEXT,
                customer_cnpj TEXT,
                amount REAL,
                remaining_amount REAL,
                date TEXT,
                description TEXT,
                account_id INTEGER,
                transaction_id INTEGER,
                created_at TEXT,
                status TEXT DEFAULT 'Open',
                FOREIGN KEY(account_id) REFERENCES accounts(id)
            )
        ''')

        # Performance indexes for frequent list/detail loading paths.
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_payment_installments_invoice_id ON payment_installments(invoice_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_payment_installments_status ON payment_installments(status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_payment_installments_invoice_term ON payment_installments(invoice_id, term_number)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_payment_invoices_created_at ON payment_invoices(created_at)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_account_transactions_account_date ON account_transactions(account_id, date, created_at)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_account_transactions_invoice ON account_transactions(related_invoice_id)")

        conn.commit()
        conn.close()


    def find_invoice_id_by_number(self, number: str):
        conn = self._connect()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT id FROM payment_invoices WHERE invoice_number = ?", (number,))
            row = cursor.fetchone()
            return row[0] if row else None
        finally:
            conn.close()

    def get_all_existing_invoice_numbers(self) -> set:
        """Return all invoice numbers for fast duplicate checks."""
        conn = self._connect()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT invoice_number FROM payment_invoices")
            return {row[0] for row in cursor.fetchall() if row and row[0]}
        finally:
            conn.close()

    def get_account_id_for_invoice(self, invoice_id: int):
        conn = self._connect()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT account_id FROM account_transactions WHERE related_invoice_id=?", (invoice_id,))
            row = cursor.fetchone()
            return row[0] if row else None
        finally:
            conn.close()

    def get_all_installments_extended(self):
        conn = self._connect()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT 
                    i.issuer_name, i.destinatario_name, i.destinatario_cnpj, i.issuer_cnpj,
                    p.due_date, p.amount, p.paid_amount, p.status, i.invoice_number
                FROM payment_installments p
                JOIN payment_invoices i ON p.invoice_id = i.id
            """)
            return cursor.fetchall()
        finally:
            conn.close()

    def search_pending_installments(self, patterns: List[str], term_number: int = None):
        if not patterns: return []
        conn = self._connect()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        try:
            query = """
                SELECT i.id as invoice_id, p.id as installment_id, p.amount, 
                       i.destinatario_name, i.destinatario_cnpj, p.due_date, i.invoice_number 
                       , p.paid_amount, p.penalty, p.term_number
                FROM payment_installments p
                JOIN payment_invoices i ON p.invoice_id = i.id
                WHERE p.status != 'Paid' AND (
            """
            conditions = []
            params = []
            for pat in patterns:
                conditions.append("i.invoice_number LIKE ?")
                params.append(pat)
            
            query += " OR ".join(conditions) + ")"
            
            if term_number is not None:
                query += " AND p.term_number = ?"
                params.append(term_number)
            
            cursor.execute(query, params)
            return cursor.fetchall()
        finally:
            conn.close()

    def register_payment(self, installment_id: int, amount: float, date: str, account_id: int, description: str) -> bool:
        amount = self._normalize_positive_amount(amount)
        if amount is None:
            return False

        conn = self._connect()
        cursor = conn.cursor()
        try:
            # 1. Fetch Installment Info
            cursor.execute("SELECT invoice_id, paid_amount, amount, penalty FROM payment_installments WHERE id=?", (installment_id,))
            inst = cursor.fetchone()
            if not inst: return False
            inv_id, current_paid, total_amount, penalty = inst
            current_paid = current_paid or 0.0
            penalty = penalty or 0.0
            due_total = total_amount + penalty
            due_remaining = max(0.0, due_total - current_paid)
            if amount > due_remaining + 0.01:
                return False
            
            # 2. Add Transaction
            cursor.execute('''
                INSERT INTO account_transactions (account_id, date, type, amount, description, related_invoice_id, related_installment_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (account_id, date, 'Income', amount, description, inv_id, installment_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            trans_id = cursor.lastrowid
            
            # 3. Update Account Balance
            cursor.execute("UPDATE accounts SET current_balance = current_balance + ? WHERE id=?", (amount, account_id))
            
            # 4. Update Installment Status
            new_paid = current_paid + amount
            new_status = 'Paid' if new_paid >= due_total - 0.01 else 'Partial'
            
            cursor.execute('''
                UPDATE payment_installments 
                SET paid_amount = ?, status = ?, paid_date = ?, transaction_id = ?, account_id = ?
                WHERE id = ?
            ''', (new_paid, new_status, date, trans_id, account_id, installment_id))
            
            # 5. Refresh Invoice Status
            self._refresh_invoice_status(cursor, inv_id)
            
            conn.commit()
            return True
        except Exception as e:
            logging.error("Error registering payment (installment_id=%s, account_id=%s): %s", installment_id, account_id, e)
            conn.rollback()
            return False
        finally:
            conn.close()

    def add_account(self, name, bank_info="", currency="BRL", initial_balance=0.0, note=""):
        conn = self._connect()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO accounts (name, bank_info, currency, initial_balance, current_balance, note)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (name, bank_info, currency, initial_balance, initial_balance, note))
            conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            return None
        finally:
            conn.close()

    def update_account(self, account_id, name, bank_info, currency, note, is_active=1) -> bool:
        conn = self._connect()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                UPDATE accounts SET name=?, bank_info=?, currency=?, note=?, is_active=?
                WHERE id=?
            ''', (name, bank_info, currency, note, is_active, account_id))
            if cursor.rowcount <= 0:
                conn.rollback()
                return False
            conn.commit()
            return True
        except Exception as e:
            logging.error("Error updating account (%s): %s", account_id, e)
            conn.rollback()
            return False
        finally:
            conn.close()

    def delete_account(self, account_id) -> bool:
        conn = self._connect()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT id FROM accounts WHERE id=?", (account_id,))
            if not cursor.fetchone():
                return False

            cursor.execute("SELECT count(*) FROM account_transactions WHERE account_id=?", (account_id,))
            if cursor.fetchone()[0] > 0:
                cursor.execute("UPDATE accounts SET is_active=0 WHERE id=?", (account_id,))
            else:
                cursor.execute("DELETE FROM accounts WHERE id=?", (account_id,))
            conn.commit()
            return True
        except Exception as e:
            logging.error("Error deleting account (%s): %s", account_id, e)
            conn.rollback()
            return False
        finally:
            conn.close()

    def get_accounts(self, active_only=True):
        conn = self._connect()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        if active_only:
            cursor.execute("SELECT * FROM accounts WHERE is_active=1 ORDER BY name")
        else:
            cursor.execute("SELECT * FROM accounts ORDER BY name")
        rows = cursor.fetchall()
        conn.close()
        return rows

    def get_account_balance(self, account_id):
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("SELECT current_balance FROM accounts WHERE id=?", (account_id,))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else 0.0

    def add_transaction(self, account_id, date, trans_type, amount, description, related_invoice_id=None, related_installment_id=None):
        amount = self._normalize_positive_amount(amount)
        if amount is None:
            return None

        trans_type = str(trans_type or "").strip().title()
        if trans_type not in {"Income", "Expense"}:
            return None

        conn = self._connect()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO account_transactions (account_id, date, type, amount, description, related_invoice_id, related_installment_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (account_id, date, trans_type, amount, description, related_invoice_id, related_installment_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            trans_id = cursor.lastrowid

            if trans_type == 'Income':
                cursor.execute("UPDATE accounts SET current_balance = current_balance + ? WHERE id=?", (amount, account_id))
            else:
                cursor.execute("UPDATE accounts SET current_balance = current_balance - ? WHERE id=?", (amount, account_id))
            
            conn.commit()
            return trans_id
        except Exception as e:
            logging.error("Error adding transaction (account_id=%s): %s", account_id, e)
            conn.rollback()
            return None
        finally:
            conn.close()

    def delete_transaction(self, trans_id) -> bool:
        conn = self._connect()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM account_transactions WHERE id=?", (trans_id,))
            trans = cursor.fetchone()
            if not trans:
                return False

            if trans['type'] == 'Income':
                cursor.execute("UPDATE accounts SET current_balance = current_balance - ? WHERE id=?", (trans['amount'], trans['account_id']))
            else:
                cursor.execute("UPDATE accounts SET current_balance = current_balance + ? WHERE id=?", (trans['amount'], trans['account_id']))

            cursor.execute("DELETE FROM account_transactions WHERE id=?", (trans_id,))
            
            if trans['related_installment_id']:
                inst_id = trans['related_installment_id']
                cursor.execute("SELECT paid_amount, invoice_id FROM payment_installments WHERE id=?", (inst_id,))
                inst_row = cursor.fetchone()
                
                if inst_row:
                    curr_paid = inst_row['paid_amount'] or 0.0
                    new_paid = max(0.0, curr_paid - trans['amount'])
                    new_status = 'Pending'
                    
                    if new_paid > 0.01:
                        new_status = 'Partial'
                    
                    cursor.execute('''
                        UPDATE payment_installments 
                        SET transaction_id = CASE WHEN transaction_id=? THEN NULL ELSE transaction_id END,
                            paid_amount = ?,
                            status = ?,
                            paid_date = CASE WHEN ? <= 0.01 THEN NULL ELSE paid_date END
                        WHERE id=?
                    ''', (trans_id, new_paid, new_status, new_paid, inst_id))

                    self._refresh_invoice_status(cursor, inst_row['invoice_id'])

            conn.commit()
            return True
        except Exception as e:
            logging.error("Error deleting transaction (%s): %s", trans_id, e)
            conn.rollback()
            return False
        finally:
            conn.close()

    def get_transactions(self, account_id, limit=100):
        conn = self._connect()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('''
            SELECT t.*, i.invoice_number, i.destinatario_name, i.issuer_name
            FROM account_transactions t
            LEFT JOIN payment_invoices i ON t.related_invoice_id = i.id
            WHERE t.account_id=? 
            ORDER BY t.date DESC, t.created_at DESC LIMIT ?
        ''', (account_id, limit))
        rows = cursor.fetchall()
        conn.close()
        return rows

    def get_need_pdf_invoices(self):
        """Return list of (id, invoice_number) for invoices marked as NEED PDF."""
        conn = self._connect()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT id, invoice_number
                FROM payment_invoices
                WHERE natureza_operacao LIKE '%NEED PDF%'
                   OR description LIKE '%NEED PDF%'
            """)
            return cursor.fetchall()
        finally:
            conn.close()

    def upsert_invoice(self, invoice_data: dict):
        if not isinstance(invoice_data, dict):
            return None
        required_fields = ("invoice_number", "issuer_name", "issue_date", "total_amount", "file_name")
        if any(invoice_data.get(k) is None for k in required_fields):
            return None

        conn = self._connect()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT id FROM payment_invoices WHERE invoice_number = ?", (invoice_data['invoice_number'],))
            row = cursor.fetchone()
            
            if row:
                inv_id = row[0]
                cursor.execute('''
                    UPDATE payment_invoices 
                    SET issuer_name=?, issue_date=?, total_amount=?, file_name=?, natureza_operacao=?, destinatario_name=?, destinatario_cnpj=?, issuer_cnpj=?, description=COALESCE(?, description)
                    WHERE id=?
                ''', (
                    invoice_data['issuer_name'], 
                    invoice_data['issue_date'], 
                    invoice_data['total_amount'],
                    invoice_data['file_name'],
                    invoice_data.get('natureza_operacao', ''),
                    invoice_data.get('destinatario_name', ''),
                    invoice_data.get('destinatario_cnpj', ''),
                    invoice_data.get('issuer_cnpj', ''),
                    invoice_data.get('description'),
                    inv_id
                ))
            else:
                cursor.execute('''
                    INSERT INTO payment_invoices (invoice_number, issuer_name, issue_date, total_amount, file_name, created_at, status, natureza_operacao, destinatario_name, destinatario_cnpj, issuer_cnpj, description)
                    VALUES (?, ?, ?, ?, ?, ?, 'Unpaid', ?, ?, ?, ?, ?)
                ''', (
                    invoice_data['invoice_number'], 
                    invoice_data['issuer_name'], 
                    invoice_data['issue_date'], 
                    invoice_data['total_amount'],
                    invoice_data['file_name'],
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    invoice_data.get('natureza_operacao', ''),
                    invoice_data.get('destinatario_name', ''),
                    invoice_data.get('destinatario_cnpj', ''),
                    invoice_data.get('issuer_cnpj', ''),
                    invoice_data.get('description', '')
                ))
                inv_id = cursor.lastrowid
                
            conn.commit()
            return inv_id
        except Exception as e:
            logging.error("Error upserting invoice (%s): %s", invoice_data.get("invoice_number"), e)
            conn.rollback()
            return None
        finally:
            conn.close()

    def update_invoice_number(self, invoice_id: int, new_number: str) -> bool:
        """Update the invoice number for a given ID."""
        conn = self._connect()
        cursor = conn.cursor()
        try:
            cursor.execute("UPDATE payment_invoices SET invoice_number = ? WHERE id = ?", (new_number, invoice_id))
            if cursor.rowcount <= 0:
                conn.rollback()
                return False
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False # New number already exists
        except Exception as e:
            logging.error("Error updating invoice number (invoice_id=%s): %s", invoice_id, e)
            conn.rollback()
            return False
        finally:
            conn.close()

    def delete_invoice_by_number(self, invoice_number: str):
        """Delete invoice and related installments/transactions from database."""
        conn = self._connect()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT id FROM payment_invoices WHERE invoice_number = ?", (invoice_number,))
            row = cursor.fetchone()
            if not row: return False
            inv_id = row['id']

            # 1) Revert balances and installment states for related transactions
            cursor.execute("SELECT * FROM account_transactions WHERE related_invoice_id = ? ORDER BY id DESC", (inv_id,))
            trans_rows = cursor.fetchall()
            for trans in trans_rows:
                if trans['type'] == 'Income':
                    cursor.execute("UPDATE accounts SET current_balance = current_balance - ? WHERE id=?", (trans['amount'], trans['account_id']))
                else:
                    cursor.execute("UPDATE accounts SET current_balance = current_balance + ? WHERE id=?", (trans['amount'], trans['account_id']))

                if trans['related_installment_id']:
                    inst_id = trans['related_installment_id']
                    cursor.execute("SELECT paid_amount, amount, penalty, invoice_id FROM payment_installments WHERE id=?", (inst_id,))
                    inst_row = cursor.fetchone()
                    if inst_row:
                        curr_paid = inst_row['paid_amount'] or 0.0
                        total_due = (inst_row['amount'] or 0.0) + (inst_row['penalty'] or 0.0)
                        new_paid = max(0.0, curr_paid - (trans['amount'] or 0.0))
                        if new_paid >= total_due - 0.01:
                            new_status = 'Paid'
                        elif new_paid > 0.01:
                            new_status = 'Partial'
                        else:
                            new_status = 'Pending'

                        cursor.execute('''
                            UPDATE payment_installments
                            SET transaction_id = CASE WHEN transaction_id=? THEN NULL ELSE transaction_id END,
                                paid_amount = ?,
                                status = ?,
                                paid_date = CASE WHEN ? <= 0.01 THEN NULL ELSE paid_date END
                            WHERE id=?
                        ''', (trans['id'], new_paid, new_status, new_paid, inst_id))
                        self._refresh_invoice_status(cursor, inst_row['invoice_id'])

            cursor.execute("DELETE FROM account_transactions WHERE related_invoice_id = ?", (inv_id,))

            # 2) Delete installments
            cursor.execute("DELETE FROM payment_installments WHERE invoice_id = ?", (inv_id,))
            
            # 3) Delete invoice
            cursor.execute("DELETE FROM payment_invoices WHERE id = ?", (inv_id,))
            
            conn.commit()
            return True
        except Exception as e:
            logging.error("Error deleting invoice (%s): %s", invoice_number, e)
            conn.rollback()
            return False
        finally:
            conn.close()

    def generate_payment_plan(self, invoice_id: int, terms: int, start_date: datetime = None, interval_days: int = 30) -> bool:
        conn = self._connect()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT total_amount, issue_date FROM payment_invoices WHERE id=?", (invoice_id,))
            row = cursor.fetchone()
            if not row:
                return False
            
            total_amount, issue_date_str = row
            
            base_date = datetime.now()
            if not start_date:
                base_date = self._parse_date(issue_date_str) or datetime.now()
            else:
                base_date = start_date

            cursor.execute("SELECT SUM(paid_amount) FROM payment_installments WHERE invoice_id=?", (invoice_id,))
            total_paid_so_far = cursor.fetchone()[0] or 0.0
            
            remaining_principal = round(total_amount - total_paid_so_far, 2)
            
            if remaining_principal <= 0.01:
                cursor.execute("UPDATE payment_invoices SET terms_count=? WHERE id=?", (terms, invoice_id))
                conn.commit()
                return True

            cursor.execute("""
                DELETE FROM payment_installments 
                WHERE invoice_id=? AND status = 'Pending' AND paid_amount = 0 AND (note IS NULL OR note = '')
            """, (invoice_id,))
            
            cursor.execute("SELECT count(*) FROM payment_installments WHERE invoice_id=?", (invoice_id,))
            kept_count = cursor.fetchone()[0]
            
            new_terms_to_gen = terms - kept_count
            if new_terms_to_gen <= 0:
                cursor.execute("UPDATE payment_invoices SET terms_count=? WHERE id=?", (terms, invoice_id))
                conn.commit()
                return True

            term_amount = round(remaining_principal / new_terms_to_gen, 2)
            
            for i in range(1, new_terms_to_gen + 1):
                actual_term_num = kept_count + i
                if start_date and i == 1 and kept_count == 0:
                    due_date = start_date
                elif start_date:
                    if i == 1: due_date = start_date
                    else: due_date = start_date + timedelta(days=interval_days * (i - 1))
                else:
                    # 默认首期为发票日期 (actual_term_num=1 时偏移为0)
                    due_date = base_date + timedelta(days=interval_days * (actual_term_num - 1))

                current_amount = term_amount if i < new_terms_to_gen else round(remaining_principal - term_amount * (i-1), 2)
                
                cursor.execute('''
                    INSERT INTO payment_installments (invoice_id, term_number, due_date, amount, paid_amount, penalty, status)
                    VALUES (?, ?, ?, ?, 0, 0, 'Pending')
                ''', (invoice_id, actual_term_num, due_date.strftime("%Y-%m-%d"), current_amount))
            
            cursor.execute("UPDATE payment_invoices SET terms_count=? WHERE id=?", (terms, invoice_id))
            conn.commit()
            return True
        except Exception as e:
            logging.error("Error generating payment plan (invoice_id=%s): %s", invoice_id, e)
            conn.rollback()
            return False
        finally:
            conn.close()

    def get_all_installments_for_export(self, invoice_number: str) -> List[dict]:
        conn = self._connect()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT pi.* FROM payment_installments pi
                JOIN payment_invoices i ON pi.invoice_id = i.id
                WHERE i.invoice_number = ?
            """, (invoice_number,))
            return [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()

    def restore_installments_from_import(self, invoice_number: str, installments: List[dict]) -> bool:
        if not installments:
            return True
        conn = self._connect()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT id FROM payment_invoices WHERE invoice_number = ?", (invoice_number,))
            row = cursor.fetchone()
            if not row:
                return False
            inv_id = row[0]
            
            cursor.execute("DELETE FROM payment_installments WHERE invoice_id = ?", (inv_id,))
            
            for inst in installments:
                cursor.execute("""
                    INSERT INTO payment_installments 
                    (invoice_id, term_number, due_date, amount, paid_amount, penalty, status, paid_date, note)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    inv_id, inst.get('term_number'), inst.get('due_date'),
                    inst.get('amount'), inst.get('paid_amount'), inst.get('penalty'),
                    inst.get('status'), inst.get('paid_date'), inst.get('note')
                ))
            self._refresh_invoice_status(cursor, inv_id)
            conn.commit()
            return True
        except Exception as e:
            logging.error("Error restoring installments for invoice %s: %s", invoice_number, e)
            conn.rollback()
            return False
        finally:
            conn.close()

    def get_invoices(self):
        conn = self._connect()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('''
            SELECT 
                i.*,
                COALESCE(COUNT(pi.id), 0) as total_terms_count,
                COALESCE(SUM(CASE WHEN pi.status = 'Paid' THEN 1 ELSE 0 END), 0) as paid_terms_count,
                COALESCE(SUM(pi.paid_amount), 0) as total_paid_amount
            FROM payment_invoices i
            LEFT JOIN payment_installments pi ON pi.invoice_id = i.id
            GROUP BY i.id
            ORDER BY i.created_at DESC
        ''')
        rows = cursor.fetchall()
        conn.close()
        return rows

    def get_installments(self, invoice_id):
        conn = self._connect()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM payment_installments WHERE invoice_id=? ORDER BY term_number", (invoice_id,))
        rows = cursor.fetchall()
        conn.close()
        return rows

    def update_installment_field(self, installment_id: int, field: str, value: any) -> bool:
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

        conn = self._connect()
        cursor = conn.cursor()
        try:
            cursor.execute(f"UPDATE payment_installments SET {field}=? WHERE id=?", (value, installment_id))
            cursor.execute("SELECT invoice_id, amount, penalty, paid_amount FROM payment_installments WHERE id=?", (installment_id,))
            row = cursor.fetchone()
            if row:
                inv_id = row[0]
                amount = row[1] or 0.0
                penalty = row[2] or 0.0
                paid_amount = row[3] or 0.0
                total_due = amount + penalty

                if field in self._INSTALLMENT_NUMERIC_FIELDS:
                    if paid_amount >= total_due - 0.01:
                        auto_status = 'Paid'
                    elif paid_amount > 0.01:
                        auto_status = 'Partial'
                    else:
                        auto_status = 'Pending'
                    cursor.execute("UPDATE payment_installments SET status=? WHERE id=?", (auto_status, installment_id))

                self._refresh_invoice_status(cursor, inv_id)
            else:
                conn.rollback()
                return False
            conn.commit()
            return True
        except Exception as e:
            logging.error("Error updating installment field (%s, %s): %s", installment_id, field, e)
            conn.rollback()
            return False
        finally:
            conn.close()

    def _refresh_invoice_status(self, cursor, invoice_id):
        cursor.execute("SELECT count(*) FROM payment_installments WHERE invoice_id=? AND status='Pending'", (invoice_id,))
        pending_count = cursor.fetchone()[0]
        cursor.execute("SELECT count(*) FROM payment_installments WHERE invoice_id=? AND status='Paid'", (invoice_id,))
        paid_count = cursor.fetchone()[0]
        cursor.execute("SELECT count(*) FROM payment_installments WHERE invoice_id=? AND status='Partial'", (invoice_id,))
        partial_count = cursor.fetchone()[0]
        
        if pending_count == 0 and partial_count == 0 and paid_count > 0:
            inv_status = 'Paid'
        elif paid_count > 0 or partial_count > 0:
            inv_status = 'Partial'
        else:
            inv_status = 'Unpaid'
        
        cursor.execute("UPDATE payment_invoices SET status=? WHERE id=?", (inv_status, invoice_id))

    def refresh_invoice_status(self, invoice_id: int) -> bool:
        conn = self._connect()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT id FROM payment_invoices WHERE id=?", (invoice_id,))
            if not cursor.fetchone():
                return False
            self._refresh_invoice_status(cursor, invoice_id)
            conn.commit()
            return True
        except Exception as e:
            logging.error("Error refreshing invoice status (invoice_id=%s): %s", invoice_id, e)
            conn.rollback()
            return False
        finally:
            conn.close()

    def add_advance(self, customer_name, customer_cnpj, amount, date, description, account_id, transaction_id):
        amount = self._normalize_positive_amount(amount)
        if amount is None:
            return None

        conn = self._connect()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO customer_advances (customer_name, customer_cnpj, amount, remaining_amount, date, description, account_id, transaction_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (customer_name, customer_cnpj, amount, amount, date, description, account_id, transaction_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logging.error("Error adding advance (account_id=%s, transaction_id=%s): %s", account_id, transaction_id, e)
            conn.rollback()
            return None
        finally:
            conn.close()

    def get_advances_by_customer(self, customer_cnpj=None, customer_name=None):
        conn = self._connect()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        if customer_cnpj:
            clean_cnpj = re.sub(r'\D', '', customer_cnpj)
            cursor.execute("SELECT * FROM customer_advances WHERE REPLACE(REPLACE(REPLACE(customer_cnpj, '.', ''), '/', ''), '-', '') = ? AND remaining_amount > 0 AND status = 'Open'", (clean_cnpj,))
        elif customer_name:
            cursor.execute("SELECT * FROM customer_advances WHERE customer_name = ? AND remaining_amount > 0 AND status = 'Open'", (customer_name,))
        else:
            cursor.execute("SELECT * FROM customer_advances WHERE remaining_amount > 0 AND status = 'Open'")
        rows = cursor.fetchall()
        conn.close()
        return rows

    def apply_advance_to_installment(self, advance_id, installment_id, amount_to_apply):
        amount_to_apply = self._normalize_positive_amount(amount_to_apply)
        if amount_to_apply is None:
            return False

        conn = self._connect()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT remaining_amount FROM customer_advances WHERE id = ? AND status = 'Open'", (advance_id,))
            adv = cursor.fetchone()
            if not adv:
                return False
            if amount_to_apply > (adv[0] or 0.0) + 0.01:
                return False

            cursor.execute("SELECT paid_amount, amount, penalty, invoice_id FROM payment_installments WHERE id = ?", (installment_id,))
            inst = cursor.fetchone()
            if not inst:
                return False

            curr_paid = inst[0] or 0.0
            total_amt = (inst[1] or 0.0) + (inst[2] or 0.0)
            inv_id = inst[3]
            due_remaining = max(0.0, total_amt - curr_paid)
            if amount_to_apply > due_remaining + 0.01:
                return False

            cursor.execute("UPDATE customer_advances SET remaining_amount = remaining_amount - ? WHERE id = ?", (amount_to_apply, advance_id))
            cursor.execute("UPDATE customer_advances SET status = 'Used' WHERE id = ? AND remaining_amount < 0.01", (advance_id,))

            new_paid = curr_paid + amount_to_apply
            new_status = 'Paid' if new_paid >= total_amt - 0.01 else 'Partial'

            cursor.execute('''
                UPDATE payment_installments
                SET paid_amount = ?, status = ?, paid_date = ?, note = ?
                WHERE id = ?
            ''', (new_paid, new_status, datetime.now().strftime("%Y-%m-%d"), f"Written off from advance ID {advance_id}", installment_id))

            self._refresh_invoice_status(cursor, inv_id)
            conn.commit()
            return True
        except Exception as e:
            logging.error(
                "Error applying advance (advance_id=%s, installment_id=%s): %s",
                advance_id,
                installment_id,
                e,
            )
            conn.rollback()
            return False
        finally:
            conn.close()

