# -*- coding: utf-8 -*-
import os
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests


class NetworkPaymentManager:
    """
    Network-based Payment System Manager.
    Proxies calls to the REST API server.
    """

    def __init__(self, server_url: str = "http://localhost:8000", timeout_sec: float = None):
        self.server_url = server_url.rstrip("/")
        self.timeout_sec = float(timeout_sec if timeout_sec is not None else os.getenv("BRAZIL_TOOL_HTTP_TIMEOUT", "10"))
        self.db_path = None  # Marker: no local SQLite path in network mode.
        token = os.getenv("BRAZIL_TOOL_SERVER_TOKEN", "").strip()
        self._default_headers = {"X-API-Key": token} if token else {}

    def _url(self, endpoint: str) -> str:
        return f"{self.server_url}{endpoint}"

    def _request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        if "timeout" not in kwargs:
            kwargs["timeout"] = self.timeout_sec
        headers = kwargs.pop("headers", None) or {}
        if self._default_headers:
            headers = {**self._default_headers, **headers}
        if headers:
            kwargs["headers"] = headers
        resp = requests.request(method=method, url=self._url(endpoint), **kwargs)
        resp.raise_for_status()
        return resp

    def _log_error(self, action: str, err: Exception):
        logging.error("Network error in %s: %s", action, err)

    def init_db(self):
        # Server handles DB initialization
        pass

    # --- Account Management ---
    def add_account(self, name, bank_info="", currency="BRL", initial_balance=0.0, note=""):
        try:
            resp = self._request(
                "POST",
                "/accounts",
                json={
                    "name": name,
                    "bank_info": bank_info,
                    "currency": currency,
                    "initial_balance": initial_balance,
                    "note": note,
                },
            )
            return resp.json().get("id")
        except Exception as e:
            self._log_error("add_account", e)
            return None

    def update_account(self, account_id, name, bank_info, currency, note, is_active=1) -> bool:
        try:
            self._request(
                "PUT",
                f"/accounts/{account_id}",
                json={
                    "account_id": account_id,
                    "name": name,
                    "bank_info": bank_info,
                    "currency": currency,
                    "note": note,
                    "is_active": is_active,
                },
            )
            return True
        except Exception as e:
            self._log_error("update_account", e)
            return False

    def delete_account(self, account_id) -> bool:
        try:
            self._request("DELETE", f"/accounts/{account_id}")
            return True
        except Exception as e:
            self._log_error("delete_account", e)
            return False

    def get_accounts(self, active_only=True):
        try:
            resp = self._request("GET", "/accounts", params={"active_only": active_only})
            return resp.json()
        except Exception as e:
            self._log_error("get_accounts", e)
            return []

    def get_account_balance(self, account_id):
        try:
            resp = self._request("GET", f"/accounts/{account_id}/balance")
            return resp.json().get("balance", 0.0)
        except Exception as e:
            self._log_error("get_account_balance", e)
            return 0.0

    # --- Transaction Management ---
    def add_transaction(self, account_id, date, trans_type, amount, description, related_invoice_id=None, related_installment_id=None):
        try:
            resp = self._request(
                "POST",
                "/transactions",
                json={
                    "account_id": account_id,
                    "date": date,
                    "trans_type": trans_type,
                    "amount": amount,
                    "description": description,
                    "related_invoice_id": related_invoice_id,
                    "related_installment_id": related_installment_id,
                },
            )
            return resp.json().get("id")
        except Exception as e:
            self._log_error("add_transaction", e)
            return None

    def delete_transaction(self, trans_id) -> bool:
        try:
            self._request("DELETE", f"/transactions/{trans_id}")
            return True
        except Exception as e:
            self._log_error("delete_transaction", e)
            return False

    def get_transactions(self, account_id, limit=100):
        try:
            resp = self._request("GET", f"/accounts/{account_id}/transactions", params={"limit": limit})
            return resp.json()
        except Exception as e:
            self._log_error("get_transactions", e)
            return []

    # --- Invoice Management ---
    def upsert_invoice(self, invoice_data: dict) -> Optional[int]:
        try:
            resp = self._request("POST", "/invoices", json={"invoice_data": invoice_data})
            return resp.json().get("id")
        except Exception as e:
            self._log_error("upsert_invoice", e)
            return None

    def delete_invoice_by_number(self, invoice_number: str):
        try:
            self._request("DELETE", f"/invoices/{invoice_number}")
            return True
        except Exception as e:
            self._log_error("delete_invoice_by_number", e)
            return False

    def generate_payment_plan(self, invoice_id: int, terms: int, start_date: datetime = None, interval_days: int = 30) -> bool:
        try:
            start_date_str = start_date.isoformat() if start_date else None
            self._request(
                "POST",
                "/invoices/plan",
                json={
                    "invoice_id": invoice_id,
                    "terms": terms,
                    "start_date": start_date_str,
                    "interval_days": interval_days,
                },
            )
            return True
        except Exception as e:
            self._log_error("generate_payment_plan", e)
            return False

    def get_invoices(self):
        try:
            resp = self._request("GET", "/invoices")
            return resp.json()
        except Exception as e:
            self._log_error("get_invoices", e)
            return []

    def get_installments(self, invoice_id):
        try:
            resp = self._request("GET", f"/invoices/{invoice_id}/installments")
            return resp.json()
        except Exception as e:
            self._log_error("get_installments", e)
            return []

    def get_all_installments_for_export(self, invoice_number: str) -> List[dict]:
        try:
            resp = self._request("GET", f"/invoices/export/{invoice_number}")
            return resp.json()
        except Exception as e:
            self._log_error("get_all_installments_for_export", e)
            return []

    def restore_installments_from_import(self, invoice_number: str, installments: List[dict]) -> bool:
        try:
            self._request("POST", f"/invoices/import/{invoice_number}", json=installments)
            return True
        except Exception as e:
            self._log_error("restore_installments_from_import", e)
            return False

    def update_installment_field(self, installment_id: int, field: str, value: any) -> bool:
        try:
            self._request(
                "PATCH",
                f"/installments/{installment_id}",
                json={"installment_id": installment_id, "field": field, "value": value},
            )
            return True
        except Exception as e:
            self._log_error("update_installment_field", e)
            return False

    def get_all_existing_invoice_numbers(self) -> set:
        try:
            resp = self._request("GET", "/invoices/existing_numbers")
            return set(resp.json().get("invoice_numbers", []))
        except Exception as e:
            self._log_error("get_all_existing_invoice_numbers", e)
            return set()

    def get_need_pdf_invoices(self):
        try:
            resp = self._request("GET", "/invoices/need_pdf")
            rows = resp.json()
            return [(r.get("id"), r.get("invoice_number")) for r in rows]
        except Exception as e:
            self._log_error("get_need_pdf_invoices", e)
            return []

    def update_invoice_number(self, invoice_id: int, new_number: str) -> bool:
        try:
            self._request("PATCH", f"/invoices/{invoice_id}/number", json={"new_number": new_number})
            return True
        except Exception as e:
            self._log_error("update_invoice_number", e)
            return False

    def refresh_invoice_status(self, invoice_id: int) -> bool:
        try:
            self._request("POST", f"/invoices/{invoice_id}/refresh_status")
            return True
        except Exception as e:
            self._log_error("refresh_invoice_status", e)
            return False

    # Compatibility for legacy callers that pass (cursor, invoice_id).
    def _refresh_invoice_status(self, _cursor, invoice_id: int):
        return self.refresh_invoice_status(invoice_id)

    # --- Advances ---
    def add_advance(self, customer_name, customer_cnpj, amount, date, description, account_id, transaction_id):
        try:
            resp = self._request(
                "POST",
                "/advances",
                json={
                    "customer_name": customer_name,
                    "customer_cnpj": customer_cnpj,
                    "amount": amount,
                    "date": date,
                    "description": description,
                    "account_id": account_id,
                    "transaction_id": transaction_id,
                },
            )
            return resp.json().get("id")
        except Exception as e:
            self._log_error("add_advance", e)
            return None

    def get_advances_by_customer(self, customer_cnpj=None, customer_name=None):
        try:
            params: Dict[str, Any] = {}
            if customer_cnpj:
                params["customer_cnpj"] = customer_cnpj
            if customer_name:
                params["customer_name"] = customer_name

            resp = self._request("GET", "/advances", params=params)
            return resp.json()
        except Exception as e:
            self._log_error("get_advances_by_customer", e)
            return []

    def apply_advance_to_installment(self, advance_id, installment_id, amount_to_apply):
        try:
            self._request(
                "POST",
                "/advances/apply",
                json={
                    "advance_id": advance_id,
                    "installment_id": installment_id,
                    "amount_to_apply": amount_to_apply,
                },
            )
            return True
        except Exception as e:
            self._log_error("apply_advance_to_installment", e)
            return False

    def find_invoice_id_by_number(self, number: str):
        try:
            resp = self._request("GET", "/invoices/find_id", params={"number": number})
            return resp.json().get("id")
        except Exception as e:
            self._log_error("find_invoice_id_by_number", e)
            return None

    def get_account_id_for_invoice(self, invoice_id: int):
        try:
            resp = self._request("GET", f"/transactions/account_for_invoice/{invoice_id}")
            return resp.json().get("account_id")
        except Exception as e:
            self._log_error("get_account_id_for_invoice", e)
            return None

    def get_all_installments_extended(self):
        try:
            resp = self._request("GET", "/reports/aging")
            return resp.json()
        except Exception as e:
            self._log_error("get_all_installments_extended", e)
            return []

    def search_pending_installments(self, patterns: List[str], term_number: int = None):
        try:
            payload = {"patterns": patterns}
            if term_number is not None:
                payload["term_number"] = term_number
            resp = self._request("POST", "/installments/search_pending", json=payload)
            return resp.json()
        except Exception as e:
            self._log_error("search_pending_installments", e)
            return []

    def register_payment(self, installment_id: int, amount: float, date: str, account_id: int, description: str):
        try:
            self._request(
                "POST",
                "/payments/register",
                json={
                    "installment_id": installment_id,
                    "amount": amount,
                    "date": date,
                    "account_id": account_id,
                    "description": description,
                },
            )
            return True
        except Exception as e:
            self._log_error("register_payment", e)
            return False
