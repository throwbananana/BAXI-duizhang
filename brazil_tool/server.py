# -*- coding: utf-8 -*-
import uvicorn
from fastapi import FastAPI, HTTPException, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional, Any, Dict
from datetime import datetime
import os

from brazil_tool.db.payment_manager import PaymentManager

app = FastAPI(title="Brazil Tool Payment Server")

# Initialize PaymentManager with the default DB path
# You might want to make this configurable via env var
DB_PATH = os.getenv("BRAZIL_TOOL_DB_PATH", "invoice_payment.db")
SERVER_TOKEN = os.getenv("BRAZIL_TOOL_SERVER_TOKEN", "").strip()
db = PaymentManager(DB_PATH)


@app.middleware("http")
async def auth_middleware(request, call_next):
    # When BRAZIL_TOOL_SERVER_TOKEN is set, require X-API-Key.
    if SERVER_TOKEN and request.headers.get("X-API-Key") != SERVER_TOKEN:
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    return await call_next(request)

# --- Pydantic Models for Request Bodies ---
class AccountCreate(BaseModel):
    name: str
    bank_info: str = ""
    currency: str = "BRL"
    initial_balance: float = 0.0
    note: str = ""

class AccountUpdate(BaseModel):
    account_id: int
    name: str
    bank_info: str
    currency: str
    note: str
    is_active: int = 1

class TransactionCreate(BaseModel):
    account_id: int
    date: str
    trans_type: str
    amount: float
    description: str
    related_invoice_id: Optional[int] = None
    related_installment_id: Optional[int] = None

class InvoiceUpsert(BaseModel):
    invoice_data: Dict[str, Any]

class PaymentPlanGenerate(BaseModel):
    invoice_id: int
    terms: int
    start_date: Optional[str] = None # ISO format preferred
    interval_days: int = 30

class InstallmentUpdate(BaseModel):
    installment_id: int
    field: str
    value: Any

class InvoiceNumberUpdate(BaseModel):
    new_number: str

class AdvanceCreate(BaseModel):
    customer_name: str
    customer_cnpj: str
    amount: float
    date: str
    description: str
    account_id: int
    transaction_id: int

class AdvanceApply(BaseModel):
    advance_id: int
    installment_id: int
    amount_to_apply: float

# --- Endpoints ---

@app.get("/")
def read_root():
    return {"status": "running", "service": "Brazil Tool Payment Server"}

# --- Accounts ---
@app.post("/accounts")
def add_account(account: AccountCreate):
    row_id = db.add_account(
        account.name, account.bank_info, account.currency, 
        account.initial_balance, account.note
    )
    if row_id is None:
        raise HTTPException(status_code=400, detail="Account creation failed (duplicate name?)")
    return {"id": row_id}

@app.put("/accounts/{account_id}")
def update_account(account_id: int, account: AccountUpdate):
    if account_id != account.account_id:
        raise HTTPException(status_code=400, detail="ID mismatch")
    ok = db.update_account(
        account.account_id, account.name, account.bank_info, 
        account.currency, account.note, account.is_active
    )
    if not ok:
        raise HTTPException(status_code=404, detail="Account not found or update failed")
    return {"status": "success"}

@app.delete("/accounts/{account_id}")
def delete_account(account_id: int):
    ok = db.delete_account(account_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Account not found or delete failed")
    return {"status": "success"}

@app.get("/accounts")
def get_accounts(active_only: bool = True):
    rows = db.get_accounts(active_only)
    # Convert sqlite3.Row to dict
    return [dict(row) for row in rows]

@app.get("/accounts/{account_id}/balance")
def get_account_balance(account_id: int):
    balance = db.get_account_balance(account_id)
    return {"balance": balance}

# --- Transactions ---
@app.post("/transactions")
def add_transaction(trans: TransactionCreate):
    trans_id = db.add_transaction(
        trans.account_id, trans.date, trans.trans_type, trans.amount,
        trans.description, trans.related_invoice_id, trans.related_installment_id
    )
    if trans_id is None:
        raise HTTPException(status_code=400, detail="Failed to add transaction")
    return {"id": trans_id}

@app.delete("/transactions/{trans_id}")
def delete_transaction(trans_id: int):
    ok = db.delete_transaction(trans_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Transaction not found or delete failed")
    return {"status": "success"}

@app.get("/accounts/{account_id}/transactions")
def get_transactions(account_id: int, limit: int = 100):
    rows = db.get_transactions(account_id, limit)
    return [dict(row) for row in rows]

# --- Invoices ---
@app.post("/invoices")
def upsert_invoice(data: InvoiceUpsert):
    inv_id = db.upsert_invoice(data.invoice_data)
    if inv_id is None:
        raise HTTPException(status_code=400, detail="Invalid invoice payload or database error")
    return {"id": inv_id}

@app.get("/invoices")
def get_invoices():
    rows = db.get_invoices()
    return [dict(row) for row in rows]

@app.delete("/invoices/{invoice_number}")
def delete_invoice(invoice_number: str):
    success = db.delete_invoice_by_number(invoice_number)
    if not success:
        raise HTTPException(status_code=404, detail="Invoice not found")
    return {"status": "success"}

# --- Installments ---
@app.get("/invoices/{invoice_id}/installments")
def get_installments(invoice_id: int):
    rows = db.get_installments(invoice_id)
    return [dict(row) for row in rows]

@app.get("/invoices/export/{invoice_number}")
def get_all_installments_for_export(invoice_number: str):
    rows = db.get_all_installments_for_export(invoice_number)
    return [dict(row) for row in rows]

@app.post("/invoices/import/{invoice_number}")
def restore_installments_from_import(invoice_number: str, installments: List[Dict[str, Any]]):
    ok = db.restore_installments_from_import(invoice_number, installments)
    if not ok:
        raise HTTPException(status_code=404, detail="Invoice not found or restore failed")
    return {"status": "success"}

@app.post("/invoices/plan")
def generate_payment_plan(plan: PaymentPlanGenerate):
    start_date_obj = None
    if plan.start_date:
        try:
            start_date_obj = datetime.fromisoformat(plan.start_date)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid start_date; use ISO format like 2026-02-26 or 2026-02-26T10:30:00",
            )
            
    ok = db.generate_payment_plan(plan.invoice_id, plan.terms, start_date_obj, plan.interval_days)
    if not ok:
        raise HTTPException(status_code=400, detail="Failed to generate payment plan (invoice not found or invalid data)")
    return {"status": "success"}

@app.patch("/installments/{installment_id}")
def update_installment_field(installment_id: int, update: InstallmentUpdate):
    if installment_id != update.installment_id:
        raise HTTPException(status_code=400, detail="ID mismatch")
    try:
        ok = db.update_installment_field(update.installment_id, update.field, update.value)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not ok:
        raise HTTPException(status_code=404, detail="Installment not found or update failed")
    return {"status": "success"}

@app.get("/invoices/existing_numbers")
def get_existing_invoice_numbers():
    numbers = sorted(db.get_all_existing_invoice_numbers())
    return {"invoice_numbers": numbers}

@app.get("/invoices/need_pdf")
def get_need_pdf_invoices():
    rows = db.get_need_pdf_invoices()
    return [{"id": r[0], "invoice_number": r[1]} for r in rows]

@app.patch("/invoices/{invoice_id}/number")
def update_invoice_number(invoice_id: int, payload: InvoiceNumberUpdate):
    ok = db.update_invoice_number(invoice_id, payload.new_number)
    if not ok:
        raise HTTPException(status_code=409, detail="Invoice number update failed (duplicate or invoice not found)")
    return {"status": "success"}

@app.post("/invoices/{invoice_id}/refresh_status")
def refresh_invoice_status(invoice_id: int):
    ok = db.refresh_invoice_status(invoice_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Invoice not found or failed to refresh status")
    return {"status": "success"}

# --- Advances ---
@app.post("/advances")
def add_advance(adv: AdvanceCreate):
    adv_id = db.add_advance(
        adv.customer_name, adv.customer_cnpj, adv.amount, 
        adv.date, adv.description, adv.account_id, adv.transaction_id
    )
    if adv_id is None:
        raise HTTPException(status_code=400, detail="Failed to add advance")
    return {"id": adv_id}

@app.get("/advances")
def get_advances(customer_cnpj: Optional[str] = None, customer_name: Optional[str] = None):
    rows = db.get_advances_by_customer(customer_cnpj, customer_name)
    return [dict(row) for row in rows]

@app.post("/advances/apply")
def apply_advance(data: AdvanceApply):
    success = db.apply_advance_to_installment(data.advance_id, data.installment_id, data.amount_to_apply)
    if not success:
        raise HTTPException(status_code=400, detail="Failed to apply advance")
    return {"status": "success"}

# --- Helpers for Legacy Refactoring ---
@app.get("/invoices/find_id")
def find_invoice_id(number: str):
    res = db.find_invoice_id_by_number(number)
    return {"id": res}

@app.get("/transactions/account_for_invoice/{invoice_id}")
def get_account_id_for_invoice(invoice_id: int):
    res = db.get_account_id_for_invoice(invoice_id)
    return {"account_id": res}

@app.get("/reports/aging")
def get_aging_data():
    rows = db.get_all_installments_extended()
    return [dict(row) for row in rows]

class SearchPattern(BaseModel):
    patterns: List[str]
    term_number: Optional[int] = None

@app.post("/installments/search_pending")
def search_pending_installments(data: SearchPattern):
    rows = db.search_pending_installments(data.patterns, data.term_number)
    return [dict(row) for row in rows]

class PaymentRegister(BaseModel):
    installment_id: int
    amount: float
    date: str
    account_id: int
    description: str

@app.post("/payments/register")
def register_payment(data: PaymentRegister):
    success = db.register_payment(data.installment_id, data.amount, data.date, data.account_id, data.description)
    if not success:
        raise HTTPException(status_code=400, detail="Failed to register payment")
    return {"status": "success"}

if __name__ == "__main__":
    # Secure default: local-only unless explicitly overridden.
    host = os.getenv("BRAZIL_TOOL_SERVER_HOST", "127.0.0.1")
    port = int(os.getenv("BRAZIL_TOOL_SERVER_PORT", "8000"))
    uvicorn.run(app, host=host, port=port)
