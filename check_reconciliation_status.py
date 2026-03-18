import os
import sqlite3
import re
from datetime import datetime
from brazil_tool.core.pdf import extract_text_from_pdf
from brazil_tool.core.report_parser import CollectionReportParser
from brazil_tool.core.statement_parser import BankStatementParser

def check_reconciliation():
    build_dir = 'build'
    if not os.path.exists(build_dir):
        print(f"Directory {build_dir} not found.")
        return

    report_files = [f for f in os.listdir(build_dir) if 'BAIXADOS' in f.upper() and f.endswith('.pdf')]
    statement_files = [f for f in os.listdir(build_dir) if 'EXTRATO' in f.upper() and f.endswith('.pdf')]

    print(f"Found {len(report_files)} report files and {len(statement_files)} statement files.")

    all_reports = []
    for f in report_files:
        path = os.path.join(build_dir, f)
        text, _ = extract_text_from_pdf(path)
        records = CollectionReportParser.parse_report(text)
        for r in records:
            r['source'] = f
        all_reports.extend(records)

    all_statements = []
    for f in statement_files:
        path = os.path.join(build_dir, f)
        text, _ = extract_text_from_pdf(path)
        txs = BankStatementParser.parse_statement(text)
        for t in txs:
            t['source'] = f
        all_statements.extend(txs)

    print(f"\nTotal Report Records: {len(all_reports)}")
    print(f"Total Statement Records (Income): {len([s for s in all_statements if s['amount'] > 0])}")

    # Simplified Matching Logic
    matched_count = 0
    mismatched_reports = []
    
    # Try to match reports to statements by amount
    for rep in all_reports:
        rep_amt = rep['amount'] or 0.0
        # Look for matching amount in statements
        match = None
        for st in all_statements:
            if abs(st['amount'] - rep_amt) < 0.05:
                match = st
                break
        
        if match:
            matched_count += 1
        else:
            mismatched_reports.append(rep)

    print(f"\nMatched: {matched_count}")
    print(f"Unmatched Reports: {len(mismatched_reports)}")
    
    if mismatched_reports:
        print("\n--- Samples of Unmatched Reports ---")
        for r in mismatched_reports[:5]:
            print(f"Source: {r['source']} | Name: {r['name']} | Amount: {r['amount']} | Date: {r['pay_date']}")

    # Check if anything is already in DB
    conn = sqlite3.connect('invoice_payment.db')
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM account_transactions WHERE type='Income'")
    db_count = cursor.fetchone()[0]
    conn.close()
    
    print(f"\nTransactions already in Database: {db_count}")

if __name__ == "__main__":
    check_reconciliation()
