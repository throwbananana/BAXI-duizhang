
import os
from datetime import datetime
from brazil_tool.core.pdf import extract_text_from_pdf
from brazil_tool.core.report_parser import CollectionReportParser
from brazil_tool.core.statement_parser import BankStatementParser

def main():
    build_dir = 'build'
    report_files = [f for f in os.listdir(build_dir) if 'BAIXADOS' in f.upper() and f.endswith('.pdf')]
    statement_files = [f for f in os.listdir(build_dir) if 'EXTRATO' in f.upper() and f.endswith('.pdf')]

    all_reports = []
    for f in report_files:
        text, _ = extract_text_from_pdf(os.path.join(build_dir, f))
        all_reports.extend(CollectionReportParser.parse_report(text))

    all_statements = []
    for f in statement_files:
        text, _ = extract_text_from_pdf(os.path.join(build_dir, f))
        all_statements.extend(BankStatementParser.parse_statement(text))

    statements_by_date = {}
    for st in all_statements:
        if st['amount'] <= 0: continue
        d = st['date']
        if d not in statements_by_date: statements_by_date[d] = []
        statements_by_date[d].append(st)

    print("--- HIGH VALUE UNMATCHED REPORTS (> 5000 BRL) ---")
    print(f"{'Date':<12} | {'Amount':<12} | {'Name':<25} | {'Ref'}")
    print("-" * 80)

    for rep in all_reports:
        amt = rep['amount'] or 0.0
        if amt < 5000: continue
        
        date = rep['pay_date']
        matched = False
        
        # Check single match
        if date in statements_by_date:
            for st in statements_by_date[date]:
                if abs(st['amount'] - amt) < 0.05:
                    matched = True
                    break
        
        # Check batch match (approximate)
        if not matched and date in statements_by_date:
            for st in statements_by_date[date]:
                if st['amount'] >= amt and ("BOLETOS" in st['desc'].upper() or "RECEBIMENTOS" in st['desc'].upper()):
                    matched = True # Assume it's part of a batch
                    break
        
        if not matched:
            print(f"{date:<12} | {amt:>12.2f} | {rep['name'][:25]:<25} | {rep['invoice_ref']}")

if __name__ == "__main__":
    main()
