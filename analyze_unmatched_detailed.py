import os
import fitz
import re
from brazil_tool.core.report_parser import CollectionReportParser
from brazil_tool.core.statement_parser import BankStatementParser
from brazil_tool.core.utils import calculate_similarity

def analyze():
    files = [
        "build/BAIXADOS E LIQUIDADOS-NORTETOOLS.pdf",
        "build/Extrato_7657_216003_05-01-2026GEOLOC.pdf",
        "build/Extrato_0057_413320_05-01-2026COMPBRASIL.pdf",
        "build/Extrato_0174_433110_05-01-2026ZHEMAX.pdf",
        "build/Extrato_0174_881235_05-01-2026DEYUN.pdf",
        "build/Extrato_0174_888321_05-01-2026CAIYA.pdf",
        "build/Extrato_7657_998857_05-01-2026NORTE.pdf",
        "build/Extrato_7657_998899_05-01-2026ARGOTECH.pdf",
        "build/Extrato_0174_998195_05-01-2026MARTELUX.pdf",
        "build/extrato-itau_05_01_2026_09-40ca.pdf",
        "build/extrato-itau_05_01_2026_09-46de.pdf",
        "build/extrato-itau_05_01_2026_09-53wang.pdf",
        "build/VENCIDOS-CAIYA.pdf",
        "build/VENCIDOS-DEYUN.pdf",
        "build/VENCIDOS-NORTETOOLS.pdf",
        "build/BAIXADOS E LIQUIDADOS-ARGOTECH.pdf",
        "build/BAIXADOS E LIQUIDADOS-CAIYA.pdf",
        "build/BAIXADOS E LIQUIDADOS-MARTELUX.pdf",
        "build/VENCIDOS-ARGOTECH.pdf",
        "build/VENCIDOS-MARTELUX.pdf",
        "build/BAIXADOS E LIQUIDADOS -DEYUN.pdf"
    ]

    all_reports = []
    all_statements = []

    for f in files:
        if not os.path.exists(f): continue
        try:
            doc = fitz.open(f)
            text = ""
            for page in doc: text += page.get_text()
            doc.close()
            if "BAIXADOS" in f.upper() or "VENCIDOS" in f.upper():
                all_reports.extend(CollectionReportParser.parse_report(text))
            else:
                all_statements.extend(BankStatementParser.parse_statement(text))
        except Exception: pass

    # Filter out reports with None amount
    reports_with_amount = [r for r in all_reports if r['amount'] is not None]
    
    print(f"Reports with amount: {len(reports_with_amount)}")
    print(f"Total statements: {len(all_statements)}")
    
    # 1. Inspect why fuzzy match is low
    print("\n--- SAMPLE AMOUNT MATCHES (NO NAME MATCH) ---")
    count = 0
    for r in reports_with_amount:
        rep_amt = r['amount']
        for s in all_statements:
            if abs(rep_amt - s['amount']) < 0.05:
                sim = calculate_similarity(r['name'], s['desc'])
                if sim < 0.5:
                    count += 1
                    if count <= 10:
                        print(f"Amt {rep_amt}: Report Name '{r['name']}' vs Stmt Desc '{s['desc']}' (Sim: {sim:.2f})")
                break

    # 2. Check for grouping (N reports to 1 statement)
    print("\n--- POTENTIAL N-TO-1 GROUP MATCHES ---")
    # Group reports by date
    from datetime import datetime
    def safe_date(d_str):
        if not d_str: return None
        for fmt in ["%d/%m/%Y", "%Y/%m/%d"]:
            try: return datetime.strptime(d_str, fmt)
            except: pass
        return None

    reports_by_date = {}
    for r in reports_with_amount:
        dt = safe_date(r['pay_date'] or r['due_date'])
        if dt:
            if dt not in reports_by_date: reports_by_date[dt] = []
            reports_by_date[dt].append(r)

    matches_n1 = 0
    for dt, reps in reports_by_date.items():
        if len(reps) < 2: continue
        total_rep_amt = sum(rx['amount'] for rx in reps)
        for s in all_statements:
            dt_s = safe_date(s['date'])
            if dt_s and abs((dt - dt_s).days) <= 2:
                if abs(total_rep_amt - s['amount']) < 0.05:
                    print(f"MATCH N-to-1: Date {dt.date()}, {len(reps)} reports total {total_rep_amt:.2f} == Stmt {s['amount']:.2f} ('{s['desc']}')")
                    matches_n1 += 1
                    break
    
    print(f"Total N-to-1 Group Matches: {matches_n1}")

analyze()