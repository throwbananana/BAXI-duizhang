import os
import fitz
import re
from datetime import datetime, timedelta
from brazil_tool.core.report_parser import CollectionReportParser
from brazil_tool.core.statement_parser import BankStatementParser
from brazil_tool.core.utils import calculate_similarity

def safe_date(d_str):
    if not d_str: return None
    for fmt in ["%d/%m/%Y", "%Y/%m/%d"]:
        try: return datetime.strptime(d_str, fmt)
        except: pass
    return None

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

    reports = [r for r in all_reports if r['amount'] is not None]
    # Filter statements to income only
    statements = [s for s in all_statements if s['amount'] > 0]

    print(f"Reports: {len(reports)}, Income Statements: {len(statements)}")

    # Grouping Logic:
    # Most income statements seem to be aggregates ('BOLETOS RECEBIDOS')
    # These usually happen on the same day or day after the pay_date in reports.
    
    # Try sliding window matching
    matches_count = 0
    used_reports = set()
    used_stmts = set()

    # Sort statements by date
    statements.sort(key=lambda x: safe_date(x['date']) or datetime.min)

    print("\n--- AGGREGATE MATCHING ATTEMPT ---")
    for s_idx, st in enumerate(statements):
        st_date = safe_date(st['date'])
        if not st_date: continue
        
        # Look for reports with pay_date within [st_date - 2, st_date]
        potential_reps = []
        for r_idx, rep in enumerate(reports):
            if r_idx in used_reports: continue
            rep_date = safe_date(rep['pay_date'] or rep['due_date'])
            if rep_date and (st_date - rep_date).days >= 0 and (st_date - rep_date).days <= 2:
                potential_reps.append((r_idx, rep))
        
        if not potential_reps: continue
        
        # Try finding a combination that sums up to st['amount']
        # For simplicity, first check if any single report matches
        matched_any = False
        for r_idx, rep in potential_reps:
            if abs(rep['amount'] - st['amount']) < 0.05:
                # print(f"1-to-1: Stmt {st['date']} {st['amount']} == Rep {rep['name']} {rep['amount']}")
                used_reports.add(r_idx)
                used_stmts.add(s_idx)
                matches_count += 1
                matched_any = True
                break
        
        if matched_any: continue

        # Try N-to-1 if not a single match
        from itertools import combinations
        # Limit to 6 reports max for performance
        for size in range(2, min(len(potential_reps), 7)):
            found_combo = False
            for combo in combinations(potential_reps, size):
                sum_amt = sum(c[1]['amount'] for c in combo)
                if abs(sum_amt - st['amount']) < 0.05:
                    print(f"N-to-1: Stmt {st['date']} {st['amount']} ({st['desc']}) == {size} reports total {sum_amt:.2f}")
                    for r_idx, _ in combo: used_reports.add(r_idx)
                    used_stmts.add(s_idx)
                    matches_count += size
                    found_combo = True
                    break
            if found_combo: break

    print(f"\nFinal Matches: {matches_count} / {len(reports)}")

analyze()