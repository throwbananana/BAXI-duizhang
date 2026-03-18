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
    reports_without_amount = [r for r in all_reports if r['amount'] is None]
    
    print(f"Total Reports: {len(all_reports)}")
    print(f"Reports with Amount: {len(reports_with_amount)}")
    print(f"Reports WITHOUT Amount: {len(reports_without_amount)}")
    
    # Check if there are many matches that were missed
    matches_1to1 = 0
    matches_with_fuzzy = 0
    
    for r in reports_with_amount:
        rep_amt = r['amount']
        found = False
        for s in all_statements:
            if abs(rep_amt - s['amount']) < 0.05:
                matches_1to1 += 1
                found = True
                
                # Check fuzzy name match
                sim = calculate_similarity(r['name'], s['desc'])
                if sim > 0.5:
                    matches_with_fuzzy += 1
                break

    print(f"1-to-1 Amount Matches: {matches_1to1}")
    print(f"Amount Matches with Fuzzy Name (>0.5): {matches_with_fuzzy}")

    # Sample of reports without amount to see why
    if reports_without_amount:
        print("\n--- SAMPLE REPORTS WITHOUT AMOUNT ---")
        for r in reports_without_amount[:10]:
            print(r)

analyze()
