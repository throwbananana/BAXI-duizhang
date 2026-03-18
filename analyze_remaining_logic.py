
import os
import fitz
import re
from datetime import datetime, timedelta
from brazil_tool.core.report_parser import CollectionReportParser
from brazil_tool.core.statement_parser import BankStatementParser
from brazil_tool.core.utils import calculate_similarity, br_to_float

def safe_date(d_str):
    if not d_str: return None
    for fmt in ["%d/%m/%Y", "%Y/%m/%d", "%Y-%m-%d"]:
        try: return datetime.strptime(d_part, fmt) if 'd_part' in locals() else datetime.strptime(d_str, fmt)
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
        doc = fitz.open(f)
        text = ""
        for page in doc: text += page.get_text()
        doc.close()
        if "BAIXADOS" in f.upper() or "VENCIDOS" in f.upper():
            all_reports.extend(CollectionReportParser.parse_report(text))
        else:
            all_statements.extend(BankStatementParser.parse_statement(text))

    unmatched_amounts = [104260.0, 32000.0, 21320.0, 30339.06, 11844.5, 13138.91, 11350.94]
    
    print("--- TARGET SEARCH: UNMATCHED LARGE AMOUNTS ---")
    for target in unmatched_amounts:
        print(f"\nSearching for {target:,.2f}:")
        # Search direct
        found_direct = False
        for s in all_statements:
            if abs(s['amount'] - target) < 100: # Broad tolerance
                print(f"  [NEAR MATCH] Stmt {s['date']} {s['amount']} - {s['desc']}")
                found_direct = True
        
        # Search Combinations (Simple 2-3 items)
        from itertools import combinations
        incomes = [s for s in all_statements if s['amount'] > 0]
        for size in range(2, 4):
            for combo in combinations(incomes, size):
                if abs(sum(x['amount'] for x in combo) - target) < 10:
                    print(f"  [COMBO MATCH] {size} stmts total {sum(x['amount'] for x in combo):.2f} == Target")
                    for x in combo: print(f"    - {x['date']} {x['amount']} {x['desc']}")

    print("\n--- KEYWORD SEARCH: NORTETOOLS / NORTE ---")
    for s in all_statements:
        if "NORTE" in s['desc'].upper() or "TOOL" in s['desc'].upper():
            print(f"  Stmt {s['date']} {s['amount']} - {s['desc']}")

    print("\n--- KEYWORD SEARCH: PALACIO ---")
    for s in all_statements:
        if "PALACIO" in s['desc'].upper():
            print(f"  Stmt {s['date']} {s['amount']} - {s['desc']}")

analyze()
