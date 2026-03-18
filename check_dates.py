
import os
from brazil_tool.core.pdf import extract_text_from_pdf
from brazil_tool.core.statement_parser import BankStatementParser

def main():
    build_dir = 'build'
    statement_files = [f for f in os.listdir(build_dir) if 'EXTRATO' in f.upper() and f.endswith('.pdf')]
    all_txs = []
    
    for f in statement_files:
        text, _ = extract_text_from_pdf(os.path.join(build_dir, f))
        all_txs.extend(BankStatementParser.parse_statement(text))
            
    print("--- Transactions around 2025-12-08 ---")
    target_dates = ['07/12/2025', '08/12/2025', '09/12/2025']
    for t in all_txs:
        if t['date'] in target_dates:
            print(f"{t['date']} | {t['amount']:>10.2f} | {t['desc']}")

if __name__ == "__main__":
    main()
