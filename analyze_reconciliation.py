import pandas as pd
import argparse
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

def resolve_result_file(explicit: str = "", required_sheets=("核对差异", "流水详情")) -> Path:
    if explicit:
        p = Path(explicit)
        if p.exists():
            return p
        raise FileNotFoundError(f"Input file not found: {p}")

    default_name = Path("reconciliation_results_20260117_101701.xlsx")
    if default_name.exists():
        return default_name

    candidates = []
    candidates.extend(Path(".").glob("reconciliation_results*.xlsx"))
    results_dir = Path("reconciliation_results")
    if results_dir.exists():
        candidates.extend(results_dir.glob("*.xlsx"))
    candidates = [p for p in candidates if p.is_file()]
    if not candidates:
        raise FileNotFoundError("No reconciliation_results*.xlsx file found.")

    sorted_candidates = sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)
    for p in sorted_candidates:
        try:
            sheets = set(pd.ExcelFile(p).sheet_names)
        except Exception:
            continue
        if all(s in sheets for s in required_sheets):
            return p

    return sorted_candidates[0]


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze reconciliation snapshot.")
    parser.add_argument("--input", default="", help="Path to reconciliation Excel file.")
    args = parser.parse_args()

    try:
        file_path = resolve_result_file(args.input)
    except FileNotFoundError as e:
        print(e)
        return 1

    try:
        df_diff = pd.read_excel(file_path, sheet_name='核对差异')
        df_stmts = pd.read_excel(file_path, sheet_name='流水详情')
    except ValueError as e:
        available = pd.ExcelFile(file_path).sheet_names
        print(f"Sheet error: {e}")
        print(f"Available sheets: {available}")
        return 1

    print(f"Input file: {file_path}")
    print("--- Analysis of Reconciliation Differences ---")
    status_counts = df_diff['对账状态'].value_counts()
    print(status_counts)

    print("\n--- Summary of Missing Statements (❌ 缺失流水) ---")
    missing = df_diff[df_diff['对账状态'] == '❌ 缺失流水'].copy()
    if not missing.empty:
        missing['到期年份'] = pd.to_datetime(missing['到期日'], errors='coerce').dt.year
        missing['支付年份'] = pd.to_datetime(missing['支付日'], errors='coerce').dt.year
        print("Missing by Due Year:")
        print(missing['到期年份'].value_counts().sort_index())
        print("\nMissing by Payment Year:")
        print(missing['支付年份'].value_counts().sort_index())

    print("\n--- Analysis of Bank Statement Transactions ---")
    def clean_amt(val):
        if isinstance(val, str):
            val = val.replace(',', '').replace(' (组合)', '').replace(' (分拆)', '')
        try:
            return float(val)
        except (TypeError, ValueError):
            return 0.0

    df_stmts['amount_num'] = df_stmts['金额'].apply(clean_amt)
    income_stmts = df_stmts[df_stmts['amount_num'] > 0].copy()
    expense_stmts = df_stmts[df_stmts['amount_num'] < 0].copy()

    print(f"Total Transactions: {len(df_stmts)}")
    print(f"Income Transactions (Amt > 0): {len(income_stmts)}")
    print(f"Expense Transactions (Amt < 0): {len(expense_stmts)}")

    if not income_stmts.empty:
        income_stmts['YearMonth'] = pd.to_datetime(income_stmts['交易日期']).dt.to_period('M')
        print("\nIncome Transactions by Month:")
        print(income_stmts.groupby('YearMonth').size())

    print("\n--- Unmatched Income Transactions ---")
    unmatched_income = income_stmts[income_stmts['状态'] == '❌ 未匹配']
    print(f"Total Unmatched Income: {len(unmatched_income)}")
    if not unmatched_income.empty:
        print("Top unmatched income descriptions:")
        print(unmatched_income['流水描述/备注'].value_counts().head(10))
        print("\nTop unmatched income amounts:")
        print(unmatched_income['amount_num'].value_counts().head(10))

    print("\n--- Examples of Unmatched Large Income ---")
    large_unmatched = unmatched_income[unmatched_income['amount_num'] > 1000].sort_values(by='amount_num', ascending=False)
    print(large_unmatched[['交易日期', '流水描述/备注', 'amount_num', '来源文件']].head(10))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
