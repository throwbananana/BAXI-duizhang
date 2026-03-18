import argparse
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd


NOISE_KEYWORDS = [
    "SDO CTA/APL",
    "REND PAGO APLIC",
    "RES APLIC AUT",
    "SDO CTA ANT",
    "SALDO TOTAL",
    "DISPONIVEL",
    "SALDO ANTERIOR",
    "SDO CTA/APL AUTOM",
    "SALDO FINAL",
    "SALDO INICIAL",
]


def safe_parse_date(date_str):
    if not date_str or date_str == "-":
        return None
    try:
        if isinstance(date_str, datetime):
            return date_str.date()
        for fmt in ("%Y/%m/%d", "%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(str(date_str).split(" ")[0], fmt).date()
            except ValueError:
                continue
    except Exception:
        return None
    return None


def normalize_text(text):
    if not text:
        return ""
    return unicodedata.normalize("NFKD", str(text)).encode("ascii", "ignore").decode("ascii").upper()


def is_noise(desc):
    norm = normalize_text(desc)
    return any(k in norm for k in NOISE_KEYWORDS)


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
    parser = argparse.ArgumentParser(description="Deep analysis for remaining unmatched records.")
    parser.add_argument("--input", default="", help="Path to reconciliation Excel file.")
    args = parser.parse_args()

    try:
        file_path = resolve_result_file(args.input)
    except FileNotFoundError as e:
        print(e)
        return 1

    try:
        df_diff = pd.read_excel(file_path, sheet_name="核对差异")
        df_stmts = pd.read_excel(file_path, sheet_name="流水详情")
    except ValueError as e:
        available = pd.ExcelFile(file_path).sheet_names
        print(f"Sheet error: {e}")
        print(f"Available sheets: {available}")
        return 1

    df_stmts["is_noise"] = df_stmts["流水描述/备注"].apply(is_noise)
    df_stmts["amt_num"] = df_stmts["金额"].apply(
        lambda x: float(str(x).replace(",", "").split(" ")[0]) if pd.notnull(x) else 0
    )

    valid_stmts = df_stmts[(df_stmts["amt_num"] > 0) & (~df_stmts["is_noise"])].copy()
    unmatched_stmts = valid_stmts[valid_stmts["状态"] == "❌ 未匹配"].copy()

    df_diff["amt_num"] = df_diff["报告金额"].apply(
        lambda x: float(str(x).replace(",", "")) if pd.notnull(x) else 0
    )
    missing_reports = df_diff[df_diff["对账状态"] == "❌ 缺失流水"].copy()

    print(f"Input file: {file_path}")
    print("--- 深度分析摘要 ---")
    print(f"真实未匹配收入流水: {len(unmatched_stmts)} 条")
    print(f"高额缺失报告项 (>1000): {len(missing_reports[missing_reports['amt_num'] > 1000])} 条")

    print("\n--- 未匹配流水 TOP 10 (按金额) ---")
    print(unmatched_stmts.sort_values("amt_num", ascending=False)[["交易日期", "流水描述/备注", "amt_num"]].head(10))

    print("\n--- 潜在关联发现：金额一致但日期跨度过大 ---")
    found_potential = []
    for _, st in unmatched_stmts.iterrows():
        s_date = safe_parse_date(st["交易日期"])
        s_amt = st["amt_num"]
        match_reps = missing_reports[abs(missing_reports["amt_num"] - s_amt) < 0.01]
        for _, rep in match_reps.iterrows():
            r_date = safe_parse_date(rep["支付日"]) or safe_parse_date(rep["到期日"])
            if r_date and s_date:
                days_diff = abs((s_date - r_date).days)
                if days_diff > 65:
                    found_potential.append(
                        {
                            "单位": rep["往来单位"],
                            "金额": s_amt,
                            "流水日期": s_date,
                            "报告日期": r_date,
                            "相差天数": days_diff,
                            "流水备注": st["流水描述/备注"],
                        }
                    )

    if found_potential:
        print(pd.DataFrame(found_potential).head(10))
    else:
        print("未发现金额一致但日期超限的项。")

    missing_reports["date_parsed"] = missing_reports["支付日"].apply(safe_parse_date)
    missing_reports["year_month"] = missing_reports["date_parsed"].apply(
        lambda x: x.strftime("%Y-%m") if x else "Unknown"
    )
    print("\n--- 缺失流水的月份分布 ---")
    print(missing_reports["year_month"].value_counts().sort_index())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
