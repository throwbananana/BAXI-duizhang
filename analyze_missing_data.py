import sys
import os
import pandas as pd
import importlib.util
from datetime import datetime, timedelta
import collections

# 1. Import the actual module
module_name = "brazil_product_code_v1.02"
file_path = os.path.abspath(f"{module_name}.py")
spec = importlib.util.spec_from_file_location(module_name, file_path)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
ReconciliationWorker = mod.ReconciliationWorker
safe_parse_date_to_date = mod.safe_parse_date_to_date

# Mock Mapping Manager
class MockMappingMgr:
    def get_partner_std(self, key):
        return str(key).strip().upper()

# 2. Load Data
excel_path = "reconciliation_results_20260120_090641.xlsx"
print(f"Loading data from {excel_path}...")

df_reports = pd.read_excel(excel_path, sheet_name='核对差异')
reports = []
for _, row in df_reports.iterrows():
    if pd.isna(row['往来单位']) or pd.isna(row['报告金额']): continue
    try:
        amt = float(str(row['报告金额']).replace(',', '').replace('R$', '').strip())
    except: continue
    
    reports.append({
        "name": str(row['往来单位']),
        "invoice_ref": str(row['关联发票/参考号']) if not pd.isna(row['关联发票/参考号']) else "",
        "amount": amt,
        "due_date": str(row['到期日']) if not pd.isna(row['到期日']) else "",
        "pay_date": str(row['支付日']) if not pd.isna(row['支付日']) else "",
        "_source_file": str(row['来源文件']) if not pd.isna(row['来源文件']) else "",
        "_std_partner": str(row['往来单位'])
    })

df_stmts = pd.read_excel(excel_path, sheet_name='流水详情')
statements = []
for _, row in df_stmts.iterrows():
    if pd.isna(row['流水描述/备注']) or pd.isna(row['金额']): continue
    try:
        amt = float(str(row['金额']).replace(',', '').replace('R$', '').strip())
    except: continue
    statements.append({
        "date": str(row['交易日期']),
        "desc": str(row['流水描述/备注']),
        "amount": amt,
        "cnpj": str(row['对方CNPJ']) if not pd.isna(row['对方CNPJ']) else "",
        "_source_file": str(row['来源文件']) if not pd.isna(row['来源文件']) else ""
    })

# 3. Run Logic
print("Running full reconciliation (Phases 0-4)...")
worker = ReconciliationWorker(reports, statements, MockMappingMgr(), tolerance=0.05)
worker.progress = type('obj', (object,), {'emit': lambda x: None})
results = worker.do_reconciliation()

unmatched_reports = [r['report'] for r in results if r['type'] == 'NONE']
print(f"\nRemaining Unmatched Reports: {len(unmatched_reports)}")

# 4. Analyze Missing Data
print("\n--- 缺失数据分析 (Missing Data Analysis) ---")

# A. Date Coverage Analysis
stmt_dates = set()
for s in statements:
    d = safe_parse_date_to_date(s['date'])
    if d:
        stmt_dates.add(d)

if not stmt_dates:
    print("CRITICAL: No valid dates found in statements!")
    sys.exit()

min_stmt_date = min(stmt_dates)
max_stmt_date = max(stmt_dates)
print(f"Statement Date Range: {min_stmt_date} to {max_stmt_date}")

# Check unmatched reports against this range
out_of_range_count = 0
in_range_missing_count = 0
date_gap_count = 0 # In range, but no statements on that specific day +/- 2 days

gap_details = collections.defaultdict(list)

for rep in unmatched_reports:
    rep_date = safe_parse_date_to_date(rep['pay_date']) or safe_parse_date_to_date(rep['due_date'])
    if not rep_date:
        continue
    
    if rep_date < min_stmt_date or rep_date > max_stmt_date:
        out_of_range_count += 1
    else:
        # Check if we have statements around this date
        has_nearby_stmt = False
        for offset in range(-5, 6): # Check +/- 5 days
            if (rep_date + timedelta(days=offset)) in stmt_dates:
                has_nearby_stmt = True
                break
        
        if not has_nearby_stmt:
            date_gap_count += 1
            gap_details[rep_date.strftime("%Y-%m")].append(rep)
        else:
            in_range_missing_count += 1

print(f"\n[Reason 1] Report Date Out of Statement Range: {out_of_range_count}")
print(f"  -> 这些发票的日期超出了当前导入流水的覆盖范围 (需导入更多月份流水)")

print(f"\n[Reason 2] Data Gap (Date in range, but no statements found near date): {date_gap_count}")
print(f"  -> 这是一个强信号：该日期附近没有任何流水记录 (可能是缺失了特定日期的文件)")
if gap_details:
    print("  -> Gaps by Month:")
    for month, items in sorted(gap_details.items())[:5]:
        print(f"     {month}: {len(items)} items missing (e.g. {items[0]['name']} {items[0]['amount']})")

print(f"\n[Reason 3] Unmatched but Statements Exist Nearby: {in_range_missing_count}")
print(f"  -> 流水存在，但无法匹配。可能原因：金额差异过大、非银行渠道收款(现金/支票)、或者是这一笔钱进了另一个银行账户。")

# B. Source Context Analysis
# Check if unmatched reports come from a specific source file that has NO corresponding statements
unmatched_by_source = collections.defaultdict(list)
for rep in unmatched_reports:
    src = rep.get('_source_file', 'Unknown')
    unmatched_by_source[src].append(rep)

print("\n--- Unmatched by Source File (Top 5) ---")
sorted_sources = sorted(unmatched_by_source.items(), key=lambda x: len(x[1]), reverse=True)
for src, items in sorted_sources[:10]:
    # Check if we have ANY statements that "look like" they might match this source
    # e.g. source "baixados_argotech.pdf" -> check statements for "ARGOTECH" keyword
    context_keyword = ""
    if "ARGOTECH" in src.upper(): context_keyword = "ARGOTECH"
    elif "MARTELUX" in src.upper(): context_keyword = "MARTELUX"
    elif "NORTETOOLS" in src.upper() or "NORTE" in src.upper(): context_keyword = "NORTE"
    elif "DEYUN" in src.upper(): context_keyword = "DEYUN"
    
    related_stmt_count = 0
    if context_keyword:
        related_stmt_count = sum(1 for s in statements if context_keyword in s['desc'].upper() or context_keyword in str(s.get('_source_file', '')).upper())
    
    print(f"Source: {src} | Unmatched: {len(items)}")
    print(f"  -> Context '{context_keyword}': Found {related_stmt_count} related statements in DB.")
    if related_stmt_count == 0 and context_keyword:
        print(f"  🚨 ALERT: Zero statements found for context '{context_keyword}'! You are likely missing the Bank Extrato for {context_keyword}.")

# C. Account Mismatch Detection
# Check if we have reports for Company A but only statements for Company B
report_contexts = set()
for r in reports:
    if "ARGOTECH" in r.get('_source_file', '').upper(): report_contexts.add("ARGOTECH")
    if "MARTELUX" in r.get('_source_file', '').upper(): report_contexts.add("MARTELUX")
    if "NORTETOOLS" in r.get('_source_file', '').upper(): report_contexts.add("NORTETOOLS")

stmt_contexts = set()
for s in statements:
    if "ARGOTECH" in s['desc'].upper() or "ARGOTECH" in str(s.get('_source_file', '')).upper(): stmt_contexts.add("ARGOTECH")
    if "MARTELUX" in s['desc'].upper() or "MARTELUX" in str(s.get('_source_file', '')).upper(): stmt_contexts.add("MARTELUX")
    if "NORTE" in s['desc'].upper() or "NORTE" in str(s.get('_source_file', '')).upper(): stmt_contexts.add("NORTETOOLS")

print("\n--- Account Coverage Check ---")
print(f"Detected Report Contexts: {report_contexts}")
print(f"Detected Statement Contexts: {stmt_contexts}")
missing_contexts = report_contexts - stmt_contexts
if missing_contexts:
    print(f"🚨 CRITICAL: Missing Statements for accounts: {missing_contexts}")
    print("   Please upload the Extrato PDF for these accounts.")
else:
    print("✅ Statement coverage seems aligned with Report contexts.")
