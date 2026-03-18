
import pandas as pd
import sys
import os
import argparse
import re
import unicodedata
from datetime import datetime
from pathlib import Path

# 模拟环境以导入主程序中的类
class MockMappingMgr:
    def get_partner_std(self, name_or_cnpj):
        return None # 简化测试，使用原始名称

# 导入主程序中的关键逻辑 (通过读取文件内容并执行，因为导入可能涉及 GUI 依赖)
with open('brazil_product_code_v1.02.py', 'r', encoding='utf-8') as f:
    code = f.read()

# 提取 ReconciliationWorker 及其依赖
# 注意：为了运行，我们需要剥离一些 GUI 依赖或模拟它们
# 这里我们直接定义一个精简版的 Worker 来测试算法核心

def safe_parse_date_to_date(date_str):
    if not date_str or date_str == '-': return None
    try:
        if isinstance(date_str, datetime): return date_str.date()
        # 尝试多种格式
        for fmt in ["%Y/%m/%d", "%d/%m/%Y", "%Y-%m-%d"]:
            try: return datetime.strptime(str(date_str).split(' ')[0], fmt).date()
            except: continue
    except: pass
    return None

def calculate_similarity(s1, s2):
    import difflib
    if not s1 or not s2: return 0.0
    s1, s2 = str(s1).strip().lower(), str(s2).strip().lower()
    return difflib.SequenceMatcher(None, s1, s2).ratio()

# 复制更新后的核心逻辑进行测试
def test_algorithm(report_records, statement_records):
    used_report_indices = set()
    used_statement_indices = set()
    recon_results = []
    tolerance = 0.05
    
    # 1. 噪音过滤测试
    noise_keywords = [
        "SDO CTA/APL", "REND PAGO APLIC", "RES APLIC AUT", "SDO CTA ANT",
        "SALDO TOTAL", "DISPONIVEL", "SALDO ANTERIOR", "SDO CTA/APL AUTOM",
        "SALDO FINAL", "SALDO INICIAL"
    ]
    
    valid_stmt_indices = []
    print(f"--- 原始流水数: {len(statement_records)} ---")
    for i, s in enumerate(statement_records):
        amt = s.get('amount', 0)
        raw_desc = s.get('desc', '').upper()
        norm_desc = unicodedata.normalize('NFKD', raw_desc).encode('ascii', 'ignore').decode('ascii').upper()
        
        if amt > 0:
            if any(k in norm_desc for k in noise_keywords): continue
            valid_stmt_indices.append(i)
    
    print(f"--- 过滤后有效收入流水: {len(valid_stmt_indices)} ---")
    
    # 2. 数据预处理 (日期修正等)
    for idx in valid_stmt_indices:
        st = statement_records[idx]
        st['_dt'] = safe_parse_date_to_date(st['date'])
        # CNPJ 提取增强测试
        if not st.get('cnpj'):
            found_plain = re.findall(r'\b\d{14}\b|\b\d{11}\b', st.get('desc', ''))
            if found_plain: st['cnpj'] = found_plain[0]
        
        raw_desc = st.get('desc', '').upper()
        st['_std_partner'] = re.sub(r'^(PIX|TED|DOC|BOLETO|RECEBIMENTO|TRANSFERENCIA|RECEBIMENTOS)S?\s+(RECEBIDO|ENVIADO|RECEBIDA|TRANSF|QRS|PAGTO|TIT)?\s*', '', raw_desc)

    for rep in report_records:
        rep['_dt_pay'] = safe_parse_date_to_date(rep.get('pay_date'))
        rep['_dt_due'] = safe_parse_date_to_date(rep.get('due_date'))
        rep['_dt_best'] = rep['_dt_pay'] or rep['_dt_due']
        rep['_std_partner'] = rep.get('name', '')

    # 3. 运行 1对1 匹配 (简化版测试)
    for r_idx, rep in enumerate(report_records):
        rep_amt = rep['amount']
        dt_rep = rep['_dt_best']
        if not dt_rep: continue
        
        best_s_idx = -1
        for s_idx in valid_stmt_indices:
            if s_idx in used_statement_indices: continue
            st = statement_records[s_idx]
            st_amt = st['amount']
            
            # 金额匹配逻辑
            is_amt_match = abs(st_amt - rep_amt) <= tolerance or (st_amt > rep_amt and (st_amt - rep_amt)/rep_amt <= 0.05)
            if not is_amt_match: continue
            
            # 简单名称匹配
            sim = calculate_similarity(rep['_std_partner'], st['_std_partner'])
            if sim > 0.6:
                best_s_idx = s_idx
                break
        
        if best_s_idx != -1:
            used_statement_indices.add(best_s_idx)
            st = statement_records[best_s_idx]
            
            # 测试匹配类型降级逻辑
            has_amt_diff = abs(st['amount'] - rep_amt) > 0.01
            m_type = "STRONG" if not has_amt_diff else "MEDIUM"
            
            note = ""
            if st['amount'] > rep_amt + 0.01:
                note = f"⚠️ 疑似含利息 (差额: {st['amount'] - rep_amt:.2f})"
            
            recon_results.append({
                "rep": rep, "st": st, "type": m_type, "note": note
            })

    return recon_results

def resolve_result_file(explicit: str = "", required_sheets=("核对差异", "流水详情")) -> Path:
    if explicit:
        candidate = Path(explicit)
        if candidate.exists():
            return candidate
        raise FileNotFoundError(f"Input file not found: {candidate}")

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
    parser = argparse.ArgumentParser(description="Run algorithm smoke test with a reconciliation snapshot.")
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

    reports = []
    for _, row in df_diff.iterrows():
        reports.append({
            'name': row['往来单位'],
            'amount': float(str(row['报告金额']).replace(',', '')),
            'due_date': row['到期日'],
            'pay_date': row['支付日'],
        })

    stmts = []
    for _, row in df_stmts.iterrows():
        amt_str = str(row['金额']).replace(',', '')
        try:
            amt = float(amt_str)
        except Exception:
            amt = 0
        stmts.append({
            'date': row['交易日期'],
            'desc': row['流水描述/备注'],
            'amount': amt,
            'cnpj': row['对方CNPJ'],
        })

    results = test_algorithm(reports, stmts)

    print("\n--- 测试结果摘要 ---")
    print(f"输入文件: {file_path}")
    strong_count = len([r for r in results if r['type'] == 'STRONG'])
    medium_count = len([r for r in results if r['type'] == 'MEDIUM'])
    print(f"完美匹配 (STRONG): {strong_count}")
    print(f"智能匹配 (MEDIUM): {medium_count}")

    print("\n--- 差异标注检查 (前5条 MEDIUM) ---")
    for r in [res for res in results if res['type'] == 'MEDIUM'][:5]:
        print(f"单位: {r['rep']['name']}, 报告: {r['rep']['amount']}, 流水: {r['st']['amount']}, 备注: {r['note']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
