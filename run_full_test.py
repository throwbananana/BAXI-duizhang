
import pandas as pd
import argparse
import unicodedata
import re
from datetime import datetime, timedelta
from itertools import combinations
import time
from pathlib import Path

# --- 模拟算法核心组件 ---

def safe_parse_date(date_str):
    if not date_str or date_str == '-': return None
    try:
        if isinstance(date_str, datetime): return date_str.date()
        for fmt in ["%Y/%m/%d", "%d/%m/%Y", "%Y-%m-%d"]:
            try: return datetime.strptime(str(date_str).split(' ')[0], fmt).date()
            except: continue
    except: pass
    return None

def normalize_text(text):
    if not text: return ""
    return unicodedata.normalize('NFKD', str(text)).encode('ascii', 'ignore').decode('ascii').upper()

def calculate_similarity(s1, s2):
    import difflib
    if not s1 or not s2: return 0.0
    s1, s2 = str(s1).strip().lower(), str(s2).strip().lower()
    return difflib.SequenceMatcher(None, s1, s2).ratio()

def _find_subset_match(items, target_amt, max_size=12, tolerance=0.05, max_combos=100000):
    if not items: return None, 0.0
    search_items = sorted(items, key=lambda x: x['amount'], reverse=True)
    n = len(search_items)
    best_combo = None
    min_diff = float('inf')
    checked = 0
    for size in range(1, min(n, max_size) + 1):
        if checked > max_combos: break
        for combo in combinations(search_items, size):
            checked += 1
            sum_amt = sum(x['amount'] for x in combo)
            diff = abs(sum_amt - target_amt)
            if diff < 1e-5: return combo, 0.0
            if diff <= tolerance and diff < min_diff:
                min_diff = diff
                best_combo = combo
                if diff < 0.01: return combo, (sum_amt - target_amt)
    if best_combo:
        actual_sum = sum(x['amount'] for x in best_combo)
        return best_combo, (actual_sum - target_amt)
    return None, 0.0

# --- 测试运行器 ---

def run_upgraded_recon(reports, stmts):
    used_report_indices = set()
    used_statement_indices = set()
    recon_results = []
    tolerance = 0.05
    
    # 1. 预处理流水 (噪音过滤 & CNPJ 增强)
    noise_keywords = ["SDO CTA/APL", "REND PAGO APLIC", "RES APLIC AUT", "SDO CTA ANT", "SALDO TOTAL", "DISPONIVEL", "SALDO ANTERIOR", "SDO CTA/APL AUTOM", "SALDO FINAL", "SALDO INICIAL"]
    valid_stmts = []
    for i, s in enumerate(stmts):
        desc = s['desc'].upper()
        norm_desc = normalize_text(desc)
        if s['amount'] > 0 and not any(k in norm_desc for k in noise_keywords):
            # 增强提取
            if not s.get('cnpj'):
                found_plain = re.findall(r'\b\d{14}\b|\b\d{11}\b', desc)
                if found_plain: s['cnpj'] = found_plain[0]
            # 剥离前缀
            s['_std_partner'] = re.sub(r'^(PIX|TED|DOC|BOLETO|RECEBIMENTO|TRANSFERENCIA|RECEBIMENTOS)S?\s+(RECEBIDO|ENVIADO|RECEBIDA|TRANSF|QRS|PAGTO|TIT)?\s*', '', desc)
            s['_dt'] = safe_parse_date(s['date'])
            s['_orig_idx'] = i
            valid_stmts.append(s)

    # 预处理报告
    for i, r in enumerate(reports):
        r['_dt_best'] = safe_parse_date(r['pay_date']) or safe_parse_date(r['due_date'])
        r['_std_partner'] = r['name']
        r['_orig_idx'] = i

    # Phase 1: 1对1 智能核对 (包含 SUSPECT 逻辑)
    for r_idx, rep in enumerate(reports):
        best_s_idx = -1
        best_score = -1
        rep_amt = rep['amount']
        dt_rep = rep['_dt_best']
        if not dt_rep: continue

        for vs_idx, st in enumerate(valid_stmts):
            if vs_idx in used_statement_indices: continue
            
            st_amt = st['amount']
            sim = calculate_similarity(rep['_std_partner'], st['_std_partner'])
            date_diff = abs((dt_rep - st['_dt']).days) if st['_dt'] else 999
            
            is_exact_amt = abs(st_amt - rep_amt) < 0.01
            max_days = 90 if any(k in st['desc'].upper() or k in rep['_std_partner'].upper() for k in ["NORTE", "SHPP", "GENIOBOX", "AMERICA"]) else 65
            if is_exact_amt and sim >= 0.85: max_days = 180
            
            score = 0
            if date_diff <= max_days or (is_exact_amt and sim >= 0.7 and date_diff <= 220):
                if is_exact_amt: score += 50
                if sim >= 0.85: score += 40
                score -= date_diff * 0.5
                
                if score > best_score and score >= 30:
                    best_score = score
                    best_s_idx = vs_idx
        
        if best_s_idx != -1:
            used_statement_indices.add(best_s_idx)
            used_report_indices.add(r_idx)
            st = valid_stmts[best_s_idx]
            
            date_diff = abs((dt_rep - st['_dt']).days)
            m_type = "MEDIUM"
            if abs(st['amount'] - rep_amt) < 0.01:
                if best_score >= 80 and date_diff <= 65: m_type = "STRONG"
                elif date_diff > 90: m_type = "SUSPECT"
            
            recon_results.append({"type": m_type, "rep": rep, "st": st, "diff": rep_amt - st['amount']})

    # Phase 2: 聚合匹配 (大额增强)
    remaining_vs = [i for i in range(len(valid_stmts)) if i not in used_statement_indices]
    for vs_idx in remaining_vs:
        st = valid_stmts[vs_idx]
        dt_st = st['_dt']
        st_amt = st['amount']
        is_large = st_amt > 30000
        
        candidates = []
        for r_idx, rep in enumerate(reports):
            if r_idx in used_report_indices: continue
            if calculate_similarity(rep['_std_partner'], st['_std_partner']) > 0.4 or is_large:
                dt_rep = rep['_dt_best']
                if dt_rep and -15 <= (dt_st - dt_rep).days <= (120 if is_large else 90):
                    candidates.append(rep)
        
        batch_tol = tolerance if not is_large else max(tolerance, st_amt * 0.0002)
        subset, b_diff = _find_subset_match(candidates, st_amt, max_size=(25 if is_large else 15), tolerance=batch_tol)
        
        if subset:
            used_statement_indices.add(vs_idx)
            for r in subset:
                used_report_indices.add(r['_orig_idx'])
                recon_results.append({"type": "BATCH", "rep": r, "st": st, "diff": b_diff})

    return recon_results, len(reports), len(valid_stmts)

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

    # Fall back to the latest file; caller will print available sheets on failure.
    return sorted_candidates[0]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run upgraded reconciliation algorithm against an Excel snapshot.")
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
            'name': str(row['往来单位']),
            'amount': float(str(row['报告金额']).replace(',', '')),
            'due_date': str(row['到期日']),
            'pay_date': str(row['支付日']),
        })

    stmts = []
    for _, row in df_stmts.iterrows():
        try:
            amt = float(str(row['金额']).replace(',', ''))
        except Exception:
            amt = 0
        stmts.append({
            'date': str(row['交易日期']),
            'desc': str(row['流水描述/备注']),
            'amount': amt,
            'cnpj': str(row['对方CNPJ']),
        })

    results, total_reps, total_stmts = run_upgraded_recon(reports, stmts)

    df_res = pd.DataFrame([{'type': r['type'], 'amt': r['rep']['amount'], 'name': r['rep']['name']} for r in results])
    print("--- 升级后测试结果 ---")
    print(f"输入文件: {file_path}")
    print(f"总报告项: {total_reps}")
    print(f"有效流水项: {total_stmts}")
    print(f"匹配成功项: {len(results)}")
    print("\n匹配类型分布:")
    print(df_res['type'].value_counts())

    print("\n--- 关键修复点检查 (SUSPECT 延迟匹配) ---")
    suspects = [r for r in results if r['type'] == "SUSPECT"]
    for s in suspects[:5]:
        days = (safe_parse_date(s['st']['date']) - safe_parse_date(s['rep']['pay_date'])).days
        print(f"单位: {s['rep']['name']}, 金额: {s['rep']['amount']}, 日期差: {days} 天")

    print("\n--- 关键修复点检查 (大额聚合匹配) ---")
    large_batch = [r for r in results if r['type'] == "BATCH" and r['st']['amount'] > 10000]
    processed_st = set()
    for b in large_batch:
        st_ptr = id(b['st'])
        if st_ptr not in processed_st:
            matches = len([x for x in large_batch if id(x['st']) == st_ptr])
            print(f"流水金额: {b['st']['amount']}, 描述: {b['st']['desc']}, 匹配报告数: {matches}")
            processed_st.add(st_ptr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
