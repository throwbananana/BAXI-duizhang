import sys
import re
import os
import importlib.util
from datetime import datetime, date, timedelta
from unittest.mock import Mock

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# 模拟 MappingManager
class MockMappingMgr:
    def get_partner_std(self, name_or_cnpj):
        if not name_or_cnpj: return None
        mapping = {
            "NORTETOOLS": "NORTETOOLS COMERCIO",
            "SHPP": "NORTETOOLS (SHPP)",
            "ARGOTECH": "ARGOTECH INDUSTRIAL"
        }
        for k, v in mapping.items():
            if k in str(name_or_cnpj).upper(): return v
        return name_or_cnpj

def run_test():
    print("开始算法内部压力测试...")
    
    # 动态加载模块
    module_name = "brazil_product_code_v1.02"
    file_path = os.path.abspath(f"{module_name}.py")
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    
    ReconciliationWorker = mod.ReconciliationWorker
    
    # 准备测试数据
    reports = [
        {"name": "CLIENTE A", "invoice_ref": "INV-001", "amount": 1000.00, "due_date": "2025-10-10", "pay_date": "2025-10-10"},
        {"name": "CLIENTE B", "invoice_ref": "INV-002", "amount": 50000.00, "due_date": "2025-10-15", "pay_date": "2025-10-15"},
        {"name": "NORTE TOOLS", "invoice_ref": "NF-3000", "amount": 5925.20, "due_date": "2025-10-01", "pay_date": "2025-10-01"},
        {"name": "FIFO TEST", "invoice_ref": "F1", "amount": 1234.56, "due_date": "2025-11-01", "pay_date": "2025-11-01"},
        {"name": "FIFO TEST", "invoice_ref": "F2", "amount": 1234.56, "due_date": "2025-11-10", "pay_date": "2025-11-10"}
    ]
    
    statements = [
        {"date": "2025-10-12", "desc": "PIX RECEBIDO CLIENTE A", "amount": 1025.00, "cnpj": ""}, 
        {"date": "2025-10-20", "desc": "PIX 1", "amount": 10000.00, "cnpj": ""},
        {"date": "2025-10-20", "desc": "PIX 2", "amount": 15000.00, "cnpj": ""},
        {"date": "2025-10-21", "desc": "PIX 3", "amount": 25000.00, "cnpj": ""},
        {"date": "2025-11-20", "desc": "RECEBIMENTOS NORTE TOOLS", "amount": 5925.20, "cnpj": ""},
        {"date": "2025-11-02", "desc": "BOLETO", "amount": 1234.56, "cnpj": ""},
        {"date": "2025-11-11", "desc": "BOLETO", "amount": 1234.56, "cnpj": ""}
    ]
    
    # 模拟信号
    worker = ReconciliationWorker(reports, statements, MockMappingMgr(), tolerance=0.05)
    worker.progress = Mock()
    worker.progress.emit = Mock()
    
    results = worker.do_reconciliation()
    
    print("\n--- 测试结果汇总 ---")
    for res in results:
        rep = res['report']
        st = res.get('statement')
        m_type = res['type']
        note = res.get('note', '')
        if st:
            print(f"[{m_type}] {rep['name']} ({rep['amount']}) -> 流水:{st['amount']} | 备注:{note}")
        else:
            print(f"[NONE] {rep['name']} ({rep['amount']}) 未匹配")

    # 验证逻辑
    success = True
    print("\n--- 关键点验证 ---")
    
    # A
    a_match = next((r for r in results if r['report']['invoice_ref'] == "INV-001"), None)
    if a_match and a_match['statement'] and "利息" in str(a_match.get('note')):
        print("[PASS] 验证通过: 场景 A (利息自适应) 识别成功")
    else:
        print("[FAIL] 验证失败: 场景 A (利息自适应) 异常")
        success = False

    # B
    b_matches = [r for r in results if r['report']['invoice_ref'] == "INV-002" and r['statement']]
    if len(b_matches) == 3:
        print("[PASS] 验证通过: 场景 B (分拆匹配) 识别成功")
    else:
        print(f"[FAIL] 验证失败: 场景 B (分拆) 匹配数: {len(b_matches)}")
        success = False

    # C
    c_match = next((r for r in results if r['report']['invoice_ref'] == "NF-3000"), None)
    if c_match and c_match['statement']:
        print("[PASS] 验证通过: 场景 C (65天窗口) 识别成功")
    else:
        print("[FAIL] 验证失败: 场景 C (窗口) 异常")
        success = False

    # D
    f1_match = next((r for r in results if r['report']['invoice_ref'] == "F1"), None)
    if f1_match and f1_match['statement'] and f1_match['statement']['date'] == "2025-11-02":
        print("[PASS] 验证通过: 场景 D (FIFO 顺序) 识别成功")
    else:
        print("[FAIL] 验证失败: 场景 D (FIFO) 异常")
        success = False

    if not success:
        sys.exit(1)

if __name__ == "__main__":
    run_test()
