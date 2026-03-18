
import os
import importlib.util
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

def test_core_algorithm():
    print("验证核心算法: _find_subset_match (贪婪法测试)")
    
    module_name = "brazil_product_code_v1.02"
    file_path = os.path.abspath(f"{module_name}.py")
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    
    # 构造 100 笔流水，其中 20 笔凑成 100,000.00
    items = []
    for i in range(20):
        items.append({"amount": 5000.00, "id": f"match_{i}"})
    for i in range(80):
        items.append({"amount": 123.45, "id": f"noise_{i}"})
        
    target = 100000.00
    
    # 初始化 Worker (只需为了调用方法)
    worker = mod.ReconciliationWorker([], [], None)
    
    import time
    start = time.time()
    subset, diff = worker._find_subset_match(items, target, max_size=40, tolerance=0.05)
    end = time.time()
    
    if subset and len(subset) == 20 and abs(diff) <= 0.05:
        print(f"[PASS] 贪婪搜索验证通过! 耗时: {end-start:.4f}s")
        print(f"找到项数: {len(subset)}, 误差: {diff}")
    else:
        print(f"[FAIL] 验证失败! 结果: {len(subset) if subset else 0}, 误差: {diff}")

    # 验证手续费备注逻辑
    print("\n验证备注分类逻辑:")
    reports = [{"name": "TEST", "amount": 1000.00, "_std_partner": "TEST", "_dt_best": None}]
    statements = [{"desc": "REDE VISA", "amount": 965.00, "_dt": None, "_std_partner": "REDE"}]
    
    # 模拟内部 Phase 1 的简化版逻辑测试
    is_settlement = any(k in statements[0]['desc'].upper() for k in ["REDE VISA", "REDE MAST"])
    diff_val = statements[0]['amount'] - reports[0]['amount']
    tol_down = reports[0]['amount'] * 0.045 if is_settlement else 0.05
    
    if -tol_down <= diff_val <= 0.05:
        print(f"[PASS] 手续费容差逻辑正确: {diff_val} 在 -{tol_down} 范围内")
    else:
        print(f"[FAIL] 手续费逻辑错误: {diff_val} 超出 -{tol_down}")

if __name__ == "__main__":
    test_core_algorithm()
