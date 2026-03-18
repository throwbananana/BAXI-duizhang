
with open('brazil_product_code_v1.02.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()
    for i, line in enumerate(lines):
        if "def apply_filter" in line or "group_combo.currentIndex()" in line or "平均" in line:
            print(f"{i+1}: {line.strip()}")
