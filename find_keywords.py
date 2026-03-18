
with open('brazil_product_code_v1.02.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()
    for i, line in enumerate(lines):
        if "合计" in line or "Summary" in line or "update_summary" in line:
            print(f"{i+1}: {line.strip()}")
