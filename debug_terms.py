
import os

file_path = r"C:\Users\123\Downloads\巴西识别工具\brazil_product_code_v1.02.py"

with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    if "'names': {}" in line:
        print(f"Line {i+1}: {line.strip()}")
