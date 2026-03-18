
import os

file_path = r"C:\Users\123\Downloads\巴西识别工具\brazil_product_code_v1.02.py"

with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
    lines = f.readlines()

count = 0
for i, line in enumerate(lines):
    # Check for docstrings ending with ?"" or similar corruption
    if '?""' in line or '锟?""' in line:
        print(f"Line {i+1}: {line.strip()}")
        count += 1
    # Check for incomplete strings ending with ?)
    elif line.strip().endswith('?)'):
        print(f"Line {i+1}: {line.strip()}")
        count += 1
    elif line.strip().endswith('?'):
        # Might be valid python if ends with ?, but unlikely in this context unless comment
        if 'print' in line or 'label' in line or 'QMessageBox' in line or '"""' in line:
             print(f"Line {i+1}: {line.strip()}")
             count += 1

print(f"Found {count} suspicious lines.")
