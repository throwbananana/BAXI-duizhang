import os

file_path = r"C:\Users\123\Downloads\巴西识别工具\brazil_product_code_v1.02.py"

with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
    lines = f.readlines()

final_lines = []
skip = False
for i, line in enumerate(lines):
    if skip:
        skip = False
        continue
    
    # Check for line 1317 approx
    if 'class ProductMappingDialog(QDialog):' in line:
        final_lines.append(line)
        if i + 1 < len(lines):
            next_line = lines[i+1]
            if '"""' in next_line and '产品编码映射管理对话' in next_line:
                print(f"Fixing ProductMappingDialog docstring at line {i+2}: {next_line.strip()}")
                final_lines.append('    """产品编码映射管理对话框"""\n')
                skip = True
            else:
                pass
    # Check for line 1324 approx
    elif 'info_label = QLabel' in line and '自动提取' in line:
         # Check if it ends with corruption
         if '提取' in line and ('?' in line or '' in line): # '' might be replaced by '?' or unicode replacement char
             print(f"Fixing QLabel info_label at line {i+1}: {line.strip()}")
             final_lines.append('        info_label = QLabel("管理巴西产品编码到国内编码的映射关系。可以手动添加或从表格中自动提取。")\n')
         else:
             final_lines.append(line)
    else:
        final_lines.append(line)

with open(file_path, 'w', encoding='utf-8') as f:
    f.writelines(final_lines)

print("File patched round 4.")