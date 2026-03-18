import os

file_path = r"C:\Users\123\Downloads\巴西识别工具\brazil_product_code_v1.02.py"

with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
    lines = f.readlines()

final_lines = []
count = 0
for i, line in enumerate(lines):
    if i == 3364:
        print(f"Replacing line 3365: {line.strip()}")
        # We need indentation.
        # It's inside groups[key] = {
        # Indent should be 20 spaces?
        # Let's guess 20.
        final_lines.append("                    'names': {}\n")
        final_lines.append("                }\n")
        count += 1
    else:
        final_lines.append(line)

print(f"Fixed {count} lines.")
with open(file_path, 'w', encoding='utf-8') as f:
    f.writelines(final_lines)