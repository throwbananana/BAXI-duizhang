
import os

file_path = r"C:\Users\123\Downloads\巴西识别工具\brazil_product_code_v1.02.py"

with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
    lines = f.readlines()

final_lines = []
i = 0
fixed_count = 0
while i < len(lines):
    line = lines[i]
    if i > 0:
        prev_line = final_lines[-1]
        # Check if current line starts with 'if ' and previous line is a comment
        # And we suspect it was a false split.
        if line.strip().startswith('if ') and prev_line.strip().startswith('#'):
            # Heuristic: If it contains ':', it's likely code (keep split)
            # If it doesn't, it's likely comment text (merge back)
            if ':' not in line:
                # Merge back
                # We want to match the indentation of the comment? 
                # Or just append to previous line.
                # prev_line has \n at end.
                merged = prev_line.rstrip() + " " + line.strip() + "\n"
                final_lines[-1] = merged
                fixed_count += 1
                i += 1
                continue
    
    final_lines.append(line)
    i += 1

print(f"Fixed {fixed_count} false splits.")

with open(file_path, 'w', encoding='utf-8') as f:
    f.writelines(final_lines)

