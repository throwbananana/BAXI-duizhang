import os

file_path = r"C:\Users\123\Downloads\巴西识别工具\brazil_product_code_v1.02.py"

with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
    lines = f.readlines()

final_lines = []
count = 0
for line in lines:
    if '#' in line and 'if ' in line and line.strip().startswith('#'):
        # Check if 'if' follows the comment on same line
        # e.g. "   # comment?   if condition:"
        parts = line.split('if ', 1)
        if len(parts) == 2:
            # Check if the part before 'if' is just comment and spaces
            prefix = parts[0]
            if '#' in prefix:
                # We have a merged line
                print(f"Fixing merged line: {line.strip()}")
                # Split it
                # We need to preserve indentation for the 'if'
                # The 'if' should have same indentation as the comment usually, or whatever works.
                # Let's assume the spaces before '#' are the indentation.
                indent = prefix[:prefix.find('#')]
                
                comment_part = prefix.strip()
                # Remove the garbage at end of comment part if any
                if comment_part.endswith('?') or comment_part.endswith('锟?'):
                    comment_part = comment_part[:-1]
                
                final_lines.append(f"{indent}{comment_part}\n")
                final_lines.append(f"{indent}if {parts[1]}")
                count += 1
                continue
    final_lines.append(line)

print(f"Fixed {count} merged lines.")

with open(file_path, 'w', encoding='utf-8') as f:
    f.writelines(final_lines)
