
import os

file_path = r"C:\Users\123\Downloads\巴西识别工具\brazil_product_code_v1.02.py"

with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
    lines = f.readlines()

final_lines = []
count = 0
keywords = ['for ', 'if ', 'self.', 'return', 'print', 'elif ', 'else:', 'try:', 'except:', 'finally:', 'with ', 'while ']

for line in lines:
    stripped = line.strip()
    # Check for merged lines after }
    # e.g. "}        for"
    if '} ' in line:
        # Find the last } that is followed by spaces and a keyword
        # But we need to be careful about dicts in code: "x = { 'a': 1 } if y else z" (valid)
        # But "x = {}    for k in v:" (invalid, two statements)
        
        # Split by '}'
        parts = line.split('}')
        # We look at parts[1], parts[2] etc.
        # If any part starts with spaces + keyword, it's a merge.
        
        # Reconstruct
        new_line_parts = []
        found_merge = False
        
        current_segment = parts[0]
        for i in range(1, len(parts)):
            segment = parts[i]
            # segment starts after '}'
            # Check if segment looks like "    keyword..."
            
            is_merge = False
            for kw in keywords:
                # We check if segment stripped starts with kw
                # AND segment has leading spaces
                if segment.lstrip().startswith(kw):
                    # Check indentation length
                    spaces = len(segment) - len(segment.lstrip())
                    if spaces >= 4: # Arbitrary threshold for "merged line indent" 
                         is_merge = True
                         break
            
            if is_merge:
                print(f"Fixing merged line at }} : ...}} {segment[:20]}...")
                new_line_parts.append(current_segment + "}\n")
                # The segment becomes the start of next line (with its indent)
                current_segment = segment #.lstrip()? No, keep indent.
                # But segment starts with spaces.
                # If we just append segment, it starts with spaces.
                # But we need to ensure it's a new line.
                # current_segment will be appended to new_line_parts on next iteration or end.
                found_merge = True
                count += 1
            else:
                current_segment += "}" + segment
        
        new_line_parts.append(current_segment)
        
        # If we split, we have multiple lines.
        # But we constructed a list of strings, some ending with \n.
        # The last one might have \n from original line.
        
        for p in new_line_parts:
            final_lines.append(p)
            # If p doesn't end with \n (and it's not the very last part of original), add \n?
            # We added \n above for splits.
            # The last part preserves original ending.
    else:
        final_lines.append(line)

print(f"Fixed {count} merged lines.")
with open(file_path, 'w', encoding='utf-8') as f:
    f.writelines(final_lines)
