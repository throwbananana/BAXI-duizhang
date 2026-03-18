import re

with open('brazil_product_code_v1.02.py', 'r', encoding='utf-8') as f:
    content = f.read()

with open('heuristic_fix.py', 'r', encoding='utf-8') as f:
    new_methods = f.read()

# Replace the entire block from _find_subset_match down to auto_save_results
# We need to find the start and end of this block
start_marker = "    def _find_subset_match"
end_marker = "    def auto_save_results"

start_idx = content.find(start_marker)
end_idx = content.find(end_marker)

if start_idx != -1 and end_idx != -1:
    new_content = content[:start_idx] + new_methods + "\n\n" + content[end_idx:]
    with open('brazil_product_code_v1.02.py', 'w', encoding='utf-8') as f:
        f.write(new_content)
    print("Successfully patched reconciliation methods.")
else:
    print(f"Markers not found: start={start_idx}, end={end_idx}")