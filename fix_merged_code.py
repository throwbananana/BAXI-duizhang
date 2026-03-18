# -*- coding: utf-8 -*-
import json
import os
import re

def is_money_like(s):
    if not isinstance(s, str): return False
    # Matches patterns like 1.234,56 or 123.456.789,00
    return bool(re.search(r'\d+[\.\,]\d+[\.\,]\d+', s)) or bool(re.search(r'\d+,\d{2}$', s) and '.' in s)

def cleanup_mapping(filepath="mapping_db.json"):
    if not os.path.exists(filepath):
        print(f"File {filepath} not found.")
        return

    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if "products" not in data:
        print("No products found in mapping.")
        return

    original_count = len(data["products"])
    new_products = {}
    removed_count = 0

    for k, v in data["products"].items():
        std_code = v.get("std_code", "")
        std_name = v.get("std_name", "")
        
        # If the key itself looks like a normalized money value (too many digits or specific sequence)
        # or if the std_code/std_name looks like formatted money, we might want to remove it.
        # But wait, some std_codes might be valid but wrongly mapped.
        
        # The most obvious corruption is when std_code or std_name contains dots and commas like currency
        if is_money_like(std_code) or is_money_like(std_name):
            removed_count += 1
            continue
            
        # Also, keys that are 12+ digits and originated from formatted money often have 00 at the end
        if len(k) >= 11 and k.endswith("00") and (is_money_like(std_code) or not std_code):
             # This is a bit risky but likely a money value
             pass 

        new_products[k] = v

    data["products"] = new_products
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Cleanup complete. Removed {removed_count} corrupted entries. Remaining: {len(new_products)}/{original_count}")

if __name__ == "__main__":
    cleanup_mapping()
