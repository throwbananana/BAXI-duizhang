import pandas as pd
import os
import re

def clean_code(code):
    if pd.isna(code):
        return ""
    code_str = str(code).strip()
    if code_str.endswith('.0'):
        code_str = code_str[:-2]
    return code_str

def identify_products():
    # Paths
    acc_book_path = r'G:\Users\123\Documents\GitHub\BAXI-duizhang\2026年巴西销售明细帐.xlsx'
    extracted_data_path = r'G:\Users\123\xwechat_files\wxid_infilry7rbc522_d779\msg\file\2026-03\2601销售发票 (2)\2601销售发票\汇总提取_03251622\1月.xlsx'
    
    # 1. Load accounting book mapping
    print(f"Loading accounting book from: {acc_book_path}")
    df_acc = pd.read_excel(acc_book_path, sheet_name='商品销售明细表', skiprows=2)
    
    code_to_names = {} # Map code to set of names
    name_to_code = {} 
    
    for _, row in df_acc.iterrows():
        code = clean_code(row.iloc[3])
        name = str(row.iloc[4]).strip() if pd.notnull(row.iloc[4]) else ""
        if code and name and name != "nan":
            if code not in code_to_names:
                code_to_names[code] = set()
            code_to_names[code].add(name)
            
            if name not in name_to_code:
                name_to_code[name] = code
            
    print(f"Loaded {len(code_to_names)} unique product codes and {len(name_to_code)} unique names.")

    # 2. Load extracted data
    print(f"Loading extracted data from: {extracted_data_path}")
    df_ext = pd.read_excel(extracted_data_path)
    
    results = []
    
    # Pre-sort names by length descending
    sorted_names = sorted(name_to_code.keys(), key=len, reverse=True)
    
    for idx, row in df_ext.iterrows():
        raw_code = row.iloc[42]
        code = clean_code(raw_code)
        desc = str(row.iloc[44]).strip() if pd.notnull(row.iloc[44]) else ""
        
        real_name = "Unknown"
        match_method = "None"
        matched_code = ""
        
        # Priority logic:
        # 1. If code matches, check if any of the associated names are in the description
        if code in code_to_names:
            names = code_to_names[code]
            # Disambiguate if multiple names for this code
            matched = False
            for n in names:
                if n.lower() in desc.lower():
                    real_name = n
                    match_method = "Code + Desc Name Match"
                    matched_code = code
                    matched = True
                    break
            
            if not matched:
                # If only one name for this code, use it
                if len(names) == 1:
                    real_name = list(names)[0]
                    match_method = "Code Match (Single)"
                    matched_code = code
                else:
                    # Multiple names, none in desc. Pick the first one but mark as ambiguous?
                    # Or just try name match globally
                    pass

        # 2. Global Name Match (if not matched by code yet or ambiguous)
        if real_name == "Unknown":
            for name in sorted_names:
                if name.lower() in desc.lower():
                    real_name = name
                    match_method = "Desc Name Match"
                    matched_code = name_to_code[name]
                    break
        
        # 3. Global Code Match in Desc
        if real_name == "Unknown":
            for c, names in code_to_names.items():
                if c in desc:
                    real_name = list(names)[0] # Just pick one
                    match_method = "Desc Code Match"
                    matched_code = c
                    break
        
        # 4. Final Fallback Heuristic
        if real_name == "Unknown" and desc:
            parts = desc.split(' ')
            if parts:
                first_part = parts[0]
                if any(c.isdigit() for c in first_part) and len(first_part) > 3:
                    real_name = first_part
                    match_method = "Heuristic (First Word)"

        results.append({
            "Row": idx + 2,
            "Extracted Code": code,
            "Description": desc[:100],
            "Real Product Name": real_name,
            "Matched Code": matched_code,
            "Match Method": match_method
        })

    # 3. Save results
    results_df = pd.DataFrame(results)
    results_df.to_excel("identified_products_v3.xlsx", index=False)
    
    print("\nIdentification Summary:")
    print(results_df['Match Method'].value_counts())
    
    print("\nFirst 10 rows:")
    print(results_df[['Row', 'Description', 'Real Product Name', 'Match Method']].head(10).to_string(index=False))
    
    print(f"\nTotal rows processed: {len(results_df)}")
    print("Full results saved to identified_products_v3.xlsx")

if __name__ == "__main__":
    identify_products()
