import pandas as pd
import os

def clean_code(code):
    if pd.isna(code):
        return ""
    code_str = str(code).strip()
    if code_str.endswith('.0'):
        code_str = code_str[:-2]
    return code_str

def apply_identification():
    # Paths
    acc_book_path = r'G:\Users\123\Documents\GitHub\BAXI-duizhang\2026年巴西销售明细帐.xlsx'
    extracted_data_path = r'G:\Users\123\xwechat_files\wxid_infilry7rbc522_d779\msg\file\2026-03\2601销售发票 (2)\2601销售发票\汇总提取_03251622\1月.xlsx'
    output_path = r'G:\Users\123\Documents\GitHub\BAXI-duizhang\1月_识别后.xlsx'
    
    # 1. Load accounting book mapping
    df_acc = pd.read_excel(acc_book_path, sheet_name='商品销售明细表', skiprows=2)
    code_to_names = {}
    name_to_code = {}
    for _, row in df_acc.iterrows():
        code = clean_code(row.iloc[3])
        name = str(row.iloc[4]).strip() if pd.notnull(row.iloc[4]) else ""
        if code and name and name != "nan":
            if code not in code_to_names: code_to_names[code] = set()
            code_to_names[code].add(name)
            if name not in name_to_code: name_to_code[name] = code
            
    # 2. Load extracted data
    df_ext = pd.read_excel(extracted_data_path)
    
    # 3. Process
    sorted_names = sorted(name_to_code.keys(), key=len, reverse=True)
    real_names = []
    match_methods = []
    
    for idx, row in df_ext.iterrows():
        raw_code = row.iloc[42]
        code = clean_code(raw_code)
        desc = str(row.iloc[44]).strip() if pd.notnull(row.iloc[44]) else ""
        real_name = "Unknown"
        method = "None"
        
        if code in code_to_names:
            names = code_to_names[code]
            for n in names:
                if n.lower() in desc.lower():
                    real_name = n
                    method = "Code + Desc Name Match"
                    break
            if real_name == "Unknown":
                if len(names) == 1:
                    real_name = list(names)[0]
                    method = "Code Match (Single)"
        
        if real_name == "Unknown":
            for name in sorted_names:
                if name.lower() in desc.lower():
                    real_name = name
                    method = "Desc Name Match"
                    break
        
        if real_name == "Unknown":
            for c, names in code_to_names.items():
                if c in desc:
                    real_name = list(names)[0]
                    method = "Desc Code Match"
                    break

        if real_name == "Unknown" and desc:
            parts = desc.split(' ')
            if parts:
                first_part = parts[0]
                if any(c.isdigit() for c in first_part) and len(first_part) > 3:
                    real_name = first_part
                    method = "Heuristic (First Word)"
        
        real_names.append(real_name)
        match_methods.append(method)

    # Add columns
    df_ext['真实品名'] = real_names
    df_ext['匹配方式'] = match_methods
    
    # Save
    df_ext.to_excel(output_path, index=False)
    print(f"File with identified names saved to: {output_path}")

if __name__ == "__main__":
    apply_identification()
