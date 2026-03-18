# -*- coding: utf-8 -*-
import os
import sys
from brazil_tool.core.pdf import extract_text_from_pdf
from brazil_tool.core.parser import parse_invoice_from_text
from brazil_tool.config import check_external_tools, load_settings

def test_parser():
    print(">>> Testing Refactored Brazil Tool Modules")
    
    # 1. Check Config & Tools
    config = load_settings()
    print(f"Config Loaded: Tesseract={config['tesseract_cmd']}, PyMuPDF={config['prefer_pymupdf']}")
    
    tools = check_external_tools(config)
    print(f"Tool Status: {tools}")
    
    files = [f for f in os.listdir('.') if f.lower().endswith('.pdf')]
    print(f"Found {len(files)} PDFs in current dir.")
    
    print("-" * 100)
    print(f"{'File':<15} | {'Status':<10} | {'Natureza':<40} | {'Total':<10}")
    print("-" * 100)

    for f in files[:5]: # Test first 5
        try:
            # 2. Extract
            text, meta = extract_text_from_pdf(
                f, 
                prefer_pymupdf=config['prefer_pymupdf'],
                enable_ocr=config['enable_ocr'],
                ocr_lang=config['ocr_lang'],
                poppler_path=config['poppler_path']
            )
            
            if not text:
                print(f"{f:<15} | No Text")
                continue
                
            # 3. Parse
            inv = parse_invoice_from_text(text, f)
            
            # 4. Result
            nat = inv.natureza_operacao or "N/A"
            if len(nat) > 38: nat = nat[:35] + "..."
            
            print(f"{f:<15} | {inv.status:<10} | {nat:<40} | {inv.total_nota or 0:.2f}")
            
        except Exception as e:
            print(f"{f:<15} | Error: {e}")

if __name__ == "__main__":
    test_parser()
