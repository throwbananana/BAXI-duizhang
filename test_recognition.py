import os
import sys
import importlib.util

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Load the main script as a module
file_path = 'brazil_product_code_v1.02.py'
module_name = 'brazil_parser'

try:
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    parser = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = parser
    spec.loader.exec_module(parser)
except Exception as e:
    print(f"Error loading module: {e}")
    sys.exit(1)

files = [
    "2062.pdf", "2063.pdf", "2064.pdf", "2075.pdf", 
    "2080.pdf", "2104.pdf", "2106.pdf"
]

print(f"{'File':<10} | {'Status':<10} | {'Natureza (Translated)':<45} | {'Total':<10} | {'Desc Sample (Fixed?)'}")
print("-" * 110)

for f in files:
    if not os.path.exists(f):
        print(f"{f:<10} | File not found")
        continue
        
    try:
        # 1. Extract Text
        text, meta = parser.extract_text_from_pdf(f, prefer_pymupdf=True)
        
        if not text:
            print(f"{f:<10} | No Text Extracted")
            continue
            
        # 2. Parse Invoice
        invoice = parser.parse_invoice_from_text(text, f)
        
        # 3. Check Results
        status = invoice.status
        natureza = invoice.natureza_operacao or "N/A"
        # Truncate natureza if too long for table
        if len(natureza) > 43: natureza = natureza[:40] + "..."
        
        total = invoice.total_nota or 0.0
        
        # Check description cleaning
        # Look for "PORTÁTIL" or "MÁQUINAS" or "PEÇAS"
        desc_sample = "N/A"
        if invoice.itens:
            # Join all descriptions to check for specific fixed words
            all_descs = " ".join([i.descricao or "" for i in invoice.itens])
            
            # Pick a relevant sample if possible
            if "PORTÁTIL" in all_descs:
                desc_sample = "Found: PORTÁTIL"
            elif "PORTaTIL" in all_descs:
                desc_sample = "FAIL: PORTaTIL"
            elif "PEÇAS" in all_descs:
                desc_sample = "Found: PEÇAS"
            elif "PEcAS" in all_descs:
                desc_sample = "FAIL: PEcAS"
            elif invoice.itens[0].descricao:
                desc_sample = invoice.itens[0].descricao[:20]
        
        print(f"{f:<10} | {status:<10} | {natureza:<45} | {total:<10.2f} | {desc_sample}")
        
    except Exception as e:
        print(f"{f:<10} | Error: {e}")
