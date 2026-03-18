import importlib.util
import sys
import re

def load_module():
    file_path = 'brazil_product_code_v1.02.py'
    module_name = 'brazil_parser'
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    parser = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = parser
    spec.loader.exec_module(parser)
    return parser

p = load_module()

def debug_pdf(fname):
    print(f"\n--- Debugging {fname} ---")
    text, _ = p.extract_text_from_pdf(fname, prefer_pymupdf=True)
    
    # Simulate parser's block extraction
    from brazil_tool.core.utils import extract_block
    dest_block = extract_block(text,
        r'DESTINAT[ÁA]RIO/REMETENTE|DESTINATARIO/REMETENTE',
        r'DADOS\s+DO\s+PRODUTO/SERVI[CÇ]O|TRANSPORTADOR|CALCULO\s+DO\s+IMPOSTO|C[ÁA]LCULO\s+DO\s+IMPOSTO')
    
    if dest_block:
        print("FOUND DEST_BLOCK:")
        print("-" * 40)
        print(dest_block)
        print("-" * 40)
        
        # Test naming regex
        # Look for the name candidate
        from brazil_tool.constants import CNPJ_RE, CPF_RE
        flat = dest_block.replace("\n", " ")
        m_name_cnpj = re.search(r'([A-Z][A-Z0-9 &\.,\-]{2,80})\s+(' + CNPJ_RE + r'|' + CPF_RE + r')', flat)
        if m_name_cnpj:
            print(f"Regex Match (Name+CNPJ): '{m_name_cnpj.group(1)}'")
        else:
            print("Regex Match (Name+CNPJ): FAILED")
            
        # Lines dump
        lines = [l.strip() for l in dest_block.splitlines() if l.strip()]
        print(f"Top lines: {lines[:5]}")
    else:
        print("DEST_BLOCK NOT FOUND!")

debug_pdf('2075.pdf')
debug_pdf('2106.pdf')
