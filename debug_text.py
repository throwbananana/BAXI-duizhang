import importlib.util
import sys

def load_module():
    file_path = 'brazil_product_code_v1.02.py'
    module_name = 'brazil_parser'
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    parser = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = parser
    spec.loader.exec_module(parser)
    return parser

p = load_module()

for fname in ['2075.pdf', '2080.pdf']:
    print(f"--- {fname} ---")
    text, _ = p.extract_text_from_pdf(fname, prefer_pymupdf=True)
    # Print the first 1000 characters to verify header/natureza
    print(text[:1000])
    print("\n" + "="*50 + "\n")
