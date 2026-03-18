import argparse
import os
import sys
from pathlib import Path

# Add current dir to path to import brazil_tool
sys.path.append(os.getcwd())

import fitz  # PyMuPDF
from brazil_tool.core.statement_parser import BankStatementParser


def extract_text(pdf_path: Path) -> str:
    text = ""
    with fitz.open(str(pdf_path)) as doc:
        for page in doc:
            text += page.get_text()
    return text


def resolve_pdf_path(explicit: str = "") -> Path | None:
    if explicit:
        p = Path(explicit)
        return p if p.exists() else None

    default = Path(r"build\extrato-itau_01_09_2025_09-55WANG.pdf")
    if default.exists():
        return default

    build_dir = Path("build")
    if build_dir.exists():
        candidates = sorted(build_dir.glob("extrato*.pdf"), key=lambda x: x.stat().st_mtime, reverse=True)
        if candidates:
            return candidates[0]
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Parse an Itau statement PDF with BankStatementParser.")
    parser.add_argument("--pdf", default="", help="PDF path. If omitted, tries default and latest build/extrato*.pdf.")
    args = parser.parse_args()

    pdf_path = resolve_pdf_path(args.pdf)
    if not pdf_path:
        print("File not found. Provide --pdf or place a statement PDF under build/.")
        return 1

    try:
        print(f"Analyzing {pdf_path}...")
        text = extract_text(pdf_path)
        print("\nRunning Parser...")
        transactions = BankStatementParser.parse_statement(text)
        print(f"\nFound {len(transactions)} transactions.")
        for tx in transactions[:10]:
            print(tx)
        return 0
    except Exception as e:
        print(f"Parsing failed: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
