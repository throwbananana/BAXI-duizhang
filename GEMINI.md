# Brazil Recognition & Reconciliation Tools

This directory contains standalone Python applications designed for financial automation, specifically focused on the Brazilian market context (fiscal document extraction) and general bank reconciliation tasks.

## Project Overview

The tools in this workspace facilitate:
1.  **Fiscal Data Extraction:** Parsing Brazilian invoice documents (PDFs/Images) to extract structured data like CNPJ, CPF, and monetary values.
2.  **Financial Reconciliation:** Automating the comparison between bank statements (PDFs) and internal financial records.

## Key Applications

### 1. Brazil Product Code Tool (`brazil_product_code_v1.02.py`)
A **PySide6** desktop application for batch processing Brazilian fiscal documents.

*   **Functionality:**
    *   Extracts fields such as CNPJ, CPF, ZIP codes (CEP), and monetary values.
    *   Supports multiple input formats: PDF (via `PyMuPDF` or `PyPDF2`) and Images (via `pytesseract` OCR).
    *   Normalizes Brazilian currency formats (e.g., `1.234,56` to `1234.56`).
*   **Key Dependencies:** `PySide6`, `pytesseract`, `pdf2image`, `PyMuPDF` (fitz), `openpyxl`.
*   **Configuration:** Uses `danfe_batch_gui_settings.ini` for persistence.

### 2. Reconciliation GUI (`duizhang_gui v1.00.py`)
A **Tkinter**-based application for bank statement reconciliation.

*   **Functionality:**
    *   Parses PDF bank statements (specifically "BAC" and "St. Georges Bank" formats).
    *   Extracts transaction details: Date, Description, Doc Reference, Debit, Credit, Balance.
    *   Likely provides a mechanism to match these transactions against an internal ledger.
*   **Key Dependencies:** `tkinter`, `pandas`, `pdfplumber`, `sqlite3`.

## Development & Usage

### Running the Tools
These scripts are designed to be run directly with Python or built into executables.

**Python Execution:**
```bash
# Run the Brazil Product Code Tool
python "brazil_product_code_v1.02.py"

# Run the Reconciliation Tool
python "duizhang_gui v1.00.py"
```

### Building Executables
The presence of `.spec` files indicates that **PyInstaller** is used for distribution.

```bash
# Build Brazil Product Code Tool
pyinstaller "brazil_product_code_v1.02.spec"

# Build Reconciliation Tool
pyinstaller "duizhang_gui v1.00.spec"
```

## Tech Stack
*   **Languages:** Python 3.x
*   **GUI Frameworks:** PySide6 (Qt), Tkinter
*   **Data Processing:** Pandas, NumPy, OpenPyXL
*   **PDF/OCR:** PyMuPDF, PyPDF2, pdfplumber, pdf2image, Tesseract OCR
*   **Packaging:** PyInstaller
