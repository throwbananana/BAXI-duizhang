# Brazil Recognition Tool Refactoring

## Overview
This project has been refactored from a monolithic script into a modular Python package `brazil_tool`. This improves maintainability, testing, and reuse.

## New Structure
- **brazil_tool/**: The main package.
  - **core/**: Core business logic.
    - `parser.py`: Invoice parsing logic (regex, rules).
    - `pdf.py`: PDF text extraction and OCR handling.
    - `models.py`: Data structures (Invoice, Item).
    - `llm.py`: Large Language Model integration.
  - **db/**: Database management.
    - `payment_manager.py`: SQLite operations for payments.
  - **config.py**: Configuration loading and dependency checking.

## Scripts
- **check_env.py**: Run this to check if Tesseract, Poppler, and Python libraries are installed correctly.
- **example_usage.py**: A simple script demonstrating how to parse PDFs using the new library without opening the GUI.
- **brazil_product_code_v1.02.py**: The original GUI application (kept for compatibility).

## How to Develop
When adding new parsing rules, edit `brazil_tool/core/parser.py`.
When changing database schema, edit `brazil_tool/db/payment_manager.py`.

## Dependencies
See `check_env.py` for a list of required libraries.
