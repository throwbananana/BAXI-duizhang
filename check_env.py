# -*- coding: utf-8 -*-
import sys
import os
import shutil

def check_environment():
    print(">>> 巴西识别工具 环境自检程序 <<<")
    print("-" * 50)
    
    issues = []
    
    # 1. Python Libraries
    required = ["PySide6", "PyPDF2", "fitz", "PIL", "pdf2image", "pytesseract", "openpyxl"]
    print("[1] 检查 Python 依赖库...")
    for lib in required:
        try:
            if lib == "fitz":
                import fitz
                print(f"  [OK] fitz (PyMuPDF)")
            elif lib == "PIL":
                import PIL
                print(f"  [OK] Pillow")
            else:
                __import__(lib)
                print(f"  [OK] {lib}")
        except ImportError:
            print(f"  [XX] 缺失库: {lib}")
            issues.append(f"缺少 Python 库: {lib} (请运行 pip install {lib if lib!='fitz' else 'pymupdf'} {lib if lib!='PIL' else 'Pillow'})")

    # 2. External Tools
    print("\n[2] 检查外部工具...")
    
    # Tesseract
    tess_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
    # Try reading from config if possible
    if os.path.exists("danfe_batch_gui_settings.ini"):
        # simple parse
        with open("danfe_batch_gui_settings.ini", "r", encoding='utf-8', errors='ignore') as f:
            for line in f:
                if "tesseract_cmd" in line and "=" in line:
                    tess_cmd = line.split("=", 1)[1].strip()
                    break
    
    if os.path.exists(tess_cmd):
        print(f"  [OK] Tesseract Found: {tess_cmd}")
    else:
        # Check PATH
        if shutil.which("tesseract"):
             print(f"  [OK] Tesseract Found in PATH")
        else:
             print(f"  [XX] Tesseract 未找到 (路径: {tess_cmd})")
             issues.append("未找到 Tesseract-OCR，OCR 功能将不可用。")

    # Poppler
    # Check if pdfinfo or pdftoppm is in path
    if shutil.which("pdftoppm") or shutil.which("pdfinfo"):
        print(f"  [OK] Poppler Found in PATH")
    else:
        print(f"  [!!] Poppler 未在系统 PATH 中 (可能影响 PDF 转图片)")
        # issues.append("未找到 Poppler，可能影响 PDF 扫描件处理。")

    print("-" * 50)
    if issues:
        print("发现以下问题，建议修复以获得完整功能：")
        for i, issue in enumerate(issues, 1):
            print(f"{i}. {issue}")
        print("\n提示：请修改 'danfe_batch_gui_settings.ini' 或安装相应软件。")
    else:
        print("环境检查通过！所有核心组件就绪。")
    
    input("\n按回车键退出...")

if __name__ == "__main__":
    check_environment()
