# -*- coding: utf-8 -*-
import logging
import time
import os
import io
import base64
import re
from typing import Optional, Tuple, Dict

# --- Lazy Imports / Dependency Checking ---
try:
    from PyPDF2 import PdfReader
except ImportError:
    PdfReader = None

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None

try:
    from PIL import Image
except ImportError:
    Image = None

try:
    from pdf2image import convert_from_path
except ImportError:
    convert_from_path = None

try:
    import pytesseract
except ImportError:
    pytesseract = None

def check_dependencies() -> Dict[str, bool]:
    return {
        "PyPDF2": PdfReader is not None,
        "PyMuPDF": fitz is not None,
        "Pillow": Image is not None,
        "pdf2image": convert_from_path is not None,
        "pytesseract": pytesseract is not None
    }

def text_is_weak(text: str, min_len: int = 50, max_spaces_ratio: float = 0.8) -> bool:
    """Check if extracted text is weak/garbage (needs OCR)."""
    text = text.strip()
    if len(text) < min_len:
        return True
    if text.count(' ') / len(text) > max_spaces_ratio:
        return True
    return False

def extract_text_from_pdf(path: str, prefer_pymupdf: bool = True,
                          enable_ocr: bool = False, ocr_lang: str = "por",
                          poppler_path: Optional[str] = None) -> Tuple[str, Dict[str, str]]:
    """
    Extract text from PDF using PyMuPDF, PyPDF2, or OCR fallback.
    """
    meta = {}
    text = ""
    t0 = time.time()

    # 1. Try PyMuPDF
    if prefer_pymupdf and fitz is not None:
        try:
            with fitz.open(path) as doc:
                parts = [page.get_text("text") for page in doc]
            text = "\n".join(parts)
            meta["extractor"] = "PyMuPDF"
        except Exception as e:
            meta["pymupdf_error"] = str(e)

    # 2. Try PyPDF2
    if not text and PdfReader is not None:
        try:
            r = PdfReader(path)
            parts = []
            for p in r.pages:
                try:
                    parts.append(p.extract_text() or "")
                except Exception:
                    parts.append("")
            text = "\n".join(parts)
            meta["extractor"] = "PyPDF2"
        except Exception as e:
            meta["pypdf2_error"] = str(e)

    # 3. Fallback to OCR
    if enable_ocr and text_is_weak(text) and convert_from_path is not None and pytesseract is not None:
        images = []
        try:
            # Note: poppler_path must be configured if not in PATH
            images = convert_from_path(path, dpi=300, poppler_path=poppler_path)
            ocr_parts = [pytesseract.image_to_string(img, lang=ocr_lang) for img in images]
            text = "\n".join(ocr_parts)
            meta["extractor"] = (meta.get("extractor","") + "+OCR") if meta.get("extractor") else "OCR"
        except Exception as e:
            meta["ocr_error"] = str(e)
            if "poppler" in str(e).lower():
                meta["ocr_hint"] = "Poppler not found. Please install or set path."
            if "tesseract" in str(e).lower():
                meta["ocr_hint"] = "Tesseract not found. Please install or set path."
        finally:
            for img in images:
                try:
                    img.close()
                except Exception:
                    pass

    meta["elapsed_sec"] = f"{time.time()-t0:.2f}"
    return text, meta

def check_cancellation_via_ocr(file_path: str, poppler_path: Optional[str] = None) -> bool:
    """Use OCR to check for 'CANCELADO' watermark on the first page."""
    if convert_from_path is None or pytesseract is None:
        return False
    images = []
    try:
        images = convert_from_path(file_path, first_page=1, last_page=1, dpi=200, poppler_path=poppler_path)
        if not images:
            return False
        text = pytesseract.image_to_string(images[0], lang='por')
        if re.search(r'\bCANCELADO\b', text, re.I):
            return True
    except Exception as e:
        logging.warning(f"OCR Cancellation Check Failed {file_path}: {e}")
    finally:
        for img in images:
            try:
                img.close()
            except Exception:
                pass
        
    return False

def get_first_page_image_data_url(file_path: str, poppler_path: Optional[str], max_side: int = 1600) -> Optional[str]:
    """Get first page as Data URL for LLM Multimodal."""
    if not file_path: return None

    ext = os.path.splitext(file_path)[1].lower()
    image = None

    try:
        if ext == ".pdf":
            if convert_from_path is None: return None
            images = convert_from_path(file_path, first_page=1, last_page=1, dpi=200, poppler_path=poppler_path)
            if not images: return None
            image = images[0]
            for extra in images[1:]:
                try:
                    extra.close()
                except Exception:
                    pass
        else:
            if Image is None: return None
            with Image.open(file_path) as src_image:
                image = src_image.copy()

        if image is None: return None
        if image.mode != "RGB": image = image.convert("RGB")

        w, h = image.size
        scale = min(1.0, float(max_side) / max(w, h))
        if scale < 1.0:
            image = image.resize((int(w * scale), int(h * scale)))

        buf = io.BytesIO()
        image.save(buf, format="PNG")
        b64 = base64.b64encode(buf.getvalue()).decode("ascii")
        return f"data:image/png;base64,{b64}"
    except Exception as e:
        logging.warning(f"Failed to generate image data URL: {e}")
        return None
    finally:
        if image is not None:
            try:
                image.close()
            except Exception:
                pass
