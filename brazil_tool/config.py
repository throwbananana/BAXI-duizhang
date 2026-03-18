# -*- coding: utf-8 -*-
import os
import json
import logging
import re
from PySide6.QtCore import QSettings

SETTINGS_FILE = "danfe_batch_gui_settings.ini"

class MappingManager:
    """Manage standardized mappings for Products and Partners."""
    def __init__(self, filepath="mapping_db.json"):
        self.filepath = filepath
        self.data = {"products": {}, "partners": {}}
        self.load()

    def load(self):
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, 'r', encoding='utf-8') as f:
                    loaded = json.load(f)
                    self.data["products"] = loaded.get("products", {})
                    self.data["partners"] = loaded.get("partners", {})
                self.normalize_data()
            except Exception as e:
                logging.error(f"Failed to load mappings: {e}")

    def normalize_data(self):
        clean_prods = {}
        for k, v in self.data["products"].items():
            clean_k = re.sub(r'\D+', '', k)
            if clean_k:
                clean_prods[clean_k] = v
        self.data["products"] = clean_prods
        
        clean_parts = {}
        for k, v in self.data["partners"].items():
            clean_k = self._normalize_partner_key(k)
            if clean_k:
                clean_parts[clean_k] = v
        self.data["partners"] = clean_parts

    @staticmethod
    def _normalize_partner_key(key: str) -> str:
        if key is None:
            return ""
        raw = str(key).strip()
        if not raw:
            return ""

        digits = re.sub(r'\D+', '', raw)
        # CNPJ/CPF-like keys should always be stored as digits.
        if digits and (len(digits) >= 11 or re.search(r'[.\-/]', raw)):
            return digits

        # Name-like keys use canonical spacing + upper-case for stable lookup.
        return re.sub(r'\s+', ' ', raw).strip().upper()

    def save(self):
        try:
            with open(self.filepath, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.error(f"Failed to save mappings: {e}")

    def get_product_std(self, code):
        if not code: return None
        res = self.data["products"].get(code)
        if res: return res
        norm_code = re.sub(r'\D+', '', str(code))
        return self.data["products"].get(norm_code)

    def set_product_std(self, code, std_code, std_name):
        norm_code = re.sub(r'\D+', '', str(code))
        if norm_code:
            self.data["products"][norm_code] = {"std_code": std_code, "std_name": std_name}

    def get_partner_std(self, key):
        if key is None:
            return None
        direct = self.data["partners"].get(key)
        if direct:
            return direct
        return self.data["partners"].get(self._normalize_partner_key(key))

    def set_partner_std(self, key, std_name):
        norm_key = self._normalize_partner_key(key)
        if norm_key:
            self.data["partners"][norm_key] = std_name

class TagManager:
    def __init__(self, filepath="user_tags.json"):
        self.filepath = filepath
        self.tags = []
        self.load()

    def load(self):
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, 'r', encoding='utf-8') as f:
                    self.tags = json.load(f)
            except: self.tags = []
        if not self.tags:
            self.tags = [{"name": "待确认", "color": "#FFCC00"}, {"name": "已核对", "color": "#99FF99"}]

    def save(self):
        try:
            with open(self.filepath, 'w', encoding='utf-8') as f:
                json.dump(self.tags, f, ensure_ascii=False)
        except: pass

    def get_tags(self):
        return self.tags

    def add_tag(self, name, color="#FFFFFF"):
        for t in self.tags:
            if t['name'] == name: return
        self.tags.append({"name": name, "color": color})
        self.save()

    def remove_tag(self, name):
        self.tags = [t for t in self.tags if t['name'] != name]
        self.save()

def load_settings() -> dict:
    settings = QSettings(SETTINGS_FILE, QSettings.IniFormat)
    config = {}
    config["tesseract_cmd"] = settings.value("tesseract_cmd", r"C:\\Program Files\\Tesseract-OCR\\tesseract.exe")
    config["enable_ocr"] = settings.value("enable_ocr", False, type=bool)
    config["ocr_lang"] = settings.value("ocr_lang", "por")
    config["poppler_path"] = settings.value("poppler_path", "")
    config["prefer_pymupdf"] = settings.value("prefer_pymupdf", True, type=bool)
    
    # LLM
    config["enable_llm"] = settings.value("enable_llm", False, type=bool)
    config["llm_mode"] = settings.value("llm_mode", "local")
    config["llm_endpoint"] = settings.value("llm_endpoint", "http://localhost:1234/v1")
    config["llm_model"] = settings.value("llm_model", "qwen/qwen3-vl-8b")
    config["llm_api_key"] = settings.value("llm_api_key", "")
    config["llm_use_multimodal"] = settings.value("llm_use_multimodal", True, type=bool)
    config["llm_timeout_sec"] = settings.value("llm_timeout_sec", 30, type=int)
    config["llm_max_chars"] = settings.value("llm_max_chars", 8000, type=int)
    
    return config

def check_external_tools(config: dict) -> dict:
    """Check if external tools (Tesseract, Poppler) are valid."""
    report = {"tesseract": False, "poppler": False}
    
    # Check Tesseract
    tess = config.get("tesseract_cmd")
    if tess and os.path.exists(tess):
        report["tesseract"] = True
    elif config.get("enable_ocr"):
        # Try finding in PATH or default locations?
        pass

    # Check Poppler
    # Poppler path usually points to 'bin' folder
    pop = config.get("poppler_path")
    if pop and os.path.exists(os.path.join(pop, "pdftoppm.exe")):
         report["poppler"] = True
    elif pop and os.path.exists(os.path.join(pop, "pdftoppm")):
         report["poppler"] = True

    return report
