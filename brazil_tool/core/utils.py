# -*- coding: utf-8 -*-
import re
import unicodedata
from typing import Optional
from brazil_tool.constants import CNPJ_RE, CPF_RE

def br_to_float(s: Optional[str]) -> Optional[float]:
    """Convert currency format to float, handling BR/US styles, D/C suffixes and parentheses."""
    if s is None: return None
    s = str(s).strip().upper()
    # Remove currency symbols and spaces
    s = re.sub(r'[R\$\s]', '', s)
    if not s: return None
    
    # Handle parentheses: (1.234,56) -> -1.234,56
    is_negative = False
    if s.startswith('(') and s.endswith(')'):
        is_negative = True
        s = s[1:-1]
    
    # Handle D/C suffixes
    if s.endswith('D'):
        is_negative = True
        s = s[:-1].strip()
    elif s.endswith('C'):
        s = s[:-1].strip()
        
    if 'E' not in s:
        dot_count = s.count('.')
        comma_count = s.count(',')
        
        if dot_count > 0 and comma_count > 0:
            # Both exist. Last one is decimal.
            if s.rfind('.') > s.rfind(','):
                # US style: 1,234.56 or OCR swap
                s = s.replace(',', '')
            else:
                # BR style: 1.234,56
                s = s.replace('.', '').replace(',', '.')
        elif comma_count > 0:
            # Only comma(s) exist. BR standard: 1234,56 or 1.234,56 (where dot was missing)
            if comma_count > 1:
                last_comma = s.rfind(',')
                s = s[:last_comma].replace(',', '') + '.' + s[last_comma+1:]
            else:
                s = s.replace(',', '.')
        elif dot_count > 0:
            # Only dot(s) exist. 
            if dot_count > 1:
                # 1.234.567 -> Thousand separators
                s = s.replace('.', '')
            else:
                # Single dot: 1.234 or 1234.56
                # Heuristic: if exactly 3 digits after, likely thousand separator in BR context.
                # Otherwise likely decimal.
                parts = s.split('.')
                if len(parts[-1]) == 3:
                    s = s.replace('.', '')
                else:
                    pass
    
    # 拒绝对过长的数字字符串进行转换 (超过 15 位数字通常不是正常的发票金额)
    clean_digits = re.sub(r'\D', '', s)
    if len(clean_digits) > 18: # Relaxed slightly for long IDs
        return None

    try:
        val = float(s)
        # 增加极致安全检查：如果数值过大，返回 None 或 0
        if abs(val) > 1e14: return None
        if is_negative: val = -val
        return val
    except Exception:
        return None

def norm_space(s: str) -> str:
    """Normalize whitespace to single space and strip."""
    return re.sub(r'\s+', ' ', s).strip()

def only_digits(s: str) -> str:
    """Keep only digits."""
    return re.sub(r'\D+', '', s or '')

def strip_cnpj_cpf(s: Optional[str]) -> Optional[str]:
    """Remove CNPJ/CPF labels and values from string."""
    if not s: return s
    out = s
    out = re.sub(r'(?i)\bCNPJ\b\s*[:\-]?\s*' + CNPJ_RE, '', out)
    out = re.sub(r'(?i)\bCPF\b\s*[:\-]?\s*' + CPF_RE, '', out)
    # 14/11 digits (OCR/PDF extraction artifacts)
    out = re.sub(r'\b\d{14}\b', '', out)  # CNPJ 14
    out = re.sub(r'\b\d{11}\b', '', out)  # CPF 11
    # Clean separators
    out = re.sub(r'[,\.;:\-]\s*$', '', out).strip()
    out = re.sub(r'\s{2,}', ' ', out)
    # Remove trailing CNPJ/CPF labels that sometimes stick to names in PDF extraction
    out = re.sub(r'(?i)\b(?:CNPJ\s*/\s*CPF|CNPJ|CPF)\s*$', '', out)
    return out.strip(" -/")

def fix_ocr_text(text: str) -> str:
    """Fix common Portuguese OCR errors."""
    if not text: return text
    
    replacements = {
        "aO": "ÃO",
        "caO": "ÇÃO",
        "coes": "ÇÕES",
        "cOES": "ÇÕES",
        "PORTaTIL": "PORTÁTIL",
        "PORTATIL": "PORTÁTIL",
        "PEcAS": "PEÇAS",
        "PECAS": "PEÇAS",
        "REPOSIcaO": "REPOSIÇÃO",
        "REPOSICAO": "REPOSIÇÃO",
        "GaS": "GÁS",
        "ELETRICA": "ELÉTRICA",
        "MAQUINAS": "MÁQUINAS",
        "SAO PAULO": "SÃO PAULO",
        "DEVOLUCAO": "DEVOLUÇÃO"
    }
    
    out = text
    for bad, good in replacements.items():
        out = out.replace(bad, good)
        
    return out

def get_after_label(label_pat: str, value_pat: str, text: str, max_span: int = 220) -> Optional[str]:
    """Find value after a label pattern."""
    flags = re.I | re.S
    for m in re.finditer(label_pat, text, flags):
        start = m.end()
        window = text[start:start+max_span]
        m2 = re.search(value_pat, window, flags)
        if m2 and m2.groups():
            return m2.group(1).strip()
    return None

def extract_block(text: str, start_pat: str, end_pat: str, max_chars: int = 8000) -> Optional[str]:
    """Extract text block between start and end patterns."""
    flags = re.I | re.S
    ms = re.search(start_pat, text, flags)
    if not ms: return None
    start = ms.end()
    me = re.search(end_pat, text[start:start+max_chars], flags)
    if me:
        return text[start:start+me.start()]
    return text[start:start+max_chars]

def calculate_similarity(s1: str, s2: str) -> float:
    """Calculate similarity ratio between two strings (0.0 to 1.0) with business name normalization."""
    import difflib
    if not s1 or not s2: return 0.0
    s1, s2 = str(s1).strip().lower(), str(s2).strip().lower()
    if s1 == s2: return 1.0
    
    # Business name normalization
    def normalize_business_name(s):
        # Remove common suffixes and keywords
        suffixes = [
            r'\bLTDA\b', r'\bME\b', r'\bEPP\b', r'\bS\.?A\.?\b', r'\bEIRELI\b',
            r'\bCOMERCIO\b', r'\bCOM\b', r'\bDE\b', r'\bFERRAMENTAS\b', r'\bFERRAME\b',
            r'\bMAQUINAS\b', r'\bMAQ\b', r'\bEQUIPAMENTOS\b', r'\bSERVICOS\b', r'\bSERV\b',
            r'\bIMPORTACAO\b', r'\bEXPORTACAO\b', r'\bDISTRIBUIDORA\b', r'\bINDUSTRIA\b'
        ]
        res = s.upper()
        for suf in suffixes:
            res = re.sub(suf, '', res)
        # Keep only alphanumeric
        res = re.sub(r'[^A-Z0-9]', '', res)
        return res

    n1 = normalize_business_name(s1)
    n2 = normalize_business_name(s2)
    
    if n1 and n2 and n1 == n2: return 1.0
    if n1 and n2 and (n1 in n2 or n2 in n1):
        if len(n1) > 3 and len(n2) > 3: return 0.9
    
    return difflib.SequenceMatcher(None, s1, s2).ratio()
