# -*- coding: utf-8 -*-
import re
import unicodedata
from typing import List, Dict, Optional
from brazil_tool.core.utils import br_to_float, norm_space

class CollectionReportParser:
    """Parser for 'BAIXADOS E LIQUIDADOS' and 'VENCIDOS' PDF reports."""
    
    @staticmethod
    def parse_report(text: str) -> List[Dict]:
        """
        Analyze text and return a list of transaction records.
        """
        lines = [l.strip() for l in text.splitlines() if l.strip()]
        records = []
        
        is_baixados = "Liquida" in text or "Baixa" in text
        is_vencidos = "VENCIDO" in text and not is_baixados
        
        date_re = r'\d{2,4}[/-]\d{2}[/-]\d{2,4}'
        money_re = r'-?\d{1,3}(?:\.\d{3}){0,4}(?:,\d{2})'
        
        if is_baixados:
            for i in range(len(lines)):
                ln = lines[i]
                if re.match(date_re, ln):
                    try:
                        venc = ln
                        baixa = lines[i+1] if i+1 < len(lines) and re.match(date_re, lines[i+1]) else None
                        
                        if baixa:
                            valor_str = ""
                            for j in range(i+2, i+5):
                                if j < len(lines) and re.search(money_re, lines[j]):
                                    valor_str = re.search(money_re, lines[j]).group(0)
                                    break
                            
                            ref = ""
                            # First pass: look for strong indicators (Seu Numero)
                            for j in range(i+2, i+8):
                                if j < len(lines):
                                    # Regular pattern: digits + Letters + digits OR 'NF' OR long digits
                                    if re.search(r'\d+\s*[A-Z]\s*\d+', lines[j]) or 'NF' in lines[j].upper() or re.search(r'^\d{4,10}$', lines[j]):
                                        ref = lines[j]
                                        break
                            
                            if not ref:
                                for j in range(i+3, i+6):
                                    if j < len(lines):
                                        ln_j = lines[j]
                                        if not re.match(date_re, ln_j) and not re.search(money_re, ln_j) and len(ln_j) > 2:
                                            ref = ln_j
                                            break
                            
                            records.append({
                                "name": norm_space(lines[i-1]) if i > 0 else "Unknown",
                                "due_date": venc,
                                "pay_date": baixa,
                                "amount": br_to_float(valor_str),
                                "invoice_ref": ref,
                                "status": "Paid"
                            })
                    except: continue
        
        elif is_vencidos:
            for i in range(len(lines)):
                ln = lines[i]
                if re.search(date_re, ln):
                    try:
                        venc = re.search(date_re, ln).group(0)
                        amt_match = re.search(money_re, ln)
                        valor_str = amt_match.group(0) if amt_match else ""
                        if not valor_str and i+1 < len(lines):
                            amt_match = re.search(money_re, lines[i+1])
                            if amt_match: valor_str = amt_match.group(0)
                        
                        ref = ""
                        for j in range(i+1, i+6):
                            if j < len(lines):
                                if re.search(r'\d+\s*[A-Z]\s*\d+', lines[j]) or 'NF' in lines[j].upper() or re.search(r'^\d{4,10}$', lines[j]):
                                    ref = lines[j]
                                    break
                        
                        if not ref:
                            for j in range(i+1, i+6):
                                if j < len(lines):
                                    if re.search(r'\d{4,}', lines[j]):
                                        txt_val = lines[j].strip()
                                        if txt_val.startswith('000') and len(txt_val) >= 9:
                                            continue
                                        ref = lines[j]
                                        break
                        
                        if not ref:
                            for j in range(i+1, i+4):
                                if j < len(lines):
                                    ln_j = lines[j]
                                    if not re.match(date_re, ln_j) and not re.search(money_re, ln_j) and len(ln_j) > 2:
                                        ref = ln_j
                                        break
                        
                        records.append({
                            "name": norm_space(lines[i-1]) if i > 0 else "Unknown",
                            "due_date": venc,
                            "pay_date": None,
                            "amount": br_to_float(valor_str),
                            "invoice_ref": ref,
                            "status": "Vencido"
                        })
                    except: continue
                    
        return records

    @staticmethod
    def clean_invoice_number(ref: str, enable_local_rules: bool = True) -> Optional[str]:
        """
        Convert references like '2570NF2260' or '2814A0910' to numeric.
        """
        if not ref: return None
        
        # Clean potential weird spaces (unicode spaces, etc.)
        ref = "".join(ref.split()) # Remove all whitespace for primary match
        # But wait, splitting might merge digits. Let's use standard normalization.
        ref = unicodedata.normalize('NFKD', ref).encode('ascii', 'ignore').decode('ascii').upper()

        if enable_local_rules:
            # 1. Enhanced Rule: prefer digits BEFORE NF or A-Z letter (Handles 1614NF1526 -> 1614)
            m_before = re.search(r'(\d{3,})(?=NF|[A-Z])', ref, re.I)
            if m_before:
                return m_before.group(1)

            # 2. Fallback: digits AFTER "NF"
            m_nf = re.search(r'NF(\d+)', ref, re.I)
            if m_nf:
                return m_nf.group(1)

        # 3. Priority 2: Match any group of 3-9 digits
        m = re.search(r'(\d{3,9})', ref)
        if m:
            return m.group(1)
        digits_only = re.sub(r'\D+', '', ref)
        if len(digits_only) >= 3:
            return digits_only
        return None

    @staticmethod
    def parse_invoice_reference(ref: str, enable_local_rules: bool = True) -> Dict:
        """
        Parse complex reference like '2060A1710'
        """
        if not ref or not enable_local_rules: return {}
        
        # Normalize
        ref = unicodedata.normalize('NFKD', ref).encode('ascii', 'ignore').decode('ascii').upper()
        # Keep spaces for the flexible regexes
        ref_norm = " ".join(ref.split())

        # 1. Handle DDMM style: digits + one letter + 4 digits (DDMM)
        # Flexible spaces: "2060 A 1710"
        m_ddmm = re.search(r'(\d+)\s*([A-Z])\s*(\d{2})(\d{2})', ref_norm, re.I)
        if m_ddmm:
            invoice = m_ddmm.group(1)
            letter = m_ddmm.group(2).upper()
            day = int(m_ddmm.group(3))
            month = int(m_ddmm.group(4))
            term_number = ord(letter) - ord('A') + 1
            if 1 <= month <= 12 and 1 <= day <= 31:
                return {
                    "invoice": invoice,
                    "term_number": term_number,
                    "day": day,
                    "month": month
                }

        # 2. Handle NF style with potential prefix: 1614NF1526
        m_prefix_nf = re.search(r'(\d+)\s*NF\s*(\d*)', ref_norm, re.I)
        if m_prefix_nf:
            return {
                "invoice": m_prefix_nf.group(1),
                "order_number": m_prefix_nf.group(2),
                "term_number": 1,
                "is_one_time": True
            }

        # 3. Simple NF style: NF1234
        m_nf = re.search(r'NF\s*(\d+)', ref_norm, re.I)
        if m_nf:
            return {
                "invoice": m_nf.group(1),
                "term_number": 1,
                "is_one_time": True
            }

        return {}
