# -*- coding: utf-8 -*-
import re
from typing import List, Dict, Optional
from brazil_tool.core.utils import br_to_float, norm_space

class BankStatementParser:
    """Parser for Brazilian Bank Statements (Itaú, Santander, etc)."""
    
    NOISE_KEYWORDS = [
        "SDO CTA/APL", "REND PAGO APLIC", "RES APLIC AUT", "SDO CTA ANT", 
        "SALDO TOTAL", "DISPONIVEL", "SALDO ANTERIOR", "SDO CTA/APL AUTOM", 
        "SALDO FINAL", "SALDO INICIAL", "SALDO DO DIA", "RESGATE AUTOM"
    ]
    
    @staticmethod
    def parse_statement(text: str) -> List[Dict]:
        lines = [l.strip() for l in text.splitlines() if l.strip()]
        transactions = []
        
        # Detect Format
        lower_text = text.lower()
        is_itau = (
            "itau" in lower_text
            or "saldo disponível" in lower_text
            or "saldo disponivel" in lower_text
        )
        
        date_re = r'\d{2,4}[/-]\d{2}[/-]\d{2,4}'
        # Money regex: allows negative sign, dots for thousands, comma for decimals, and D/C suffix or parentheses
        money_re = r'\(?-?\d{1,3}(?:\.\d{3}){0,4}(?:,\d{2})\)?\s*[DC]?'
        cnpj_re = r'\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}'

        if is_itau:
            # Itaú logic: Date -> Desc -> [multiple lines] -> Value -> Balance
            for i in range(len(lines)):
                m_date = re.match(f'^({date_re})\\s*(.*)', lines[i])
                if m_date:
                    try:
                        date = m_date.group(1)
                        desc_inline = m_date.group(2).strip()
                        amount = 0.0
                        
                        # If description is on the same line, use it. Otherwise use next line.
                        if desc_inline:
                            desc = desc_inline
                            inline_match = re.search(money_re, desc_inline)
                            if inline_match:
                                inline_amount = br_to_float(inline_match.group(0))
                                if inline_amount is not None:
                                    amount = inline_amount
                                desc = (desc_inline[:inline_match.start()] + " " + desc_inline[inline_match.end():]).strip()
                            start_search = i + 1
                        else:
                            if i + 1 >= len(lines):
                                continue
                            desc = lines[i+1]
                            start_search = i + 2
                        
                        # Noise Check
                        if any(k in desc.upper() for k in BankStatementParser.NOISE_KEYWORDS):
                            continue

                        # Look for amount in the next lines when inline amount is absent.
                        for j in range(start_search, min(i+12, len(lines))):
                            if amount != 0:
                                break
                            # Skip balance lines or total lines
                            if "SALDO" in lines[j].upper():
                                break
                                
                            val_matches = list(re.finditer(money_re, lines[j]))
                            if val_matches:
                                val = br_to_float(val_matches[-1].group(0))
                                if val is not None:
                                    amount = val
                                    break
                        
                        if amount != 0 and desc:
                            transactions.append({
                                "date": date,
                                "desc": norm_space(desc),
                                "amount": amount,
                                "cnpj": None,
                                "bank": "Itaú"
                            })
                    except Exception:
                        continue
        else:
            # Santander / General logic
            for i in range(len(lines)):
                m_date = re.match(f'^({date_re})\\s*(.*)', lines[i])
                if m_date:
                    try:
                        date = m_date.group(1)
                        desc_inline = m_date.group(2).strip()
                        amount = 0.0
                        cnpj = None
                        if desc_inline:
                            desc = desc_inline
                            inline_match = re.search(money_re, desc_inline)
                            if inline_match:
                                inline_amount = br_to_float(inline_match.group(0))
                                if inline_amount is not None:
                                    amount = inline_amount
                                desc = (desc_inline[:inline_match.start()] + " " + desc_inline[inline_match.end():]).strip()
                        else:
                            if i + 1 >= len(lines):
                                continue
                            desc = lines[i+1]
                        
                        # Look for CNPJ and amount in context
                        for j in range(i, min(i+8, len(lines))):
                            c_match = re.search(cnpj_re, lines[j])
                            if c_match: cnpj = c_match.group(0)
                            
                            if amount != 0:
                                continue

                            m_matches = list(re.finditer(money_re, lines[j]))
                            if m_matches:
                                val = br_to_float(m_matches[-1].group(0))
                                if val is not None and val != 0: 
                                    # If we found a value, we check if it's the first one we find
                                    amount = val
                        
                        # Noise Check
                        if any(k in desc.upper() for k in BankStatementParser.NOISE_KEYWORDS):
                            continue

                        # Include all non-zero transactions for reconciliation
                        if amount != 0 and desc:
                            transactions.append({
                                "date": date,
                                "desc": norm_space(desc),
                                "amount": amount,
                                "cnpj": cnpj,
                                "bank": "General/Santander"
                            })
                    except Exception:
                        continue
                    
        return transactions
