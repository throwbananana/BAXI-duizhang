# -*- coding: utf-8 -*-
import re

# --- Regex Patterns ---
# 限制千分位最多重复 4 次 (即支持到十亿级别)，防止匹配过长的流水号
MONEY_RE = r'(?:\d{1,3}(?:\.\d{3}){0,4}\,\d{2}|\d{1,12}\.\d{2}|\d{1,12},\d{2})'
CNPJ_RE  = r'\d{2}\.\d{3}\.\d{3}\/\d{4}\-\d{2}'
CPF_RE   = r'\d{3}\.\d{3}\.\d{3}\-\d{2}'
DIGITS44 = r'\b\d{44}\b'
ACCESS_KEY_RE = r'(?:\D|^)(\d[\d\s]{40,60}\d)(?:\D|$)'
PHONE_RE = r'(?:\(?\d{2}\)?\s*\d{4,5}\-?\d{4})'
CEP_RE = r'\b\d{5}\-?\d{3}\b'
