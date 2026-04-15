# -*- coding: utf-8 -*-
import re
import unicodedata
from typing import Optional, List, Tuple
from brazil_tool.constants import MONEY_RE, CNPJ_RE, CPF_RE, ACCESS_KEY_RE, DIGITS44, PHONE_RE, CEP_RE
from brazil_tool.core.models import Invoice, Item, PaymentEntry
from brazil_tool.core.utils import (
    br_to_float, norm_space, only_digits, strip_cnpj_cpf,
    fix_ocr_text, get_after_label, extract_block
)

DEST_BLOCK_START = r'DESTINAT[ÁA]RIO\s*/\s*REMETENTE|DESTINATARIO\s*/\s*REMETENTE'
PRODUCT_BLOCK_START = r'DADOS\s+DO(?:S)?\s+PRODUTO(?:S)?\s*/\s*SERVI[CÇ]O(?:S)?'
PRODUCT_BLOCK_END = r'INFORMA[cç][OÕ]ES\s+COMPLEMENTARES|DADOS\s+ADICIONAIS|Reservado\s+ao\s+Fisco|CALCULO\s+DO\s+IMPOSTO|C[ÁA]LCULO\s+DO\s+IMPOSTO|$'
TRANSPORT_BLOCK_START = r'TRANSPORTADOR\s*/\s*VOLUMES\s+TRANSPORTADOS'
ALNUM_PRODUCT_CODE_RE = r'[A-Z0-9][A-Z0-9\-./]{2,}'
SHOPEE_PEDIDO_RE = re.compile(r'\b\d{6}[A-Z0-9]{8}\b', re.I)
MERCADO_LIVRE_ORDER_RE = re.compile(r'\b20\d{14}\b')
XPED_LABEL_RE = re.compile(r'(?i)\bxPed\b[\s:：\(\)\-\/]*([A-Z0-9][A-Z0-9\-]{5,19})')
PEDIDO_LABEL_RE = re.compile(r'(?i)\bPedido\b[\s:：\(\)\-\/]*([A-Z0-9][A-Z0-9\-]{5,19})')
PEDIDO_INLINE_RE = re.compile(r'(?i)\(?\bPedido\b[\s:：]*[A-Z0-9][A-Z0-9\-]{5,19}\)?')


BAD_FIELD_VALUES = {
    "HORA", "UF", "CEP", "FONE", "FONE/FAX", "FONE / FAX",
    "CNPJ/CPF", "CNPJ / CPF", "ENDERECO", "ENDEREÇO",
    "MUNICIPIO", "MUNICÍPIO", "INSCRICAO ESTADUAL", "INSCRIÇÃO ESTADUAL",
    "DATA DA EMISSAO", "DATA DA SAIDA", "DATA DA SAÍDA/ENTRADA",
    "FRETE", "CÓDIGO ANTT", "CODIGO ANTT", "PLACA DO VEÍCULO", "PLACA DO VEICULO",
    "PESO BRUTO", "PESO LÍQUIDO", "PESO LIQUIDO", "QUANTIDADE", "ESPÉCIE",
    "ESPECIE", "MARCA", "NUMERAÇÃO", "NUMERACAO"
}


def normalize_marketplace_order_token(token: Optional[str]) -> Optional[str]:
    if not token:
        return None
    cleaned = re.sub(r'[^A-Z0-9]', '', str(token).upper())
    return cleaned or None


def classify_marketplace_order(token: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    normalized = normalize_marketplace_order_token(token)
    if not normalized:
        return None, None
    if SHOPEE_PEDIDO_RE.fullmatch(normalized) and any(ch.isalpha() for ch in normalized[6:]):
        return "Shopee", normalized
    if MERCADO_LIVRE_ORDER_RE.fullmatch(normalized):
        return "Mercado Livre", normalized
    return None, None


def is_marketplace_order_token(token: Optional[str]) -> bool:
    platform, order_number = classify_marketplace_order(token)
    return bool(platform and order_number)


def format_invoice_number(raw_digits: Optional[str]) -> Optional[str]:
    digits = only_digits(raw_digits or "")
    if len(digits) != 9:
        return None
    return f"{digits[:3]}.{digits[3:6]}.{digits[6:]}"


def derive_invoice_fields_from_access_key(access_key: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    digits = only_digits(access_key or "")
    if len(digits) != 44:
        return None, None
    serie = digits[22:25]
    numero = format_invoice_number(digits[25:34])
    return numero, serie


def extract_access_key_candidate(text: str) -> Optional[str]:
    if not text:
        return None

    candidates = []

    label_match = re.search(r'Chave\s+de\s+Acesso', text, re.I)
    if label_match:
        segment = text[label_match.end():label_match.end() + 800]
        digits_segment = only_digits(segment)
        for start in range(0, max(0, len(digits_segment) - 43)):
            candidate = digits_segment[start:start + 44]
            if len(candidate) == 44:
                candidates.append(candidate)

    for match in re.finditer(DIGITS44, text):
        candidates.append(match.group(0))

    seen = set()
    ordered = []
    for candidate in candidates:
        if candidate not in seen:
            seen.add(candidate)
            ordered.append(candidate)

    for candidate in ordered:
        if len(candidate) == 44 and candidate[20:22] == "55":
            return candidate

    for candidate in ordered:
        if len(candidate) == 44:
            return candidate

    return None


def extract_marketplace_order_info(text: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not text:
        return None, None, None

    platform = None
    pedido = None
    order_number = None
    text_upper = text.upper()
    labeled_candidates = []

    for regex in (XPED_LABEL_RE, PEDIDO_LABEL_RE):
        for match in regex.finditer(text):
            candidate = normalize_marketplace_order_token(match.group(1))
            if candidate:
                labeled_candidates.append(candidate)

    best_classified = None
    for candidate in labeled_candidates:
        platform_candidate, order_candidate = classify_marketplace_order(candidate)
        if not (platform_candidate and order_candidate):
            continue
        if best_classified is None or len(order_candidate) > len(best_classified[2]):
            best_classified = (platform_candidate, candidate, order_candidate)

    if best_classified:
        platform, pedido, order_number = best_classified

    if pedido is None and labeled_candidates:
        pedido = max(labeled_candidates, key=len)

    if order_number is None:
        shopee_match = SHOPEE_PEDIDO_RE.search(text_upper)
        if shopee_match:
            token = normalize_marketplace_order_token(shopee_match.group(0))
            platform_candidate, order_candidate = classify_marketplace_order(token)
            if platform_candidate and order_candidate:
                platform = platform or platform_candidate
                pedido = pedido or token
                order_number = order_candidate

    if order_number is None:
        ml_match = MERCADO_LIVRE_ORDER_RE.search(text_upper)
        if ml_match:
            token = normalize_marketplace_order_token(ml_match.group(0))
            platform = platform or "Mercado Livre"
            order_number = token
            if pedido and normalize_marketplace_order_token(pedido) == token:
                pedido = token

    if platform is None:
        if "SHOPEE" in text_upper:
            platform = "Shopee"
        elif any(term in text_upper for term in ("MERCADO LIVRE", "MERCADOLIVRE", "MELI")):
            platform = "Mercado Livre"

    if pedido and order_number is None:
        order_number = pedido

    return platform, pedido, order_number


def clean_inline_value(v: Optional[str]) -> Optional[str]:
    if not v:
        return None
    v = norm_space(v).strip(" :;-")
    if not v:
        return None
    if v.upper() in BAD_FIELD_VALUES:
        return None
    if re.match(r'^\d+\s*-\s*', v):
        return None
    return v


def looks_like_ie(v: Optional[str]) -> bool:
    if not v:
        return False
    u = v.upper().strip()
    if u in BAD_FIELD_VALUES:
        return False
    return bool(re.search(r"\d", u))


def next_valid_value_after_label(block: str, label_regex: str, max_lookahead: int = 4) -> Optional[str]:
    lines = [norm_space(x) for x in block.splitlines() if norm_space(x)]
    for i, ln in enumerate(lines):
        if re.search(label_regex, ln, re.I):
            for cand in lines[i + 1:i + 1 + max_lookahead]:
                cand = clean_inline_value(cand)
                if cand:
                    return cand
    return None


def translate_natureza(natureza: str) -> str:
    """Translate Natureza da Operação to Chinese category."""
    if not natureza:
        return "未知"

    nat = natureza.upper()

    if "DEVOLU" in nat: return "退货 (Devolução)"
    if "RETORNO" in nat: return "返还 (Retorno)"

    if "VENDA" in nat: return "销售 (Venda)"
    if "COMPRA" in nat: return "购买 (Compra)"
    if "PRESTACAO DE SERVICO" in nat: return "服务 (Serviço)"

    if "REMESSA" in nat:
        if "CONSERTO" in nat: return "送修 (Conserto)"
        if "DEPOSITO" in nat: return "仓储 (Depósito)"
        if "AMOSTRA" in nat: return "样品 (Amostra)"
        if "BRINDE" in nat: return "赠品 (Brinde)"
        return "运送 (Remessa)"

    if "TRANSFERENCIA" in nat: return "转运 (Transferência)"
    if "SIMPLES REMESSA" in nat: return "简单运送 (Simples Remessa)"

    return "其他 (Outros)"


def detect_natureza_by_keywords(text: str) -> Optional[str]:
    """Infer Natureza by searching for keywords in the header."""
    header_text = text[:2000]

    phrases = [
        "DEVOLUCAO DE VENDA", "REMESSA PARA CONSERTO", "RETORNO DE CONSERTO",
        "REMESSA POR CONTA E ORDEM", "REMESSA P/ DEPOSITO", "SAIDA DE RETORNO",
        "VENDA DE MERCADORIA", "VENDA", "COMPRA PARA INDUSTRIALIZACAO",
        "REMESSA SAIDA P/ DEPOSITO FECHADO", "SAIDA DE RETORNO RECEBIDO PARA CONSERTO",
        "DEVOLUCAO DE COMPRA"
    ]

    for phrase in phrases:
        if re.search(r'(^|\n)\s*' + re.escape(phrase) + r'\s*($|\n)', header_text, re.I):
            return phrase

    lines = header_text.splitlines()
    for ln in lines:
        ln = ln.strip()
        if not ln or len(ln) < 3: continue
        if "NATUREZA" in ln.upper(): continue

        if re.match(r'^(VENDA|REMESSA|DEVOLUCAO|RETORNO|TRANSFERENCIA|SAIDA|ENTRADA)', ln, re.I):
            if "PROTOCOLO" in ln.upper() or "AUTORIZACAO" in ln.upper():
                continue
            if len(ln) < 80:
                return ln
    return None


def first_company_like_after(header_block: str, full_text: str) -> Optional[str]:
    """Find first company-like name in DESTINATÁRIO block."""
    if not header_block: return None
    flags = re.I | re.S
    idx = full_text.find(header_block)
    if idx < 0:
        m = re.search(DEST_BLOCK_START, full_text, flags)
        idx = m.start() if m else -1
    if idx < 0: return None

    segment = full_text[idx:idx+600]
    lines = [norm_space(x) for x in segment.splitlines() if norm_space(x)]

    blacklist = re.compile(r'(?i)(CNPJ|CPF|Data|Emiss|Endere|Bairro|Munic|UF\b|CEP|IE\b|Fone|Telefone)')

    for ln in lines[1:10]:
        if blacklist.search(ln): continue
        if re.search(CNPJ_RE, ln) or re.search(CPF_RE, ln) or re.search(r'\b\d{11}\b', ln) or re.search(r'\b\d{14}\b', ln):
            continue
        if re.match(r'[A-Za-zÁÉÍÓÚÃÕÂÊÔÇ0-9][A-Za-zÁÉÍÓÚÃÕÂÊÔÇ0-9 &\.,\-]{2,}', ln):
            return strip_cnpj_cpf(ln)
    return None


def parse_items_danfe_block(block: str) -> List[Item]:
    lines = [norm_space(x) for x in block.splitlines() if norm_space(x)]
    items: List[Item] = []
    i = 0

    def is_noise_line(s: str) -> bool:
        s_up = s.upper()
        normalized = normalize_marketplace_order_token(s_up.rstrip(')'))
        return (
            s_up.startswith('XPED:') or
            'TOTAL APROXIMADO DE TRIBUTOS' in s_up or
            'FEDERAIS, ESTADUAIS E MUNICIPAIS' in s_up or
            'DESCRIÇÃO DO PRODUTO' in s_up or
            'CÓDIGO PRODUTO' in s_up or
            'NCM/SH' in s_up or
            'VALOR UNIT' in s_up or
            s_up.startswith('PFCPUFDEST=') or
            s_up.startswith('PICMS') or
            s_up.startswith('VICMS') or
            s_up.startswith('PREDBC=') or
            'PEDIDO' in s_up or
            re.fullmatch(r'\(?PEDIDO(?:\s*:\s*)?[A-Z0-9]+\)?', s_up) is not None or
            (normalized is not None and is_marketplace_order_token(normalized)) or
            re.fullmatch(r'\d+\)?', s_up) is not None
        )

    while i < len(lines):
        ln = lines[i]
        m = re.match(r'^(' + ALNUM_PRODUCT_CODE_RE + r')(?:\s+(.+))?$', ln)
        if not m or 'CÓDIGO PRODUTO' in ln.upper() or 'CODIGO PRODUTO' in ln.upper():
            i += 1
            continue

        code_token = m.group(1)
        if not re.search(r'\d', code_token):
            i += 1
            continue

        item = Item()
        item.codigo_produto = code_token
        first_desc = m.group(2)
        desc_parts = [first_desc] if first_desc else []
        i += 1

        while i < len(lines) and not re.match(r'^\d{8}\b', lines[i]):
            current_line = lines[i]
            if not desc_parts and is_noise_line(current_line):
                i += 1
                continue
            if not is_noise_line(current_line):
                desc_parts.append(current_line)
            i += 1

        if i >= len(lines):
            break

        row = lines[i]
        m_row = re.match(r'^(?P<ncm>\d{8})\s+(?P<cst>\d{3})\s+(?P<cfop>\d{4})\s+(?P<unidade>[A-Z]+)\s+(?P<rest>.+)$', row)
        if m_row:
            item.ncm = m_row.group('ncm')
            item.cst = m_row.group('cst')
            item.cfop = m_row.group('cfop')
            item.unidade = m_row.group('unidade')
            values = re.findall(MONEY_RE, m_row.group('rest'))
            if len(values) >= 6:
                item.quantidade = br_to_float(values[0])
                item.valor_unitario = br_to_float(values[1])
                item.valor_total = br_to_float(values[2])
                item.desconto = br_to_float(values[3])
                item.bc_icms = br_to_float(values[4])
                item.valor_icms = br_to_float(values[5])
                trailing = values[6:]
                if len(trailing) == 1:
                    item.aliquota_icms = br_to_float(trailing[0])
                elif len(trailing) >= 2:
                    item.valor_ipi = br_to_float(trailing[0])
                    item.aliquota_icms = br_to_float(trailing[1])
                    if len(trailing) >= 3:
                        item.aliquota_ipi = br_to_float(trailing[2])
                item.descricao = fix_ocr_text(' '.join(desc_parts))
                items.append(item)
                i += 1
                continue

        # fallback for layouts where each field is broken into separate lines
        item.ncm = lines[i]
        i += 1
        if i < len(lines): item.cst = lines[i]; i += 1
        if i < len(lines): item.cfop = lines[i]; i += 1
        if i < len(lines): item.unidade = lines[i]; i += 1
        if i < len(lines): item.quantidade = br_to_float(lines[i]); i += 1
        if i < len(lines): item.valor_unitario = br_to_float(lines[i]); i += 1
        if i < len(lines): item.valor_total = br_to_float(lines[i]); i += 1
        if i < len(lines): item.desconto = br_to_float(lines[i]); i += 1
        if i < len(lines): item.bc_icms = br_to_float(lines[i]); i += 1
        if i < len(lines): item.valor_icms = br_to_float(lines[i]); i += 1

        trailing = []
        while i < len(lines) and re.fullmatch(r'[\d\., ]+', lines[i]):
            trailing.append(lines[i].replace(' ', ''))
            i += 1

        if len(trailing) == 1:
            item.aliquota_icms = br_to_float(trailing[0])
        elif len(trailing) >= 2:
            item.valor_ipi = br_to_float(trailing[0])
            item.aliquota_icms = br_to_float(trailing[1])
            if len(trailing) >= 3:
                item.aliquota_ipi = br_to_float(trailing[2])

        item.descricao = fix_ocr_text(' '.join(desc_parts))
        items.append(item)

    return items


def parse_mashed_items(block: str) -> List[Item]:
    """Robust extraction for items in mashed text (e.g. PyPDF2 output)."""
    full_text = " ".join([norm_space(x) for x in block.splitlines() if x.strip()])
    items: List[Item] = []

    # Anchor pattern: NCM(8) + Space + CST(3)CFOP(4) + UN(1-4) + QTY
    # Example: 83119000 2006108un1,0000
    pattern = r'(\d{8})\s+(\d{3})\s*(\d{4})\s*([a-zA-Z]{1,4})\s*(\d+[\d\.,]*)'
    matches = list(re.finditer(pattern, full_text))

    for i, m in enumerate(matches):
        it = Item()
        it.ncm = m.group(1)
        it.cst = m.group(2)
        it.cfop = m.group(3)
        it.unidade = m.group(4)
        it.quantidade = br_to_float(m.group(5))

        # 1. Extraction of Head (Description and Product Code)
        start_idx = matches[i-1].end() if i > 0 else 0
        # If there are money values after previous match, the head starts after them
        if i > 0:
            prev_tail = full_text[matches[i-1].end():m.start()]
            money_m = list(re.finditer(MONEY_RE, prev_tail))
            if money_m:
                start_idx = matches[i-1].end() + money_m[-1].end()

        head = full_text[start_idx:m.start()].strip()

        # Clean head of noise
        head = re.sub(r'(?i)(?:pFCPUFDest|pICMSUFDest|pICMSInterPart|vFCPUFDest|vICMSUFDest|vICMSUFRemet|vTotTrib)\s*=\s*[\d\.,%]*', '', head)
        head = PEDIDO_INLINE_RE.sub('', head)
        # Clean common header labels more aggressively (including mashed ones)
        header_terms = [
            r'C[OÓ]DIGO\s+PRODUTO', r'DESCRI[CÇ][AÃ]O\s+DO\s+PRODUTO\s*/\s*SERVI[CÇ]O',
            r'NCM/SH', r'O/CST', r'CFOP', r'UNQUANTVALOR', r'UNITVALOR', r'TOTALVALOR', r'DESCB\.C[ÁA]LC',
            r'ICMSVALOR', r'IPIAL[ÍI]Q\.', r'ICMSAL[ÍI]Q\.', r'AL[ÍI]Q\.',
            r'UN', r'QUANT', r'VALOR', r'UNIT', r'TOTAL', r'ICMS', r'IPI', r'B\.C[ÁA]LC'
        ]
        for term in header_terms:
            head = re.sub(r'(?i)' + term, '', head)
        
        head = head.strip(" ,-)")

        # Extract code from head (usually first word)
        # We look for the first word that isn't empty and has some length
        words = [w for w in head.split() if len(w) >= 3]
        if words:
            it.codigo_produto = words[0]
            it.descricao = fix_ocr_text(head[head.find(words[0]) + len(words[0]):].strip(" ,-/"))
        else:
            it.descricao = fix_ocr_text(head)

        # 2. Extraction of Money Values (after the anchor)
        next_start = matches[i+1].start() if i + 1 < len(matches) else len(full_text)
        tail = full_text[m.end():next_start]
        values = re.findall(MONEY_RE, tail)

        if len(values) >= 2:
            it.valor_unitario = br_to_float(values[0])
            it.valor_total = br_to_float(values[1])
            # Additional fields if available
            v_idx = 2
            if len(values) > v_idx: it.desconto = br_to_float(values[v_idx]); v_idx += 1
            if len(values) > v_idx: it.bc_icms = br_to_float(values[v_idx]); v_idx += 1
            if len(values) > v_idx: it.valor_icms = br_to_float(values[v_idx]); v_idx += 1
            if len(values) > v_idx: it.valor_ipi = br_to_float(values[v_idx]); v_idx += 1

        items.append(it)

    return items


def parse_invoice_from_text(text: str, file_name: str) -> Invoice:
    """Core logic to parse Invoice object from text."""
    inv = Invoice(file_name=file_name)

    if re.search(r'\bCANCELADO\b', text, re.I):
        inv.status = "Cancelled"

    if len(text.strip()) < 30:
        inv.natureza_operacao = "(提示) 该 PDF 可能是扫描件，需 OCR 才能识别"
        return inv

    inv.numero = get_after_label(r'\b(?:N[º°]\.?)\s*(?:Nota\s*)?:?', r'(\d{1,3}(?:\.\d{3}){0,2}|\d{1,9})', text, 80) or \
                 get_after_label(r'\bNro\.\s*(?:Nota\s*)?:?', r'(\d{1,3}(?:\.\d{3}){0,2}|\d{1,9})', text, 80)
    inv.serie = get_after_label(r'S[eé]rie:?', r'(\b[0-9]{1,3}\b)', text, 250) or \
                get_after_label(r'S[eé]rie:?', r'\b([A-Za-z0-9]{1,5})\b', text, 250)

    raw_natureza = get_after_label(r'Natureza\s+de\s+Opera[cç][aã]o', r'((?!Protocolo)[A-ZÁÉÍÓÚÃÕÂÊÔÇ][A-ZÁÉÍÓÚÃÕÂÊÔÇa-zÁéíóúãõâêôç0-9 \-/]{2,80}?)[\n\r\s]*(?:PROTOCOLO|AUTORIZACAO|INSCRI|CNPJ|CPF|DATA|$)', text, 220)

    if not raw_natureza:
         # even more relaxed fallback for Natureza
         m_nat = re.search(r'NATUREZA\s+DA\s+OPERA[ÇC][ÃA]O\s*(?:\n|\r\n)?\s*(.+?)\s*(?:PROTOCOLO|AUTORIZACAO|$)', text, re.I | re.S)
         if m_nat:
             raw_natureza = m_nat.group(1).strip()

    is_bad = False
    if not raw_natureza:
        is_bad = True
    else:
        bad_keywords = ["PROTOCOLO", "AUTORIZACAO", "INSCRI", "CNPJ", "CPF", "DATA"]
        if any(bk in raw_natureza.upper() for bk in bad_keywords):
            is_bad = True

    if is_bad:
        fallback = detect_natureza_by_keywords(text)
        if fallback:
            raw_natureza = fallback

    if raw_natureza:
        clean_natureza = fix_ocr_text(raw_natureza)
        cn_label = translate_natureza(clean_natureza)
        inv.natureza_operacao = f"{cn_label} | {clean_natureza}"
    else:
        inv.natureza_operacao = "未知 (Desconhecido)"

    inv.chave_acesso = extract_access_key_candidate(text)

    derived_numero, derived_serie = derive_invoice_fields_from_access_key(inv.chave_acesso)
    if not inv.numero and derived_numero:
        inv.numero = derived_numero
    serie_digits = only_digits(inv.serie or "")
    if (not inv.serie or len(serie_digits) != 3) and derived_serie:
        inv.serie = derived_serie

    inv.protocolo_autorizacao = get_after_label(r'Protocolo\s+de\s+autoriza[cç][aã]o', r'(\d{10,20})', text, 180)

    m_dest_start = re.search(DEST_BLOCK_START, text, re.I | re.S)
    header_block = text[:m_dest_start.start()] if m_dest_start else text[:2500]
    raw_emit_ie = clean_inline_value(
        get_after_label(
            r'Inscri[cç][aã]o\s+Estadual\b',
            r'([A-Z0-9\.\-\/]+)',
            header_block,
            80
        )
    )
    if looks_like_ie(raw_emit_ie):
        inv.emitente_ie = raw_emit_ie

    cnpjs = re.findall(CNPJ_RE, text)
    if cnpjs:
        inv.emitente_cnpj = cnpjs[0] if len(cnpjs) >= 1 else None
        inv.destinatario_cnpj = cnpjs[1] if len(cnpjs) >= 2 else None

    m_recv = re.search(r'(?i)Recebemos\s+de\s+(.{5,120}?)\s+os\s+produtos', text, re.S)
    if m_recv:
        inv.emitente_nome = strip_cnpj_cpf(norm_space(m_recv.group(1)))
        tail = text[m_recv.end(): m_recv.end()+250]
        m_cnpj = re.search(CNPJ_RE, tail)
        if m_cnpj and not inv.emitente_cnpj:
            inv.emitente_cnpj = m_cnpj.group(0)

    dest_block = extract_block(
        text,
        DEST_BLOCK_START,
        r'DADOS\s+DO\s+PRODUTO/SERVI[CÇ]O|DADOS\s+DOS\s+PRODUTOS\s*/\s*SERVI[CÇ]OS|TRANSPORTADOR|CALCULO\s+DO\s+IMPOSTO|C[ÁA]LCULO\s+DO\s+IMPOSTO'
    )
    if dest_block:
        m_dest = re.search(
            r'NOME\s*/\s*RAZ[ÃA]O\s+SOCIAL\s+(.+?)\s*(?:CNPJ|CPF)',
            dest_block,
            re.I | re.S
        )
        if m_dest:
            inv.destinatario_nome = strip_cnpj_cpf(norm_space(m_dest.group(1)))

        dest_cnpj_cpf = re.search(r'(' + CNPJ_RE + r'|' + CPF_RE + r')', dest_block)
        if dest_cnpj_cpf:
            inv.destinatario_cnpj = dest_cnpj_cpf.group(1)
            # If name not found yet, try to find it before the CNPJ/CPF
            if not inv.destinatario_nome:
                pre_cnpj = dest_block[:dest_cnpj_cpf.start()].strip()
                lines = [l.strip() for l in pre_cnpj.splitlines() if l.strip()]
                if lines:
                    last_line = lines[-1]
                    if "SOCIAL" in last_line.upper():
                        # Try to find after "SOCIAL"
                        m_social = re.search(r'SOCIAL\s*(?:\n|\r\n)?\s*(.+)$', last_line, re.I)
                        if m_social:
                             inv.destinatario_nome = strip_cnpj_cpf(m_social.group(1))
                    else:
                        inv.destinatario_nome = strip_cnpj_cpf(last_line)

        flat = dest_block.replace("\n", " ")
        m_name_cnpj = re.search(r'([A-Z][A-Z0-9 &\.,\-]{2,80})\s+(' + CNPJ_RE + r'|' + CPF_RE + r')', flat)
        if m_name_cnpj and not inv.destinatario_nome:
            inv.destinatario_nome = strip_cnpj_cpf(norm_space(m_name_cnpj.group(1)))
            if not inv.destinatario_cnpj:
                inv.destinatario_cnpj = m_name_cnpj.group(2)

        if not inv.destinatario_nome:
            # Fallback for "Block Style" PDFs: Find labels then search values below
            if "Nome/Razao Social" in dest_block or "NOME / RAZÃO SOCIAL" in dest_block.upper():
                lines = [l.strip() for l in dest_block.splitlines() if l.strip()]
                try:
                    idx_label = -1
                    for j, ln in enumerate(lines):
                        upper_ln = ln.upper()
                        if "NOME / RAZ" in upper_ln or "NOME/RAZAO SOCIAL" in upper_ln:
                            idx_label = j
                            break

                    if idx_label >= 0:
                        labels = {"CNPJ/CPF", "CNPJ / CPF", "DATA DA EMISSAO", "ENDERECO", "BAIRRO/DISTRITO", "BAIRRO / DISTRITO", "CEP", "DATA DA SAIDA", "MUNICIPIO", "FONE/FAX", "FONE / FAX", "UF", "INSCRICAO ESTADUAL", "HORA"}
                        for ln in lines[idx_label+1:]:
                            upper_ln = ln.upper()
                            if len(ln) > 5 and not any(label in upper_ln for label in labels):
                                if not re.search(r'\d{2}/\d{2}/\d{4}', ln):
                                    inv.destinatario_nome = strip_cnpj_cpf(ln)
                                    break
                except Exception:
                    pass

        if not inv.destinatario_nome:
            inv.destinatario_nome = first_company_like_after(dest_block, text)

        if not inv.destinatario_nome:
            candidate = get_after_label(r'Nome\s*/\s*Raz[ãa]o\s+Social', r'([^\n\r]+)', dest_block, 220)
            if candidate and not re.match(r'(?i)^\s*(CNPJ|CPF)\s*$', candidate):
                inv.destinatario_nome = strip_cnpj_cpf(candidate)

        inv.destinatario_endereco = get_after_label(r'Endere[cç]o', r'([^\n\r]+?)(?=\s*(?:Bairro\s*/\s*Distrito|Bairro|Distrito|CEP|Data|Munic|Fone))', dest_block, 260)
        if not inv.destinatario_endereco:
            inv.destinatario_endereco = get_after_label(r'Endere[cç]o', r'([^\n\r]+)', dest_block, 260)

        inv.destinatario_bairro = get_after_label(r'Bairro\s*/\s*Distrito|Bairro(?:/Distrito)?', r'([^\n\r]+?)(?=\s*(?:CEP|Data|Munic|Fone|UF))', dest_block, 140)
        inv.destinatario_municipio = get_after_label(r'Munic[ií]pio', r'([^\n\r]+?)(?=\s*(?:Fone|Fax|UF|Inscr))', dest_block, 140)
        inv.destinatario_uf = get_after_label(r'\bUF\b', r'([A-Z]{2})', dest_block, 60) or inv.destinatario_uf
        inv.destinatario_cep = get_after_label(r'CEP', r'(' + CEP_RE + r')', dest_block, 80)
        inv.destinatario_fone = get_after_label(r'(?:Telefone|Fone\s*/?\s*Fax?)', r'(' + PHONE_RE + r')', dest_block, 160)
        raw_ie = get_after_label(
            r'Inscri[cç][aã]o\s+Estadual\b',
            r'([A-Z0-9\.\-\/]+)',
            dest_block,
            80
        )
        raw_ie = clean_inline_value(raw_ie)
        if looks_like_ie(raw_ie):
            inv.destinatario_ie = raw_ie

        if not inv.destinatario_ie:
            cand_ie = next_valid_value_after_label(dest_block, r'Inscri[cç][aã]o\s+Estadual')
            if looks_like_ie(cand_ie):
                inv.destinatario_ie = cand_ie

    m_data_emissao = re.search(r'(?:Data\s*(?:/\s*Hora)?\s+da\s+Emiss[aã]o)\s*.*?([0-3]?\d/[01]?\d/\d{2,4}(?:\s+\d{2}:\d{2}:\d{2})?)', text, re.I | re.S)
    if m_data_emissao:
        inv.data_emissao = m_data_emissao.group(1)

    m_data_saida_entrada = re.search(
        r'(?:Data\s*(?:/\s*Hora)?\s+d[ae]\s+Sa[ií]da\s*/\s*Entrada)\s*.*?([0-3]?\d/[01]?\d/\d{2,4}(?:\s+\d{2}:\d{2}:\d{2})?)',
        text,
        re.I | re.S
    )
    if m_data_saida_entrada:
        inv.data_saida_entrada = m_data_saida_entrada.group(1)

    calc_block = extract_block(text,
        r'(?=Base\s+de\s+Calculo\s+ICMS|Base\s+de\s+C[áa]lculo\s+do\s+ICMS|BASE\s+DE\s+C[ÁA]LC\.?\s+DO\s+ICMS)',
        TRANSPORT_BLOCK_START + r'|INFORMA[cç][OÕ]ES\s+COMPLEMENTARES|DADOS\s+ADICIONAIS|Reservado\s+ao\s+Fisco|$')

    if not calc_block or len(calc_block) < 20:
        calc_block = extract_block(text,
            r'(?=CALCULO\s+DO\s+IMPOSTO|C[ÁA]LCULO\s+DO\s+IMPOSTO)',
            TRANSPORT_BLOCK_START + r'|INFORMA[cç][OÕ]ES\s+COMPLEMENTARES|DADOS\s+ADICIONAIS|Reservado\s+ao\s+Fisco|$')

    if calc_block:
        lines = [l.strip() for l in calc_block.splitlines() if l.strip()]

        def grab_in_calc(label_pat: str) -> Optional[float]:
            v = get_after_label(label_pat, r'(' + MONEY_RE + r')', calc_block, 220)
            return br_to_float(v)

        inv.base_calculo_icms = grab_in_calc(r'(?:Base\s+de\s+C[áa]lculo\s+(?:do\s+)?ICMS|BASE\s+DE\s+C[ÁA]LC\.?\s+DO\s+ICMS)')
        inv.total_icms = grab_in_calc(r'(?:Valor\s+do\s+ICMS|V\.\s*ICMS)')
        inv.base_calculo_st = grab_in_calc(r'(?:Base\s+de\s+C[áa]lculo\s+de\s+Subst\.?\s+Trib\.?|Base\s+de\s+C[áa]lc\.?\s+do\s+ICMS\s+Subst\.?|BASE\s+DE\s+C[ÁA]LC\.?\s+ICMS\s+S\.?T\.?)')
        inv.valor_icms_st = grab_in_calc(r'(?:Valor\s+do\s+ICMS\s+Subst\.?\s+Trib\.?|Valor\s+do\s+ICMS\s+Substitui[cç][aã]o|VALOR\s+DO\s+ICMS\s+SUBST\.?)')
        inv.total_nota = grab_in_calc(r'(?:Valor\s+total\s+da\s+Nota|Valor\s+Total\s+da\s+Nota|V\.\s*TOTAL\s+DA\s+NOTA)')
        inv.total_ipi  = grab_in_calc(r'(?:Valor\s+total\s+do\s+IPI|Valor\s+do\s+IPI|VALOR\s+TOTAL\s+IPI|V\.\s*TOTAL\s+IPI)')
        inv.frete      = grab_in_calc(r'(?:Valor\s+do\s+frete|Frete\b|V\.\s*FRETE)')
        inv.seguro     = grab_in_calc(r'(?:Valor\s+do\s+seguro|Seguro\b|V\.\s*SEGURO)')
        inv.desconto_total = grab_in_calc(r'(?:Desconto\b|V\.\s*DESC)')
        inv.outras_despesas = grab_in_calc(r'(?:Outras\s+despesas\s+acess[oó]rias|Outras\s+Despesas|OUTRAS\s+DESP\.?)')

        money_line_re = re.compile(r'^' + MONEY_RE + r'$')
        value_runs: List[Tuple[int, List[str]]] = []
        run_start = None
        current_run: List[str] = []
        for idx, ln in enumerate(lines):
            if money_line_re.match(ln):
                if run_start is None:
                    run_start = idx
                current_run.append(ln)
            else:
                if current_run:
                    value_runs.append((run_start, current_run))
                    run_start = None
                    current_run = []
        if current_run:
            value_runs.append((run_start, current_run))

        best_run = max(value_runs, key=lambda x: len(x[1])) if value_runs else None
        if best_run and best_run[0] > 0 and len(best_run[1]) >= 4:
            label_seq = lines[:best_run[0]]
            value_seq = [br_to_float(v) for v in best_run[1]]

            if len(value_seq) == len(label_seq) + 1:
                label_seq = ["Base de Calculo ICMS"] + label_seq
            if len(label_seq) >= len(value_seq):
                label_seq = label_seq[-len(value_seq):]

            if len(label_seq) == len(value_seq):
                def norm_label(lbl: str) -> str:
                    base = unicodedata.normalize("NFD", lbl)
                    base = base.encode("ascii", "ignore").decode("ascii")
                    return re.sub(r'[^a-z]', '', base.lower())

                mapping = {norm_label(lbl): val for lbl, val in zip(label_seq, value_seq)}

                def pick(keys, exclude=None):
                    exclude = exclude or []
                    for lbl, val in mapping.items():
                        if all(k in lbl for k in keys) and not any(ex in lbl for ex in exclude):
                            return val
                    return None

                mapped_base_icms = pick(["base", "icms"], exclude=["subst", "st"])
                mapped_val_icms = pick(["valor", "icms"], exclude=["subst", "st"])
                mapped_base_st = pick(["base", "subst"]) or pick(["base", "st"])
                mapped_val_st = pick(["valor", "subst"]) or pick(["valor", "st"])
                mapped_total_nota = pick(["valortotaldanota"]) or pick(["valortotalnota"])
                mapped_total_ipi = pick(["ipi"], exclude=["municipio"])
                mapped_frete = pick(["frete"])
                mapped_seguro = pick(["seguro"])
                mapped_desc = pick(["desconto"])
                mapped_outros = pick(["outras", "despesas"])

                if mapped_base_icms is not None and inv.base_calculo_icms is None:
                    inv.base_calculo_icms = mapped_base_icms
                if mapped_val_icms is not None and inv.total_icms is None:
                    inv.total_icms = mapped_val_icms
                if mapped_base_st is not None and (inv.base_calculo_st is None or inv.base_calculo_st == inv.base_calculo_icms):
                    inv.base_calculo_st = mapped_base_st
                if mapped_val_st is not None and inv.valor_icms_st is None:
                    inv.valor_icms_st = mapped_val_st
                if mapped_total_nota is not None and inv.total_nota is None:
                    inv.total_nota = mapped_total_nota
                if mapped_total_ipi is not None and inv.total_ipi is None:
                    inv.total_ipi = mapped_total_ipi
                if mapped_frete is not None and inv.frete is None:
                    inv.frete = mapped_frete
                if mapped_seguro is not None and inv.seguro is None:
                    inv.seguro = mapped_seguro
                if mapped_desc is not None and inv.desconto_total is None:
                    inv.desconto_total = mapped_desc
                if mapped_outros is not None and inv.outras_despesas is None:
                    inv.outras_despesas = mapped_outros

    if calc_block:
        all_calc_values = re.findall(MONEY_RE, calc_block)
        if len(all_calc_values) >= 5:
            if inv.base_calculo_st is None and len(all_calc_values) >= 3:
                if inv.base_calculo_icms == br_to_float(all_calc_values[0]):
                    inv.base_calculo_st = br_to_float(all_calc_values[2])

            if inv.valor_icms_st is None and len(all_calc_values) >= 4:
                if inv.base_calculo_icms == br_to_float(all_calc_values[0]):
                    inv.valor_icms_st = br_to_float(all_calc_values[3])

    pay_block = extract_block(
        text,
        r'PAGAMENTO',
        r'C[ÁA]LCULO\s+DO\s+IMPOSTO|' + TRANSPORT_BLOCK_START + r'|DADOS\s+DOS\s+PRODUTOS|$'
    )
    if pay_block:
        pay_lines = [norm_space(x) for x in pay_block.splitlines() if norm_space(x)]
        i = 0
        while i < len(pay_lines):
            ln = pay_lines[i]

            m_inline = re.search(
                r'Forma\s+(.+?)\s+Valor\s+R\$\s*(' + MONEY_RE + r'|\d[\d\.,]*)',
                ln,
                re.I
            )
            if m_inline:
                inv.pagamentos.append(
                    PaymentEntry(
                        forma=norm_space(m_inline.group(1)),
                        valor=br_to_float(m_inline.group(2))
                    )
                )
                i += 1
                continue

            if re.match(r'(?i)^Forma\s+', ln):
                forma = re.sub(r'(?i)^Forma\s+', '', ln).strip()
                valor = None
                for j in range(i + 1, min(i + 4, len(pay_lines))):
                    m_val = re.search(r'Valor\s+R\$\s*(' + MONEY_RE + r'|\d[\d\.,]*)', pay_lines[j], re.I)
                    if m_val:
                        valor = br_to_float(m_val.group(1))
                        i = j
                        break
                inv.pagamentos.append(PaymentEntry(forma=forma, valor=valor))

            i += 1

    ret_block = extract_block(
        text,
        r'INFORMA[cç][OÕ]ES\s+DO\s+LOCAL\s+DE\s+RETIRADA',
        r'PAGAMENTO|C[ÁA]LCULO\s+DO\s+IMPOSTO|TRANSPORTADOR/?VOLUMES\s+TRANSPORTADOS|$'
    )
    if ret_block:
        inv.retirada_cnpjcpf = get_after_label(
            r'CNPJ\s*/\s*CPF',
            r'(' + CNPJ_RE + r'|' + CPF_RE + r')',
            ret_block,
            120
        )
        inv.retirada_endereco = get_after_label(
            r'Endere[cç]o',
            r'([^\n\r]+?)(?=\s*(?:Bairro|CEP|Munic|UF|Fone|$))',
            ret_block,
            260
        ) or get_after_label(r'Endere[cç]o', r'([^\n\r]+)', ret_block, 260)
        inv.retirada_bairro = get_after_label(
            r'Bairro\s*/\s*Distrito|Bairro(?:/Distrito)?',
            r'([^\n\r]+?)(?=\s*(?:CEP|Munic|UF|Fone|$))',
            ret_block,
            140
        )
        inv.retirada_cep = get_after_label(r'CEP', r'(' + CEP_RE + r')', ret_block, 80)
        inv.retirada_municipio = get_after_label(
            r'Munic[ií]pio',
            r'([^\n\r]+?)(?=\s*(?:UF|Fone|$))',
            ret_block,
            140
        )
        inv.retirada_uf = get_after_label(r'\bUF\b', r'([A-Z]{2})', ret_block, 40)

    inv.modalidade_frete_raw = get_after_label(
        r'(?:Frete\s+por\s+Conta|Modalidade\s+do\s+frete|Frete)',
        r'([0-9]\s*-\s*[^\n\r]+)',
        text,
        160
    )
    transp_block = extract_block(text,
        TRANSPORT_BLOCK_START,
        r'INFORMA[cç][OÕ]ES\s+COMPLEMENTARES|DADOS\s+ADICIONAIS|Reservado\s+ao\s+Fisco|$')
    if transp_block:
        inv.transportador_nome = (
            get_after_label(
                r'(?:Nome\s*/\s*Raz[ãa]o\s+Social|Transportador/?Remetente)',
                r'([^\n\r]+)',
                transp_block,
                180
            )
            or inv.transportador_nome
        )
        inv.transportador_nome = clean_inline_value(inv.transportador_nome)
        if not inv.transportador_nome:
            transp_lines = [norm_space(x) for x in transp_block.splitlines() if norm_space(x)]
            for idx, ln in enumerate(transp_lines):
                if re.search(r'(?:Nome\s*/\s*Raz[ãa]o\s+Social|Transportador/?Remetente)', ln, re.I):
                    for cand in transp_lines[idx + 1:idx + 6]:
                        cleaned = clean_inline_value(cand)
                        if not cleaned:
                            continue
                        if not re.search(r'[A-Za-zÁÉÍÓÚÃÕÂÊÔÇáéíóúãõâêôç]', cleaned):
                            continue
                        if re.search(r'(?i)\bpor\s+conta\b', cleaned):
                            continue
                        inv.transportador_nome = cleaned
                        break
                    if inv.transportador_nome:
                        break

        inv.transportador_cnpjcpf = (
            get_after_label(
                r'CNPJ\s*/\s*CPF',
                r'(' + CNPJ_RE + r'|' + CPF_RE + r')',
                transp_block,
                120
            )
            or inv.transportador_cnpjcpf
        )

        raw_t_ie = get_after_label(
            r'(?:Inscri[cç][aã]o\s+Estadual|\bIE\b)',
            r'([A-Z0-9\.\-\/]+)',
            transp_block,
            120
        )
        raw_t_ie = clean_inline_value(raw_t_ie)
        if looks_like_ie(raw_t_ie):
            inv.transportador_ie = raw_t_ie
        inv.placa_veiculo = get_after_label(r'Placa\s+do\s+Ve[ií]culo', r'([A-Z]{3}\-?\d{4}|\w{5,8})', transp_block, 80)
        inv.uf_veiculo = get_after_label(r'\bUF\b', r'([A-Z]{2})', transp_block, 40) or inv.uf_veiculo
        inv.rntc = get_after_label(r'RNTC', r'([A-Z0-9\-\ /]+)', transp_block, 80)
        inv.volumes_qtd = get_after_label(r'Quantidade', r'([0-9]+)', transp_block, 60)
        inv.volumes_especie = get_after_label(r'Esp[eé]cie', r'([^\n\r]+)', transp_block, 100)
        inv.volumes_marca = get_after_label(r'Marca', r'([^\n\r]+)', transp_block, 100)
        inv.volumes_numeracao = get_after_label(r'Numera[cç][aã]o', r'([^\n\r]+)', transp_block, 100)

        def grab_weight(label):
            v = get_after_label(label, r'(' + MONEY_RE + r'|\d+[\,\.]\d+|\d+)', transp_block, 80)
            return br_to_float(v or "")
        inv.peso_bruto = grab_weight(r'Peso\s+Bruto')
        inv.peso_liquido = grab_weight(r'Peso\s+L[ií]quido')

    add_block = extract_block(
        text,
        r'(?:DADOS\s+ADICIONAIS|INFORMA[cç][OÕ]ES\s+COMPLEMENTARES)',
        r'RESERVADO\s+AO\s+FISCO|$'
    )
    if add_block:
        m_contrib = re.search(
            r'Inf\.\s*Contribuinte\s*:\s*(.*?)(?=(?:Inf\.\s*fisco\s*:|RESERVADO\s+AO\s+FISCO|$))',
            add_block,
            re.I | re.S
        )
        m_fisco = re.search(
            r'Inf\.\s*fisco\s*:\s*(.*?)(?=(?:RESERVADO\s+AO\s+FISCO|$))',
            add_block,
            re.I | re.S
        )
        if m_contrib:
            inv.info_compl_contribuinte = norm_space(m_contrib.group(1))
        else:
            cleaned_add_block = norm_space(re.sub(r'(?i)\bINFORMA[cç][OÕ]ES\s+COMPLEMENTARES\b', '', add_block))
            cleaned_add_block = norm_space(re.sub(r'(?i)\bDADOS\s+ADICIONAIS\b', '', cleaned_add_block))
            if cleaned_add_block:
                inv.info_compl_contribuinte = cleaned_add_block
        if m_fisco:
            inv.info_compl_fisco = norm_space(m_fisco.group(1))

    block = extract_block(text, PRODUCT_BLOCK_START, PRODUCT_BLOCK_END) or text
    itens: List[Item] = parse_items_danfe_block(block)

    # Suspect bad parsing if description is too short, just numbers, or contains header labels
    is_suspicious = False
    if itens:
        for it in itens:
            desc = (it.descricao or "").upper()
            if len(desc) < 5 or re.fullmatch(r'[\d\., ]+', desc):
                is_suspicious = True
                break
            if "DESCRIÇÃO DO PRODUTO" in desc or "CÓDIGO PRODUTO" in desc or "NCM/SH" in desc:
                is_suspicious = True
                break
            if not it.codigo_produto or not it.ncm:
                is_suspicious = True
                break

    if not itens or is_suspicious:
        mashed = parse_mashed_items(block)
        if mashed:
            # If mashed found something and original was suspicious, prefer mashed
            # or if original found nothing, use mashed.
            itens = mashed

    plataforma, pedido, numero_pedido = extract_marketplace_order_info(text)
    inv.plataforma = plataforma
    inv.pedido = pedido
    inv.numero_pedido = numero_pedido

    inv.emitente_nome = strip_cnpj_cpf(inv.emitente_nome)
    inv.destinatario_nome = strip_cnpj_cpf(inv.destinatario_nome)
    inv.itens = itens
    return inv
