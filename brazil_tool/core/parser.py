# -*- coding: utf-8 -*-
import re
import unicodedata
from typing import Optional, List, Tuple
from brazil_tool.constants import MONEY_RE, CNPJ_RE, CPF_RE, ACCESS_KEY_RE, DIGITS44, PHONE_RE, CEP_RE
from brazil_tool.core.models import Invoice, Item
from brazil_tool.core.utils import (
    br_to_float, norm_space, only_digits, strip_cnpj_cpf,
    fix_ocr_text, get_after_label, extract_block
)

DEST_BLOCK_START = r'DESTINAT[ÁA]RIO\s*/\s*REMETENTE|DESTINATARIO\s*/\s*REMETENTE'
PRODUCT_BLOCK_START = r'DADOS\s+DO(?:S)?\s+PRODUTO(?:S)?\s*/\s*SERVI[CÇ]O(?:S)?'
PRODUCT_BLOCK_END = r'INFORMA[cç][OÕ]ES\s+COMPLEMENTARES|DADOS\s+ADICIONAIS|Reservado\s+ao\s+Fisco|CALCULO\s+DO\s+IMPOSTO|C[ÁA]LCULO\s+DO\s+IMPOSTO|$'
ALNUM_PRODUCT_CODE_RE = r'[A-Z0-9][A-Z0-9\-./]{2,}'


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
        return (
            s_up.startswith('XPED:') or
            'TOTAL APROXIMADO DE TRIBUTOS' in s_up or
            'FEDERAIS, ESTADUAIS E MUNICIPAIS' in s_up or
            s_up.startswith('PFCPUFDEST=') or
            s_up.startswith('PICMS') or
            s_up.startswith('VICMS') or
            s_up.startswith('PREDBC=') or
            'PEDIDO' in s_up or
            re.fullmatch(r'\(?PEDIDO\s+\d+\)?', s_up) is not None or
            re.fullmatch(r'\d+\)?', s_up) is not None
        )

    while i < len(lines):
        ln = lines[i]
        m = re.match(r'^(' + ALNUM_PRODUCT_CODE_RE + r')\s+(.+)$', ln)
        if not m or 'CÓDIGO PRODUTO' in ln.upper() or 'CODIGO PRODUTO' in ln.upper():
            i += 1
            continue

        item = Item()
        item.codigo_produto = m.group(1)
        desc_parts = [m.group(2)]
        i += 1

        while i < len(lines) and not re.match(r'^\d{8}\b', lines[i]):
            if not is_noise_line(lines[i]):
                desc_parts.append(lines[i])
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

    raw_natureza = get_after_label(r'Natureza\s+de\s+Opera[cç][aã]o', r'((?!Protocolo)[A-Z][A-Z \-/]{2,60})', text, 220)

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

    key_raw = get_after_label(r'Chave\s+de\s+Acesso', ACCESS_KEY_RE, text, 600) or \
              (re.search(ACCESS_KEY_RE, text, re.S).group(1) if re.search(ACCESS_KEY_RE, text, re.S) else None)
    if key_raw:
        digits = only_digits(key_raw)
        inv.chave_acesso = digits[:44] if len(digits) >= 44 else None
    else:
        m44 = re.search(DIGITS44, text)
        if m44: inv.chave_acesso = m44.group(0)

    inv.protocolo_autorizacao = get_after_label(r'Protocolo\s+de\s+autoriza[cç][aã]o', r'(\d{10,20})', text, 180)

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
            r'NOME\s*/\s*RAZ[ÃA]O\s+SOCIAL\s+(.+?)\s+CNPJ\s*/\s*CPF\s+(' + CNPJ_RE + r'|' + CPF_RE + r')',
            dest_block,
            re.I | re.S
        )
        if m_dest:
            inv.destinatario_nome = strip_cnpj_cpf(norm_space(m_dest.group(1)))
            inv.destinatario_cnpj = m_dest.group(2)

        dest_cnpj_cpf = get_after_label(r'CNPJ\s*/\s*CPF', r'(' + CNPJ_RE + r'|' + CPF_RE + r')', dest_block, 120)
        if dest_cnpj_cpf:
            inv.destinatario_cnpj = dest_cnpj_cpf

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
        inv.destinatario_ie = get_after_label(r'Inscri[cç][aã]o\s+Estadual\b', r'([A-Z0-9\.\-\/]+)', dest_block, 80)

    m_data_emissao = re.search(r'(?:Data\s*(?:/\s*Hora)?\s+da\s+Emiss[aã]o)\s*.*?([0-3]?\d/[01]?\d/\d{2,4}(?:\s+\d{2}:\d{2}:\d{2})?)', text, re.I | re.S)
    if m_data_emissao:
        inv.data_emissao = m_data_emissao.group(1)

    m_data_saida_entrada = re.search(r'(?:Data\s*(?:/\s*Hora)?\s+de\s+Sa[ií]da/Entrada)\s*.*?([0-3]?\d/[01]?\d/\d{2,4}(?:\s+\d{2}:\d{2}:\d{2})?)', text, re.I | re.S)
    if m_data_saida_entrada:
        inv.data_saida_entrada = m_data_saida_entrada.group(1)

    calc_block = extract_block(text,
        r'(?=Base\s+de\s+Calculo\s+ICMS|Base\s+de\s+C[áa]lculo\s+do\s+ICMS|BASE\s+DE\s+C[ÁA]LC\.?\s+DO\s+ICMS)',
        r'TRANSPORTADOR/?VOLUMES\s+TRANSPORTADOS|INFORMA[cç][OÕ]ES\s+COMPLEMENTARES|DADOS\s+ADICIONAIS|Reservado\s+ao\s+Fisco|$')

    if not calc_block or len(calc_block) < 20:
        calc_block = extract_block(text,
            r'(?=CALCULO\s+DO\s+IMPOSTO|C[ÁA]LCULO\s+DO\s+IMPOSTO)',
            r'TRANSPORTADOR/?VOLUMES\s+TRANSPORTADOS|INFORMA[cç][OÕ]ES\s+COMPLEMENTARES|DADOS\s+ADICIONAIS|Reservado\s+ao\s+Fisco|$')

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

    inv.modalidade_frete_raw = get_after_label(r'(?:Frete\s+por\s+Conta|Modalidade\s+do\s+frete)', r'([0-9]\s*-\s*[^\n\r]+)', text, 160)
    transp_block = extract_block(text,
        r'TRANSPORTADOR/?VOLUMES\s+TRANSPORTADOS',
        r'INFORMA[cç][OÕ]ES\s+COMPLEMENTARES|DADOS\s+ADICIONAIS|Reservado\s+ao\s+Fisco|$')
    if transp_block:
        inv.transportador_nome = get_after_label(r'Transportador/?Remetente', r'([^\n\r]+)', transp_block, 180)
        inv.transportador_cnpjcpf = get_after_label(r'CNPJ/CPF', r'([0-9\.\-\/]+)', transp_block, 100)
        inv.transportador_ie = get_after_label(r'IE\b', r'([A-Z0-9\.\-\/]+)', transp_block, 80)
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

    block = extract_block(text, PRODUCT_BLOCK_START, PRODUCT_BLOCK_END) or text
    itens: List[Item] = parse_items_danfe_block(block)

    if not itens:
        full_item_text = " ".join([norm_space(x) for x in block.splitlines()])
        product_code_re = r'(?<![A-Za-z])\b\d{10,}\b'
        matches = list(re.finditer(product_code_re, full_item_text))

        raw_entries = []
        for i, match in enumerate(matches):
            start = match.start()
            end = matches[i+1].start() if i + 1 < len(matches) else None
            raw_entries.append(full_item_text[start:end])

        for entry in raw_entries:
            if len(entry) < 20 or not re.search(MONEY_RE, entry):
                continue

            it = Item()

            m = re.search(r"^(?P<codigo_produto>\d{10,})", entry)
            if m: it.codigo_produto = m.group('codigo_produto')

            m = re.search(r"\b(?P<ncm>\d{8})\b", entry)
            if m: it.ncm = m.group('ncm')

            if it.ncm:
                m = re.search(re.escape(it.ncm) + r"\s+(?P<cst>\d{3})", entry)
                if m: it.cst = m.group('cst')

            m = re.search(r"\b(?P<cfop>\d{4})\b", entry)
            if m: it.cfop = m.group('cfop')

            m = re.search(r"\b(?P<unidade>UN|UNID|PC|KG|LT|M|CX|PCT|SC)\b", entry, re.I)
            if m: it.unidade = m.group('unidade')

            money_values = re.findall(MONEY_RE, entry)
            map_values = money_values[:]
            if len(map_values) >= 7 and re.search(r"(\d+[\,\.]\d{2})\s+(\d+[\,\.]\d{2})\s*$", entry):
                map_values = map_values[:-2]
            else:
                aliq_pair_match = None
                for mm in re.finditer(r"(\d+[\,\.]\d{2})\s+(\d+[\,\.]\d{2})(?=\s+[A-Za-z])", entry):
                    aliq_pair_match = mm
                if len(map_values) >= 7 and aliq_pair_match:
                    if map_values[-2] == aliq_pair_match.group(1) and map_values[-1] == aliq_pair_match.group(2):
                        map_values = map_values[:-2]

            m_quant = re.search(r'\b(?:' + '|'.join(['UN', 'UNID', 'PC', 'KG', 'LT', 'M', 'CX', 'PCT', 'SC']) + r')\b\s*(\d+(?:,\d+)?)', entry, re.I)

            if m_quant:
                it.quantidade = br_to_float(m_quant.group(1))

                if len(map_values) >= 2:
                    it.valor_unitario = br_to_float(map_values[0])

                    val_1 = br_to_float(map_values[1])
                    raw_total = (it.quantidade or 0) * (it.valor_unitario or 0)

                    is_index_1_total = False
                    if raw_total > 0 and val_1 is not None:
                        diff = abs(val_1 - raw_total)
                        if diff < 1.0 or (diff / raw_total) < 0.05:
                            is_index_1_total = True

                    total_index = 2
                    expected_total = None

                    if is_index_1_total:
                        it.desconto = 0.0
                        it.valor_total = val_1
                        total_index = 1
                    else:
                        it.desconto = val_1
                        expected_total = raw_total - (it.desconto or 0)

                        if len(map_values) > 2:
                            candidates = []
                            for idx in (2, 3):
                                if len(map_values) > idx:
                                    candidates.append((idx, br_to_float(map_values[idx])))

                            if candidates:
                                valid_candidates = [c for c in candidates if c[1] is not None]
                                if valid_candidates:
                                    total_index = min(valid_candidates, key=lambda c: abs(c[1] - expected_total))[0]

                    if len(map_values) > total_index and it.valor_total is None:
                        it.valor_total = br_to_float(map_values[total_index])
                    if len(map_values) > total_index + 1:
                        it.bc_icms = br_to_float(map_values[total_index + 1])
                    if len(map_values) > total_index + 2:
                        it.valor_icms = br_to_float(map_values[total_index + 2])
                    if len(map_values) > total_index + 3:
                        it.valor_ipi = br_to_float(map_values[total_index + 3])
                    elif total_index == 3 and len(map_values) > 2:
                        it.valor_ipi = br_to_float(map_values[2])
            else:
                if len(map_values) >= 7:
                    it.quantidade = br_to_float(map_values[0])
                    it.valor_unitario = br_to_float(map_values[1])
                    it.desconto = br_to_float(map_values[2])
                    it.valor_total = br_to_float(map_values[3])
                    it.bc_icms = br_to_float(map_values[4])
                    it.valor_icms = br_to_float(map_values[5])
                    it.valor_ipi = br_to_float(map_values[6])

            aliq_end_index = -1
            if it.ncm:
                ncm_match = re.search(r"\b" + re.escape(it.ncm) + r"\b", entry)
                if ncm_match:
                    start_search = ncm_match.end()

                    aliq_matches = list(re.finditer(r"(\d+[\,\.]\d{2})\s+(\d+[\,\.]\d{2})(?=\s|$|\s+[A-Za-z])", entry[start_search:]))

                    if aliq_matches:
                        m_aliq = aliq_matches[-1]
                        it.aliquota_icms = br_to_float(m_aliq.group(1))
                        it.aliquota_ipi = br_to_float(m_aliq.group(2))
                        aliq_end_index = start_search + m_aliq.end()

            if it.aliquota_icms is None:
                m = re.search(r"(\d+[\,\.]\d{2})\s+(\d+[\,\.]\d{2})\s*$", entry)
                if m and len(m.groups()) == 2:
                    it.aliquota_icms = br_to_float(m.group(1))
                    it.aliquota_ipi = br_to_float(m.group(2))
                    aliq_end_index = m.end()

            desc_part1 = ""
            if it.codigo_produto and it.ncm:
                pattern = re.escape(it.codigo_produto) + r"\s+(.*?)\s+(?=\b" + re.escape(it.ncm) + r"\b)"
                m_desc1 = re.search(pattern, entry)
                if m_desc1:
                    desc_part1 = m_desc1.group(1).strip()
                    desc_part1 = norm_space(desc_part1)

            desc_part2 = ""
            if aliq_end_index > 0 and aliq_end_index < len(entry):
                raw_tail = entry[aliq_end_index:].strip()
                if raw_tail:
                    desc_part2 = norm_space(raw_tail)
            else:
                desc_entry = re.sub(r"(\d+[\,\.]\d{2})\s+(\d+[\,\.]\d{2})\s*$", "", entry).strip()
                all_money_positions = [(m.start(), m.end()) for m in re.finditer(MONEY_RE, desc_entry)]
                if all_money_positions:
                    last_money_end = all_money_positions[-1][1]
                    raw_tail = desc_entry[last_money_end:].strip()
                    clean_tail = re.sub(r'\b\d{8}\b', '', raw_tail)
                    clean_tail = re.sub(r'\b\d{3}\b', '', clean_tail)
                    clean_tail = re.sub(r'\b\d{4}\b', '', clean_tail)
                    desc_part2 = norm_space(clean_tail)

            full_desc_parts = [p for p in [desc_part1, desc_part2] if len(p) > 2]
            if full_desc_parts:
                raw_desc = " ".join(full_desc_parts)
                it.descricao = fix_ocr_text(raw_desc)

            itens.append(it)

    inv.emitente_nome = strip_cnpj_cpf(inv.emitente_nome)
    inv.destinatario_nome = strip_cnpj_cpf(inv.destinatario_nome)
    inv.itens = itens
    return inv
