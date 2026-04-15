# -*- coding: utf-8 -*-

import re
from datetime import datetime
from typing import Dict, Any, List, Tuple
from brazil_tool.core.models import Invoice

EXPORT_COLUMNS: List[Tuple[str, str]] = [
    ("文件名", "file_name"),
    ("状态", "status"),
    ("发票号", "numero"),
    ("系列", "serie"),
    ("访问密钥", "chave_acesso"),
    ("业务性质", "natureza_operacao"),
    ("协议号", "protocolo_autorizacao"),
    ("开票日期", "data_emissao"),
    ("进出日期", "data_saida_entrada"),
    ("发件人", "emitente_nome"),
    ("发件人CNPJ", "emitente_cnpj"),
    ("收货方", "destinatario_nome"),
    ("收货方证件号", "destinatario_cnpj"),
    ("收货方IE", "destinatario_ie"),
    ("收货地址", "destinatario_endereco"),
    ("收货区", "destinatario_bairro"),
    ("收货城市", "destinatario_municipio"),
    ("收货州", "destinatario_uf"),
    ("收货邮编", "destinatario_cep"),
    ("总金额", "total_nota"),
    ("ICMS", "total_icms"),
    ("ICMS基数", "base_calculo_icms"),
    ("ST基数", "base_calculo_st"),
    ("ICMS ST", "valor_icms_st"),
    ("IPI", "total_ipi"),
    ("运费", "frete"),
    ("保险", "seguro"),
    ("折扣", "desconto_total"),
    ("其他费用", "outras_despesas"),
    ("运费方式", "modalidade_frete_raw"),
    ("承运人", "transportador_nome"),
    ("承运人证件号", "transportador_cnpjcpf"),
    ("承运人IE", "transportador_ie"),
    ("车牌", "placa_veiculo"),
    ("车辆UF", "uf_veiculo"),
    ("RNTC", "rntc"),
    ("件数", "volumes_qtd"),
    ("包装种类", "volumes_especie"),
    ("包装标记", "volumes_marca"),
    ("包装编号", "volumes_numeracao"),
    ("毛重", "peso_bruto"),
    ("净重", "peso_liquido"),
    ("付款汇总", "pagamentos_resumo"),
    ("提货地证件号", "retirada_cnpjcpf"),
    ("提货地址", "retirada_endereco"),
    ("提货区", "retirada_bairro"),
    ("提货城市", "retirada_municipio"),
    ("提货州", "retirada_uf"),
    ("提货邮编", "retirada_cep"),
    ("附加信息", "info_compl_contribuinte"),
    ("平台", "plataforma"),
    ("Pedido", "pedido"),
    ("订单号", "numero_pedido"),
]

EXPORT_DATE_KEYS = {"data_emissao", "data_saida_entrada"}


def format_export_date(value: Any) -> str:
    if value is None:
        return ""

    text = str(value).strip()
    if not text or text.lower() in {"none", "null"}:
        return ""

    date_part = text.split()[0]
    if re.fullmatch(r"\d{8}", date_part):
        return date_part

    for fmt in ("%d/%m/%Y", "%Y/%m/%d", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(date_part, fmt).strftime("%Y%m%d")
        except ValueError:
            continue

    return text


def _fmt(v: Any, key: str = "") -> str:
    if v is None:
        return ""
    if key in EXPORT_DATE_KEYS:
        return format_export_date(v)
    if isinstance(v, float):
        return f"{v:.2f}"
    return str(v)


def _pagamentos_resumo(inv: Invoice) -> str:
    parts = []
    for p in getattr(inv, "pagamentos", []) or []:
        forma = (p.forma or "").strip()
        valor = "" if p.valor is None else f"{p.valor:.2f}"
        if forma and valor:
            parts.append(f"{forma}:{valor}")
        elif forma:
            parts.append(forma)
        elif valor:
            parts.append(valor)
    return " | ".join(parts)


def invoice_to_export_row(inv: Invoice) -> Dict[str, str]:
    row = {k: _fmt(getattr(inv, k, ""), k) for _, k in EXPORT_COLUMNS if k != "pagamentos_resumo"}
    row["pagamentos_resumo"] = _pagamentos_resumo(inv)
    return row
