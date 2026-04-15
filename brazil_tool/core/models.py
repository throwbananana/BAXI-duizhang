# -*- coding: utf-8 -*-
from dataclasses import dataclass, field
from typing import List, Dict, Optional

@dataclass
class Item:
    codigo_produto: Optional[str] = None
    codigo_domestico: Optional[str] = None  # Internal Mapping
    descricao: Optional[str] = None
    ncm: Optional[str] = None
    cst: Optional[str] = None
    cfop: Optional[str] = None
    unidade: Optional[str] = None
    quantidade: Optional[float] = None
    valor_unitario: Optional[float] = None
    valor_total: Optional[float] = None
    bc_icms: Optional[float] = None
    valor_icms: Optional[float] = None
    aliquota_icms: Optional[float] = None
    valor_ipi: Optional[float] = None
    aliquota_ipi: Optional[float] = None
    desconto: Optional[float] = None
    tags: List[str] = field(default_factory=list)


@dataclass
class PaymentEntry:
    forma: Optional[str] = None
    valor: Optional[float] = None

@dataclass
class Invoice:
    file_name: str = ""
    file_path: str = ""
    status: str = "Active"  # Active, Cancelled
    numero: Optional[str] = None
    serie: Optional[str] = None
    natureza_operacao: Optional[str] = None
    chave_acesso: Optional[str] = None
    protocolo_autorizacao: Optional[str] = None

    emitente_nome: Optional[str] = None
    emitente_cnpj: Optional[str] = None
    emitente_ie: Optional[str] = None
    emitente_iest: Optional[str] = None
    emitente_endereco: Optional[str] = None
    emitente_bairro: Optional[str] = None
    emitente_municipio: Optional[str] = None
    emitente_uf: Optional[str] = None
    emitente_cep: Optional[str] = None
    emitente_fone: Optional[str] = None

    destinatario_nome: Optional[str] = None
    destinatario_cnpj: Optional[str] = None
    destinatario_ie: Optional[str] = None
    destinatario_endereco: Optional[str] = None
    destinatario_bairro: Optional[str] = None
    destinatario_municipio: Optional[str] = None
    destinatario_uf: Optional[str] = None
    destinatario_cep: Optional[str] = None
    destinatario_fone: Optional[str] = None

    data_emissao: Optional[str] = None
    data_saida_entrada: Optional[str] = None

    total_nota: Optional[float] = None
    total_icms: Optional[float] = None
    base_calculo_icms: Optional[float] = None
    base_calculo_st: Optional[float] = None
    valor_icms_st: Optional[float] = None
    total_ipi: Optional[float] = None
    frete: Optional[float] = None
    seguro: Optional[float] = None
    desconto_total: Optional[float] = None
    outras_despesas: Optional[float] = None

    modalidade_frete_raw: Optional[str] = None
    transportador_nome: Optional[str] = None
    transportador_cnpjcpf: Optional[str] = None
    transportador_ie: Optional[str] = None
    placa_veiculo: Optional[str] = None
    uf_veiculo: Optional[str] = None
    rntc: Optional[str] = None
    volumes_qtd: Optional[str] = None
    volumes_especie: Optional[str] = None
    volumes_marca: Optional[str] = None
    volumes_numeracao: Optional[str] = None
    peso_bruto: Optional[float] = None
    peso_liquido: Optional[float] = None


    pagamentos: List[PaymentEntry] = field(default_factory=list)

    retirada_cnpjcpf: Optional[str] = None
    retirada_endereco: Optional[str] = None
    retirada_bairro: Optional[str] = None
    retirada_cep: Optional[str] = None
    retirada_municipio: Optional[str] = None
    retirada_uf: Optional[str] = None

    info_compl_contribuinte: Optional[str] = None
    info_compl_fisco: Optional[str] = None
    llm_table_note: Optional[str] = None
    plataforma: Optional[str] = None
    pedido: Optional[str] = None
    numero_pedido: Optional[str] = None
    tags: List[str] = field(default_factory=list)

    extract_meta: Dict[str, str] = field(default_factory=dict)
    itens: List[Item] = field(default_factory=list)
