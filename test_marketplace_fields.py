import sys
import types


qtcore = types.ModuleType("PySide6.QtCore")


class DummyQSettings:
    IniFormat = object()

    def __init__(self, *args, **kwargs):
        pass

    def value(self, key, default=None, type=None):
        return default


qtcore.QSettings = DummyQSettings
sys.modules.setdefault("PySide6", types.ModuleType("PySide6"))
sys.modules["PySide6.QtCore"] = qtcore

from brazil_tool.core.export_schema import format_export_date, invoice_to_export_row
from brazil_tool.core.parser import extract_marketplace_order_info, parse_invoice_from_text


def test_extract_marketplace_order_info():
    platform, pedido, order_number = extract_marketplace_order_info(
        "Pedido: 260331H3UBSQVY"
    )
    assert platform == "Shopee"
    assert pedido == "260331H3UBSQVY"
    assert order_number == "260331H3UBSQVY"

    platform, pedido, order_number = extract_marketplace_order_info(
        "Pedido: 2000015783474152"
    )
    assert platform == "Mercado Livre"
    assert pedido == "2000015783474152"
    assert order_number == "2000015783474152"

    platform, pedido, order_number = extract_marketplace_order_info(
        "xPed:2000015325577368 (Pedido 200001532557736) Cnpj: 03007331007405"
    )
    assert platform == "Mercado Livre"
    assert pedido == "2000015325577368"
    assert order_number == "2000015325577368"


def test_parse_invoice_marketplace_fields():
    sample_text = """
Nº. 000.050.613
Série 002
NATUREZA DA OPERAÇÃO
Venda de mercadorias
PROTOCOLO DE AUTORIZAÇÃO DE USO
135261211454088
DADOS DOS PRODUTOS / SERVIÇOS
N60-220V Bomba Periferica 1/2cv N60-220v Brasbombas 84137090 100 6108 un 1,0000 139,8900 139,89 6,99 132,90 5,32 4,00 (Pedido 260331H3UBSQVY)
DADOS ADICIONAIS
INFORMAÇÕES COMPLEMENTARES
Inf. Contribuinte: Tributos aproximados. Pedido: 260331H3UBSQVY
"""
    invoice = parse_invoice_from_text(sample_text, "sample.pdf")
    assert invoice.plataforma == "Shopee"
    assert invoice.pedido == "260331H3UBSQVY"
    assert invoice.numero_pedido == "260331H3UBSQVY"
    assert invoice.itens
    assert "PEDIDO" not in (invoice.itens[0].descricao or "").upper()

    row = invoice_to_export_row(invoice)
    assert row["plataforma"] == "Shopee"
    assert row["pedido"] == "260331H3UBSQVY"
    assert row["numero_pedido"] == "260331H3UBSQVY"


def test_export_date_format_is_yyyymmdd():
    sample_text = """
Nº. 000.050.613
Série 002
Data da Emissão 02/01/2026
Data de Saída/Entrada 03/01/2026
"""
    invoice = parse_invoice_from_text(sample_text, "sample.pdf")
    row = invoice_to_export_row(invoice)
    assert row["data_emissao"] == "20260102"
    assert row["data_saida_entrada"] == "20260103"

    assert format_export_date("2026/01/04") == "20260104"
    assert format_export_date("2026-01-05 10:20:30") == "20260105"
    assert format_export_date("20260106") == "20260106"


def test_derive_invoice_number_from_access_key():
    sample_text = """
CHAVE DE ACESSO
3526 0239 9698 9000 0180 5500 3000 0111 8512 8640 2431
"""
    invoice = parse_invoice_from_text(sample_text, "sample-danfe.pdf")
    assert invoice.numero == "000.011.185"
    assert invoice.serie == "003"


def test_access_key_extraction_ignores_protocol_digits():
    sample_text = """
CHAVE DE ACESSO
Consulta de autenticidade
3526 0239 9698 9000 0180 5500 3000 0111 8512 8640 2431
PROTOCOLO DE AUTORIZAÇÃO DE USO
135260764424
"""
    invoice = parse_invoice_from_text(sample_text, "sample-danfe.pdf")
    assert invoice.chave_acesso == "35260239969890000180550030000111851286402431"
    assert invoice.numero == "000.011.185"
    assert invoice.serie == "003"


def test_shopee_search_does_not_match_plain_digits():
    platform, pedido, order_number = extract_marketplace_order_info(
        "Mercadoria depositada sob o CNPJ 03007331009793."
    )
    assert platform is None
    assert pedido is None
    assert order_number is None


if __name__ == "__main__":
    test_extract_marketplace_order_info()
    test_parse_invoice_marketplace_fields()
    test_export_date_format_is_yyyymmdd()
    test_derive_invoice_number_from_access_key()
    test_access_key_extraction_ignores_protocol_digits()
    test_shopee_search_does_not_match_plain_digits()
    print("test_marketplace_fields: PASS")
