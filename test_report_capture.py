
import sys
import os
sys.path.append(os.getcwd())
from brazil_tool.core.report_parser import CollectionReportParser

# 模拟一段 Baixados 报告文本，包含各种参考号格式
mock_text = """
Data Venc.  Data Baixa  Valor Pago  Seu Numero
01/10/2025
02/10/2025
1.148,33
2079
05/10/2025
06/10/2025
1.248,14
1614NF1526
10/10/2025
11/10/2025
896,68
2922A1111
"""

records = CollectionReportParser.parse_report(mock_text)

print(f"{'Due Date':<12} | {'Amount':<10} | {'Invoice Ref':<15} | {'Clean Num'}")
print("-" * 60)

for r in records:
    clean = CollectionReportParser.clean_invoice_number(r['invoice_ref'])
    print(f"{r['due_date']:<12} | {r['amount']:<10.2f} | {r['invoice_ref']:<15} | {clean}")
