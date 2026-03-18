
import sys
import os
sys.path.append(os.getcwd())
from brazil_tool.core.report_parser import CollectionReportParser

test_cases = [
    '2922A1111',
    '4003E1405',
    '1614NF1526 Liquidado',
    '4083NF3504 Baixado',
    '3996NF3422 VENCIDO',
    '1708A2708'
]

print(f"{'Raw Reference':<25} | {'Clean Invoice':<15} | {'Parsed Info'}")
print("-" * 75)

for ref in test_cases:
    clean = CollectionReportParser.clean_invoice_number(ref)
    info = CollectionReportParser.parse_invoice_reference(ref)
    print(f"{ref:<25} | {str(clean):<15} | {info}")
