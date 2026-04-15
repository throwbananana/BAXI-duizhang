from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Iterable, List

from openpyxl import Workbook

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from brazil_tool.core.export_schema import EXPORT_COLUMNS, invoice_to_export_row
from brazil_tool.core.models import Invoice
from brazil_tool.core.parser import parse_invoice_from_text
from brazil_tool.core.pdf import extract_text_from_pdf


ITEM_COLUMNS = [
    ("商品编码", "codigo_produto"),
    ("国内编码", "codigo_domestico"),
    ("商品描述", "descricao"),
    ("NCM", "ncm"),
    ("CST", "cst"),
    ("CFOP", "cfop"),
    ("单位", "unidade"),
    ("数量", "quantidade"),
    ("单价", "valor_unitario"),
    ("总价", "valor_total"),
    ("ICMS基数", "bc_icms"),
    ("ICMS金额", "valor_icms"),
    ("ICMS税率", "aliquota_icms"),
    ("IPI金额", "valor_ipi"),
    ("IPI税率", "aliquota_ipi"),
    ("折扣", "desconto"),
]

COMBINED_COLUMNS = EXPORT_COLUMNS + ITEM_COLUMNS


def _fmt(value):
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def iter_pdfs(input_dir: Path) -> Iterable[Path]:
    return sorted(path for path in input_dir.rglob("*.pdf") if path.is_file())


def parse_invoices(pdf_paths: Iterable[Path]) -> List[Invoice]:
    invoices: List[Invoice] = []
    for index, pdf_path in enumerate(pdf_paths, 1):
        text, meta = extract_text_from_pdf(str(pdf_path), prefer_pymupdf=True, enable_ocr=False)
        invoice = parse_invoice_from_text(text, pdf_path.name)
        invoice.file_path = str(pdf_path)
        invoice.extract_meta = meta
        invoices.append(invoice)
        if index % 100 == 0:
            print(f"[parse] {index} PDFs")
    return invoices


def build_combined_rows(invoices: List[Invoice]) -> List[List[str]]:
    invoice_keys = [key for _, key in EXPORT_COLUMNS]
    item_keys = [key for _, key in ITEM_COLUMNS]
    rows: List[List[str]] = []
    for invoice in invoices:
        invoice_dict = invoice_to_export_row(invoice)
        invoice_part = [_fmt(invoice_dict.get(key, "")) for key in invoice_keys]
        items = invoice.itens or []
        if not items:
            rows.append(invoice_part + ["" for _ in item_keys])
            continue

        for item in items:
            item_part = [_fmt(getattr(item, key, "")) for key in item_keys]
            rows.append(invoice_part + item_part)
    return rows


def build_summary_rows(invoices: List[Invoice]) -> List[List[str]]:
    total_amount = sum(invoice.total_nota or 0.0 for invoice in invoices)
    item_count = sum(len(invoice.itens or []) for invoice in invoices)
    platform_counter = Counter(invoice.plataforma or "未识别" for invoice in invoices)
    pedido_count = sum(1 for invoice in invoices if invoice.pedido)
    order_count = sum(1 for invoice in invoices if invoice.numero_pedido)

    rows = [
        ["生成时间", datetime.now().isoformat(timespec="seconds")],
        ["发票数量", str(len(invoices))],
        ["商品行数量", str(item_count)],
        ["Pedido 识别数", str(pedido_count)],
        ["订单号识别数", str(order_count)],
        ["发票总金额", f"{total_amount:.2f}"],
        ["", ""],
        ["平台", "数量"],
    ]
    for platform, count in sorted(platform_counter.items(), key=lambda item: (-item[1], item[0])):
        rows.append([platform, str(count)])
    return rows


def write_csv(path: Path, headers: List[str], rows: List[List[str]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerows(rows)


def write_xlsx(path: Path, combined_rows: List[List[str]], summary_rows: List[List[str]]) -> None:
    workbook = Workbook()

    sheet = workbook.active
    sheet.title = "Invoices"
    sheet.append([label for label, _ in COMBINED_COLUMNS])
    for row in combined_rows:
        sheet.append(row)

    summary_sheet = workbook.create_sheet("Summary")
    for row in summary_rows:
        summary_sheet.append(row)

    workbook.save(path)


def write_json(path: Path, invoices: List[Invoice]) -> None:
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "invoice_count": len(invoices),
        "invoices": [asdict(invoice) for invoice in invoices],
    }
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, default=str)


def main() -> int:
    parser = argparse.ArgumentParser(description="Batch export DANFE/NFe invoices from a directory.")
    parser.add_argument("input_dir", help="Directory containing PDF invoices.")
    parser.add_argument("--output-dir", default="", help="Output directory. Defaults to input directory.")
    parser.add_argument("--prefix", default="", help="Output file prefix. Defaults to batch_export_YYYYMMDD_HHMMSS.")
    args = parser.parse_args()

    input_dir = Path(args.input_dir).expanduser().resolve()
    if not input_dir.exists() or not input_dir.is_dir():
        raise SystemExit(f"Input directory not found: {input_dir}")

    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else input_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    prefix = args.prefix or f"batch_export_{timestamp}"

    pdf_paths = list(iter_pdfs(input_dir))
    if not pdf_paths:
        raise SystemExit(f"No PDF files found under: {input_dir}")

    print(f"[input] {len(pdf_paths)} PDFs from {input_dir}")
    invoices = parse_invoices(pdf_paths)
    combined_rows = build_combined_rows(invoices)
    summary_rows = build_summary_rows(invoices)

    xlsx_path = output_dir / f"{prefix}.xlsx"
    csv_path = output_dir / f"{prefix}.csv"
    json_path = output_dir / f"{prefix}.json"

    write_xlsx(xlsx_path, combined_rows, summary_rows)
    write_csv(csv_path, [label for label, _ in COMBINED_COLUMNS], combined_rows)
    write_json(json_path, invoices)

    print(f"[output] {xlsx_path}")
    print(f"[output] {csv_path}")
    print(f"[output] {json_path}")
    print(f"[summary] invoices={len(invoices)} detail_rows={len(combined_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
