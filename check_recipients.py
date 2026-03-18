import sqlite3
conn = sqlite3.connect('invoice_payment.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()
cursor.execute("SELECT id, invoice_number, issuer_name, destinatario_name FROM payment_invoices LIMIT 10")
rows = cursor.fetchall()
print(f"{'ID':<5} | {'Inv Num':<15} | {'Issuer':<30} | {'Recipient'}")
print("-" * 80)
for r in rows:
    print(f"{r['id']:<5} | {r['invoice_number']:<15} | {r['issuer_name'][:30]:<30} | {r['destinatario_name']}")
conn.close()
