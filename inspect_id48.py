import sqlite3
conn = sqlite3.connect('invoice_payment.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()
cursor.execute("SELECT * FROM payment_invoices WHERE id=48")
row = cursor.fetchone()
if row:
    for k in row.keys():
        print(f"{k}: {row[k]}")
conn.close()
