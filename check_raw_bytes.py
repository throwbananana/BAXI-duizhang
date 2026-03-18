import sqlite3
conn = sqlite3.connect('invoice_payment.db')
cursor = conn.cursor()
cursor.execute("SELECT CAST(natureza_operacao AS BLOB) FROM payment_invoices WHERE id=48")
blob = cursor.fetchone()[0]
print(f"Hex: {blob.hex(' ')}")
try:
    print(f"As UTF-8: {blob.decode('utf-8')}")
except:
    print("Cannot decode as UTF-8")

try:
    print(f"As GBK: {blob.decode('gbk')}")
except:
    print("Cannot decode as GBK")
conn.close()
