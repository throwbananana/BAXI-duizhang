import sqlite3

db_path = "invoice_payment.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("--- Invoices vs Installments Summary ---")
cursor.execute("SELECT count(*) FROM payment_invoices")
print(f"Total Invoices: {cursor.fetchone()[0]}")

cursor.execute("SELECT count(*) FROM payment_installments")
print(f"Total Installments: {cursor.fetchone()[0]}")

cursor.execute("SELECT sum(total_amount) FROM payment_invoices")
print(f"Sum of Invoice total_amount: {cursor.fetchone()[0]}")

cursor.execute("SELECT sum(amount) FROM payment_installments")
print(f"Sum of Installment amount: {cursor.fetchone()[0]}")

cursor.execute("SELECT sum(penalty) FROM payment_installments")
print(f"Sum of Installment penalty: {cursor.fetchone()[0]}")

print("\n--- Check for Mismatched Invoices (Invoice Total != Sum of Installments) ---")
cursor.execute("""
    SELECT i.id, i.invoice_number, i.total_amount, SUM(pi.amount) as inst_sum
    FROM payment_invoices i
    JOIN payment_installments pi ON i.id = pi.invoice_id
    GROUP BY i.id
    HAVING abs(i.total_amount - inst_sum) > 0.01
""")
mismatches = cursor.fetchall()
if mismatches:
    for m in mismatches:
        print(f"Invoice {m[1]} (ID {m[0]}): Invoice Total={m[2]}, Installments Total={m[3]}, Diff={m[2]-m[3]}")
else:
    print("No mismatches found between Invoice total and Installment sum.")

conn.close()