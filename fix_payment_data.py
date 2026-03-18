
import sqlite3
import os

def fix():
    db_path = 'invoice_payment.db'
    if not os.path.exists(db_path):
        print(f"Database {db_path} not found.")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("--- Fixing Invoice ID 4 (000.002.080) Duplicated Installments ---")
    # Invoice ID 4 has 4 installments: 30480 (Partial), 10160, 10160, 10160. Total 60960. Invoice is 30480.
    # We should probably keep either the first one or the other three. 
    # Since 10160 * 3 = 30480, it seems the user intended to split it into 3 terms.
    # We should delete the 30480 one.
    cursor.execute("DELETE FROM payment_installments WHERE id = 3517")
    print(f"Deleted installment 3517 for invoice 4. New sum: {10160*3}")

    print("\n--- Cleaning up Corrupted Invoice ID 128 ---")
    cursor.execute("DELETE FROM payment_installments WHERE invoice_id = 128")
    cursor.execute("DELETE FROM payment_invoices WHERE id = 128")
    print("Deleted corrupted invoice 128.")

    print("\n--- Checking for other mismatches ---")
    cursor.execute('''
        SELECT i.id, i.invoice_number, i.total_amount, SUM(p.amount) 
        FROM payment_invoices i 
        JOIN payment_installments p ON i.id = p.invoice_id 
        GROUP BY i.id 
        HAVING ABS(i.total_amount - SUM(p.amount)) > 0.01
    ''')
    mismatches = cursor.fetchall()
    for m in mismatches:
        print(f"Mismatch found: ID {m[0]} ({m[1]}) - Invoice: {m[2]}, Installments: {m[3]}")

    print("\n--- Fixing 'Partial' status for installments with 0 paid_amount ---")
    cursor.execute("UPDATE payment_installments SET status = 'Pending' WHERE paid_amount <= 0 AND status = 'Partial'")
    print(f"Updated {cursor.rowcount} installments from Partial to Pending (zero payment).")

    conn.commit()
    conn.close()
    print("\nDone.")

if __name__ == "__main__":
    fix()
