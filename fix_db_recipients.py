import json
import sqlite3
import os

def fix_missing_recipients():
    json_path = "danfe_data_autosave.json"
    db_path = "invoice_payment.db"
    
    if not os.path.exists(json_path):
        print("Autosave JSON not found.")
        return

    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    invoices = data.get('invoices', [])
    print(f"Read {len(invoices)} invoices from JSON.")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    update_count = 0
    for inv in invoices:
        num = inv.get('numero')
        dest_name = inv.get('destinatario_name')
        dest_cnpj = inv.get('destinatario_cnpj')
        issuer_cnpj = inv.get('emitente_cnpj')
        
        if num and dest_name:
            cursor.execute("""
                UPDATE payment_invoices 
                SET destinatario_name = ?, destinatario_cnpj = ?, issuer_cnpj = ?
                WHERE invoice_number = ?
            """, (dest_name, dest_cnpj, issuer_cnpj, num))
            if cursor.rowcount > 0:
                update_count += 1
    
    conn.commit()
    conn.close()
    print(f"Successfully updated {update_count} invoices in database.")

if __name__ == "__main__":
    fix_missing_recipients()
