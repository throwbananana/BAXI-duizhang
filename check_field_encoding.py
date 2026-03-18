import sqlite3
import re

def is_garbage(s):
    if not s: return False
    # Check for typical UTF-8 misread as Latin-1/GBK patterns
    # Like Ã followed by something, or chinese characters that look like noise
    if "Ã" in s or "©" in s or "Â" in s:
        return True
    # Check for Mojibake (UTF-8 as GBK)
    # Common nonsense Chinese characters in Mojibake: 浠, 撳, 偍, 閿, €, 鍞
    if re.search(r'[浠撳偍閿€鍞]', s):
        return True
    return False

def check_encoding():
    try:
        conn = sqlite3.connect('invoice_payment.db')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        print(f"{'ID':<5} | {'Field':<20} | {'Value'}")
        print("-" * 60)
        
        cursor.execute("SELECT id, issuer_name, natureza_operacao FROM payment_invoices")
        rows = cursor.fetchall()
        
        garbage_count = 0
        for row in rows:
            for field in ['issuer_name', 'natureza_operacao']:
                val = row[field]
                if is_garbage(val):
                    print(f"{row['id']:<5} | {field:<20} | {val}")
                    garbage_count += 1
        
        if garbage_count == 0:
            print("No obvious garbage detected in investigated fields.")
            # Let's print a sample to see what's actually there
            if rows:
                print("\nSample Data:")
                r = rows[0]
                print(f"ID: {r['id']}, Issuer: {r['issuer_name']}, Natureza: {r['natureza_operacao']}")
        else:
            print(f"\nTotal potential garbage fields found: {garbage_count}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_encoding()
