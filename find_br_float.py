
with open('brazil_product_code_v1.02.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()
    for i, line in enumerate(lines):
        if "def br_to_float" in line:
            print(f"{i+1}: {line.strip()}")
            # print next few lines
            for j in range(1, 40):
                print(f"{i+1+j}: {lines[i+j].strip()}")
            break
