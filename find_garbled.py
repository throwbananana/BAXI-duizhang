with open('brazil_product_code_v1.02.py', 'rb') as f:
    for i, line in enumerate(f):
        if b'\xe9\x9a\x91\xe8\x97\x8f\xe6\xad\xa4\xe5\x88\x97' in line: # "隐藏此列" in UTF-8
            print(f"Line {i+1}: {line}")
            print(f"Hex: {line.hex(' ')}")
        elif b'\xef\xbf\xbd' in line: # Unicode Replacement Character (garbage)
            print(f"Garbage at Line {i+1}: {line}")
