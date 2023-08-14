import re
import sys

file = sys.argv[1]

with open(file, 'r', encoding='utf-8') as f:
    c = f.readlines()
    
for line in c:
    m = re.search(r'20090715-([0-9]{1,3})', line)
    
    #print(line)
    #print(m)
    if m:
        num = m.group(1)
        fnum = num.zfill(3)
        result = re.sub(r'20090715-([0-9]{1,3})', f'20090715-{fnum}', line)
        print(result)
