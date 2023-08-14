import sys

results = []
for line in sys.stdin:
    id_, name = line.strip().split('\t')
    results.append(f'{id_} {name}')
print(','.join(results))
