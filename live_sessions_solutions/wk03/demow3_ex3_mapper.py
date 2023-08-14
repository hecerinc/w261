# mapper.py

#!/usr/bin/env python

import re
import sys

def getPartitionKey(word):
    if word[0] < 'g':
        return 'A'
    elif word[0] < 'n':
        return 'B'
    elif word[0] < 't':
        return 'C'
    return 'D'

# initialize local aggregator for total word count
TOTAL_WORDS = 0
# read from standard input
for line in sys.stdin:
    line = line.strip()
    # tokenize
    words = re.findall(r'[a-z]+', line.lower())
    # emit words and increment total counter
    for word in words:
        TOTAL_WORDS += 1
        pkey = getPartitionKey(word)
        print(f'{pkey}\t{word}\t{1}')

# emit ottal count to each partition (note this is a partial total)
for pkey in ['A', 'B', 'C', 'D']:
    print(f'{pkey}\t!total\t{TOTAL_WORDS}')
