#!/usr/bin/env python
"""
INPUT:                                                    
    count \t word                
OUTPUT:                                                   
    partitionKey \t count \t word
"""

import os
import re                                                   
import sys                                                  
import numpy as np      
from operator import itemgetter


if os.getenv('mapreduce_job_reduces') == None:
    N = 1
else:
    N = int(os.getenv('mapreduce_job_reduces'))

# helper functions
def makeIndex(key, num_reducers = N):
    """
    Mimic the Hadoop string-hash function.
    
    key             the key that will be used for partitioning
    num_reducers    the number of reducers that will be configured
    """
    byteof = lambda char: int(format(ord(char), 'b'), 2)
    current_hash = 0
    for c in key:
        current_hash = (current_hash * 31 + byteof(c))
    return current_hash % num_reducers

# helper function
def makeKeyFile(num_reducers = N):
    KEYS = list(map(chr, range(ord('A'), ord('Z')+1)))[:num_reducers]
    partition_keys = sorted(KEYS, key=lambda k: makeIndex(k,num_reducers))

    return partition_keys


# call your helper function to get partition keys
pKeys = makeKeyFile()

def makePartitionFile():
    # returns a list of split points
    return [20,10,0]

pFile = makePartitionFile()

for line in sys.stdin: 
    line = line.strip()
    key,value = line.split('\t')

    # Prepend the approriate key by finding the bucket, and using the index to fetch the key.
    for idx in range(N):
        if float(key) > pFile[idx]:
            print(str(pKeys[idx])+"\t"+key+"\t"+value)
            break
