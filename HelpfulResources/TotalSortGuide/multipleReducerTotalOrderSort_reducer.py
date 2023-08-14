#!/usr/bin/env python
"""
INPUT:
    partitionKey \t count \t word
OUTPUT:
    count \t word
"""
import sys

for line in sys.stdin:
    pkey, key, value = line.strip().split('\t')
    print(key,value)
