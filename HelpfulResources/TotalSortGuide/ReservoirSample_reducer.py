#!/usr/bin/env python

"""
INPUT:                                                    
    data point \t reservoirIndex(j) \t streamIndex(i)             
OUTPUT:                                                   
    data point
    
Requirement: the inputs must be sorted with the 
following option: -k2,2 -k3,3n. This way all of 
records associated with position j in the reservoir
are consecutive and the one that will occupy position
j in the reservoir at the end is the one with the
highest stream index (the one that came last in
the original stream)
"""


import sys

current_j = None
current_num = None

for i, line in enumerate(sys.stdin):
    num, j, i = line.strip().split('\t')
    
    if current_j == j:
        current_num = num
    else:
        if current_j is not None:
            print(current_num)
        current_j = j
        current_num = num
        
print(current_num)
