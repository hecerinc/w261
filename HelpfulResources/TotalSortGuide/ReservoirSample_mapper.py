#!/usr/bin/env python
"""
INPUT:                                                    
    data point  
    R -> size of the reservoir
OUTPUT:                                                   
    data point \t reservoirIndex(j) \t streamIndex(i) 
"""
import os
import sys
import numpy as np


if os.getenv('R') == None:
    R = 3
else:
    R = int(os.getenv('R'))

for i, line in enumerate(sys.stdin):
    num = line.strip()
    
    if i < R:
        print(f'{num}\t{i}\t{i}')
    else:
        j = np.random.randint(i) 
        if j < R: 
            print(f'{num}\t{j}\t{i}')
