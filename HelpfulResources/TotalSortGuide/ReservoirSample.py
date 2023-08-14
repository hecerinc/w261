#!/usr/bin/env python

import sys
import numpy as np

#########################################################
#  Sample n numbers uniformly at random
#########################################################

print(sys.argv)

R = 5 # size of the reservoir
res = [] # reservoir

for i, line in enumerate(sys.stdin):
    num = line.strip()
    if len(res) < R:
        res.append(num)
    else:
        j = np.random.randint(i)
        if j < R:
            res[j] = num

# print 
for j in res:
    print(j)
