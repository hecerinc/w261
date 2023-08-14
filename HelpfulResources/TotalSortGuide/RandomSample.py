#!/usr/bin/env python

import sys
import numpy as np

#########################################################
#  Emit a random sample of 1/100th of the data
#########################################################

for line in sys.stdin:
    s = np.random.uniform(0,1)
    if s < .01: 
        print(line.strip())
