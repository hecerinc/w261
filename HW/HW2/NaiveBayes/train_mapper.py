#!/usr/bin/env python
"""
Mapper reads in text documents and emits word counts by class.
INPUT:
    DocID \t true_class \t subject \t body
OUTPUT:
    partitionKey \t word \t ham_partialCount,spam_partialCount

Instructions:
    You know what this script should do, go for it!
    (As a favor to the graders, please comment your code clearly!)
    
    A few reminders:
    1) To make sure your results match ours please be sure
       to use the same tokenizing that we have provided in
       all the other jobs:
         words = re.findall(r'[a-z]+', text-to-tokenize.lower())
         
    2) Don't forget to handle the various "totals" that you need
       for your conditional probabilities and class priors.
       
Partitioning:
    In order to send the totals to each reducer, we need to implement
    a custom partitioning strategy.
    
    We will generate a list of keys based on the number of reduce tasks 
    that we read in from the environment configuration of our job.
    
    We'll prepend the partition key by hashing the word and selecting the
    appropriate key from our list. This will end up partitioning our data
    as if we'd used the word as the partition key - that's how it worked
    for the single reducer implementation. This is not necessarily "good",
    as our data could be very skewed. However, in practice, for this
    exercise it works well. The next step would be to generate a file of
    partition split points based on the distribution as we've seen in 
    previous exercises.
    
    Now that we have a list of partition keys, we can send the totals to 
    each reducer by prepending each of the keys to each total.
       
"""

import re                                                   
import sys                                                  
import numpy as np      

from operator import itemgetter
import os

#################### YOUR CODE HERE ###################


NUM_REDUCERS = int(os.environ['mapreduce_job_reduces']) if 'mapreduce_job_reduces' in os.environ else 4

KEYS = list(map(chr, range(ord('A'), ord('Z')+1)))[:NUM_REDUCERS]

def makeKeyHash(key, num_reducers):
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

TOTAL_HAM  = 0
TOTAL_SPAM = 0
TOTAL_HAM_WORDS  = 0
TOTAL_SPAM_WORDS = 0

# read from standard input
for line in sys.stdin:
    # parse input
    docID, _class, subject, body = line.split('\t')
    # tokenize
    words = re.findall(r'[a-z]+', (subject + ' ' + body).lower())

    for word in words:
        key = KEYS[makeKeyHash(word, NUM_REDUCERS)]
        payload = '1,0' if int(_class) == 0 else '0,1'
        print(f"{key}\t{word}\t{payload}")
    if int(_class) == 0:
        TOTAL_HAM_WORDS += len(words)
        TOTAL_HAM += 1
    else:
        TOTAL_SPAM_WORDS += len(words)
        TOTAL_SPAM += 1

for pkey in KEYS[:NUM_REDUCERS]:
    print(f"{pkey}\t!total_spam\t{TOTAL_SPAM}")
    print(f"{pkey}\t!total_ham\t{TOTAL_HAM}")
    print(f"{pkey}\t!total_spam_words\t{TOTAL_SPAM_WORDS}")
    print(f"{pkey}\t!total_ham_words\t{TOTAL_HAM_WORDS}")




















#################### (END) YOUR CODE ###################