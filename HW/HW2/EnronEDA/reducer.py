#!/usr/bin/env python
"""
Reducer takes words with their class and partial counts and computes totals.
INPUT:
    word \t class \t partialCount 
OUTPUT:
    word \t class \t totalCount  
"""
import re
import sys

# initialize trackers
current_word = None
spam_count, ham_count = 0,0

# read from standard input
for line in sys.stdin:
    # parse input
    word, is_spam, count = line.split('\t')
    
############ YOUR CODE HERE #########
    is_spam = int(is_spam)
    # tally counts from current key
    if word == current_word:
        if is_spam == 1:
            spam_count += int(count)
        else:
            ham_count += int(count)
    # OR emit current total and start a new tally 
    else:
        if current_word:
            # Print HAM (0) count
            print(f'{current_word}\t0\t{ham_count}')
            # Print SPAM (1) count
            print(f'{current_word}\t1\t{spam_count}')
        if is_spam == 1:
            current_word, spam_count = word, int(count)
            ham_count = 0
        else:
            current_word, ham_count  = word, int(count)            
            spam_count = 0

# don't forget the last record! 
# Print HAM (0) count
print(f'{current_word}\t0\t{ham_count}')
# Print SPAM (1) count
print(f'{current_word}\t1\t{spam_count}')




############ (END) YOUR CODE #########