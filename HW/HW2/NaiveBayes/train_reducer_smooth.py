#!/usr/bin/env python
'''
INPUT:

word \t ham_count,spam_count,P(word|ham),P(word|spam)
1. Sum the VOCAB_SIZE keywords to get the total VOCABULARY_SIZE
2. Store the ClassPriors
3. Process the words
'''
import os
import sys                                                  
import numpy as np  

#################### YOUR CODE HERE ###################

cur_word = None
cur_spam_count = 0
cur_ham_count = 0

# Read from env
VOCAB_SIZE = int(os.getenv('VOCAB', default=6))

def get_counts_from_payload(key, payload):
    if key == "!ClassPriors":
        return 0,0,0,0
    ham_partialCount, spam_partialCount, total_ham, total_spam = payload.split(',')
    return int(ham_partialCount), int(spam_partialCount), int(total_ham), int(total_spam)

TOTAL_HAM  = 0 # docs
TOTAL_SPAM = 0 # docs
TOTAL_HAM_WORDS  = 0
TOTAL_SPAM_WORDS = 0
HAM_PRIOR = 0
SPAM_PRIOR = 0

for line in sys.stdin:
    try:
        key, payload = line.strip().split("\t")
    except:
        print("OFFENDER:", line, file=sys.stderr)
        raise Exception("Failed to split")

    
    # tally counts from current key
    if key == cur_word:
        if key == "!VOCAB_SIZE":
            VOCAB_SIZE += int(payload)
    else:
        # Store word count totals (for both classes)
        if key == "!VOCAB_SIZE":
            VOCAB_SIZE = int(payload)
        elif key == "!ClassPriors":
            priors = payload.split(',')
            assert len(priors) == 4, "Length of priors != 4"
            priors = [float(x) for x in priors]
            TOTAL_HAM = priors[0]
            TOTAL_SPAM = priors[1]
            HAM_PRIOR = priors[2]
            SPAM_PRIOR = priors[3]

        # emit relative freq
        if cur_word and cur_word != '!ClassPriors' and cur_word != "!VOCAB_SIZE":
            pword_ham = (cur_ham_count + 1) / float(TOTAL_HAM_WORDS + VOCAB_SIZE)
            pword_spam = (cur_spam_count + 1) / float(TOTAL_SPAM_WORDS + VOCAB_SIZE) 
            print(f'{cur_word}\t{float(cur_ham_count)},{float(cur_spam_count)},{pword_ham},{pword_spam}')

        # start a new tally
        cur_ham_count, cur_spam_count, TOTAL_HAM_WORDS, TOTAL_SPAM_WORDS = get_counts_from_payload(key, payload)
        cur_word = key

# If there was nothing in the file (rare, but could occur), then let's make sure to not print the !total sums
# don't forget the last record!
if cur_word and cur_word != '!ClassPriors' and cur_word != "!VOCAB_SIZE":
    pword_ham = (cur_ham_count + 1) / float(TOTAL_HAM_WORDS + VOCAB_SIZE) 
    pword_spam = (cur_spam_count + 1) / float(TOTAL_SPAM_WORDS + VOCAB_SIZE) 
    print(f'{cur_word}\t{cur_ham_count},{cur_spam_count},{pword_ham},{pword_spam}')


print(f"ClassPriors\t{TOTAL_HAM},{TOTAL_SPAM},{HAM_PRIOR},{SPAM_PRIOR}")































#################### (END) YOUR CODE ###################