#!/usr/bin/env python
"""
Reducer aggregates word counts by class and emits frequencies.

INPUT:
    partitionKey \t word \t ham_partialCount,spam_partialCount
OUTPUT:
    word \t ham_count,spam_count,P(word|ham),P(word|spam)
    
Instructions:
    Again, you are free to design a solution however you see 
    fit as long as your final model meets our required format
    for the inference job we designed in Question 8. Please
    comment your code clearly and concisely.
    
    A few reminders: 
    1) Don't forget to emit Class Priors (with the right key).
    2) In python2: 3/4 = 0 and 3/float(4) = 0.75
"""
##################### YOUR CODE HERE ####################
import sys

cur_word = None
cur_spam_count = 0
cur_ham_count = 0

def get_counts_from_payload(key, payload):
    if key.startswith("!total"):
        return int(payload), 0
    ham_partialCount, spam_partialCount = payload.split(',')
    ham_partialCount = int(ham_partialCount)
    spam_partialCount = int(spam_partialCount)
    return ham_partialCount, spam_partialCount

TOTAL_HAM  = 0 # docs
TOTAL_SPAM = 0 # docs
TOTAL_HAM_WORDS  = 0
TOTAL_SPAM_WORDS = 0

for line in sys.stdin:
    try:
        part_key, key, payload = line.strip().split("\t")
    except:
        print("OFFENDER:", line, file=sys.stderr)
        raise Exception("Failed to split")

    
    # tally counts from current key
    if key == cur_word:
        if key.startswith("!total"):
            if key == '!total_spam_words':
                TOTAL_SPAM_WORDS += int(payload)
            elif key == "!total_ham_words":
                TOTAL_HAM_WORDS += int(payload)
            elif key == "!total_ham":
                TOTAL_HAM += int(payload)
            else:
                TOTAL_SPAM += int(payload)
        else:
            ham_partialCount, spam_partialCount = get_counts_from_payload(key, payload)
            cur_spam_count += spam_partialCount
            cur_ham_count  += ham_partialCount
    else:
        # Store word count totals (for both classes)
        if key.startswith("!total"):
            if key == '!total_spam_words':
                TOTAL_SPAM_WORDS = int(payload)
            elif key == "!total_ham_words":
                TOTAL_HAM_WORDS = int(payload)
            elif key == "!total_ham":
                TOTAL_HAM = int(payload)
            else:
                TOTAL_SPAM = int(payload)
        # emit relative freq
        if cur_word and not cur_word.startswith('!total'):
            print(f'{cur_word}\t{cur_ham_count}\t{cur_spam_count}\t{cur_ham_count/float(TOTAL_HAM_WORDS) if TOTAL_HAM_WORDS != 0 else 0}\t{cur_spam_count/float(TOTAL_SPAM_WORDS) if TOTAL_SPAM_WORDS != 0 else 0}')

        # start a new tally
        cur_ham_count, cur_spam_count = get_counts_from_payload(key, payload)
        cur_word = key

# Calc + emit priors
TOTAL_DOCS = TOTAL_HAM + TOTAL_SPAM
spam_prior = TOTAL_SPAM/float(TOTAL_DOCS) if TOTAL_DOCS != 0 else 0
ham_prior  = TOTAL_HAM/float(TOTAL_DOCS) if TOTAL_DOCS != 0 else 0
print(f"ClassPriors\t{TOTAL_HAM}\t{TOTAL_SPAM}\t{ham_prior}\t{spam_prior}")

# If there was nothing in the file (rare, but could occur), then let's make sure to not print the !total sums
if cur_word and not cur_word.startswith('!total'):
    # don't forget the last record!
    print(f'{cur_word}\t{cur_ham_count}\t{cur_spam_count}\t{cur_ham_count/float(TOTAL_HAM_WORDS) if TOTAL_HAM_WORDS != 0 else 0}\t{cur_spam_count/float(TOTAL_SPAM_WORDS) if TOTAL_SPAM_WORDS != 0 else 0}')

##################### (END) CODE HERE ####################