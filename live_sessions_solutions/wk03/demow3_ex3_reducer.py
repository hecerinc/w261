# reducer.py
#!/usr/bin/env python
import sys

# initialize trackers
cur_word = None
cur_count = 0
total = 0

# read input key-value pairs from standard input
for line in sys.stdin:
    part, key, value = line.split()
    # USE counters to see if everything is tallied correctly
    #   when you run the Hadoop, look at the output from the job for the COUNTER info
    sys.stderr.write(f'reporter:counter:MyCounters,{part},1\n')
    # tally counts from current key
    if key == cur_word:
        cur_count += int(value)
    else:
        # store word count total
        if cur_word == '!total':
            total = float(cur_count)
        # emit relative frequency
        if cur_word and cur_word != '!total':
            print(f'{cur_word}\t{cur_count/total}')
        # and start a new tally
        cur_word, cur_count = key, int(value)

# don't forget the last record!
print(f'{cur_word}\t{cur_count/total}')
