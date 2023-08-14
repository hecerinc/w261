#!/usr/bin/env python3

import sys, os

options = sys.argv[1].split(',')
wrong_choice = ''
while(True):
    print(f'{wrong_choice}Please select {options[0]} from the options below:')
    print()
    for i, option in enumerate(options[1:]):
        print(f'[{i + 1}] {option.strip()}')
    print()

    try:
        selection = int(input('Selection: '))
        if 0 < selection < len(options):
            break
    except:
        pass
    wrong_choice = 'Wrong Choice\n'
    os.system('clear')
print(options[selection])
with open('${HOME}/python_selection', '+w') as fh:
    fh.write(options[selection])
