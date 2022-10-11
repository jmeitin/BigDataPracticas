#!/usr/bin/python

# ./P1_mapper.py Project < ../../Datasets/input.txt | sort -k 1,1 -t $'\t' | ./P1_reducer.py

# FILTERING (QUERY)

import sys
import re

wordToFind = sys.argv[1]

for line in sys.stdin:

    if line != '\n':

        words = re.sub(r'\W+', ' ', line).split()
        
        for word in words:
            if (word == wordToFind):
                print(line, end='')
                break
    
