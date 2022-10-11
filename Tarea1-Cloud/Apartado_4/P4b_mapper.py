#!/usr/bin/python

import sys

# ./P4a_mapper.py < ratings.csv | sort -k 1,1 -t $'\t' | ./P4a_reducer.py | ./P4b_mapper.py | sort -k 1,1 -t $'\t' | ./P1b_reducer.py


for line in sys.stdin:

    id, rating = line.split()

    rating = float(rating)

    if rating <= 1.0: rating = 1
    elif rating <= 2.0: rating = 2
    elif rating <= 3.0: rating = 3
    elif rating <= 4.0: rating = 4
    elif rating <= 5: rating = 5

    print(str(rating) + "\t" + id)