#!/usr/bin/python

import sys

# ./P4a_mapper.py < ../../Datasets/ml-latest-small/ratings.csv | sort -k 1,1 -t $'\t' | ./P4a_reducer.py | ./P4b_mapper.py | sort -k 1,1 -t $'\t' | ./P4b_reducer.py

sys.stdin.readline()

for line in sys.stdin:

    data = line.split(',')
    movieId = data[1]
    rating = float(data[2])

    print(movieId + "\t" + str(rating))