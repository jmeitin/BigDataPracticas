#!/usr/bin/python

# SUMMARIZATION (Average -> ANALYTICS)

import sys

sys.stdin.readline()

for line in sys.stdin:

    data = line.split(',')
    date = data[0].split('-')
    year = date[0]

    print(year + "\t" + str(float(data[4])))
