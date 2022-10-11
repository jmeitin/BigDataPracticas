#!/usr/bin/python

# SUMMARIZATION (Record Count -> ANALYTICS)

import sys

for line in sys.stdin:

    lineSplitted = line.split()

    print(lineSplitted[6] + "\t1")
