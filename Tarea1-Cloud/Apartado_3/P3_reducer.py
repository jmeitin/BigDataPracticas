#!/usr/bin/python

import sys

previousYear = None
sum = 0
numDaysInYear = 0

for line in sys.stdin:
    year, value = line.split('\t')
    
    if year != previousYear:
        if previousYear is not None:
            print(str(float(sum/numDaysInYear)) + '\t' + previousYear)
        previousYear = year
        sum = 0
        numDaysInYear = 0

    numDaysInYear += 1
    sum = sum + float(value)

print(str(float(sum/numDaysInYear)) + '\t' + year)
