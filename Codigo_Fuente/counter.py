#!/usr/bin/python

import sys
import re

sums = {}

for line in sys.stdin:
    words = re.sub(r'\W+', ' ', line).split()

    for word in words:
        word = word.lower()
        sums[word] = sums.get(word, 0) + 1

print(sums)
