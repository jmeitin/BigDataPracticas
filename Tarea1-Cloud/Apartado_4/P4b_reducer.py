#!/usr/bin/python

import sys

previousRange = None
moviesInRange = []

for line in sys.stdin:
    range, id = line.split()
    
    if range != previousRange:
        if previousRange is not None:
            print(f"Range {previousRange}: {moviesInRange}\n")
        previousRange = range

    moviesInRange.append(id)

print(f"Range {range}: {moviesInRange}")
