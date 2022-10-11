#!/usr/bin/python

# SUMMARIZATION (Average -> ANALYTICS)

import sys

sys.stdin.readline()

for line in sys.stdin:

    linea = line.split(',')
    nameType = linea[3]
    mass = linea[4]

    # En caso de que el string del tipo de meteorito tenga una coma
    if  len(linea) > 11:        
        nameType = linea[3] + linea[4]
        mass = linea[5]
    
    print(nameType + "\t" + str(mass))
