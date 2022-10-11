#!/usr/bin/python

import sys

previousType = None
mass_sum = 0
nMeteoriteWithSameType = 0

for line in sys.stdin:
    type, mass = line.split('\t')

    # La masa puede ser un caracter vacio por lo que se produce un error al intentar convertirlo a float
    if mass.count('.') == 1: mass = float(mass)
    else:
        mass = mass.replace(".", "")

        if mass.isnumeric(): mass = int(mass.replace("\n", ""))
        else: mass = 0
    
    if type != previousType:
        if previousType is not None:
            print(previousType + '\t' + str(float(mass_sum / nMeteoriteWithSameType)))
        previousType = type
        mass_sum = 0
        nMeteoriteWithSameType = 0

    nMeteoriteWithSameType += 1
    mass_sum = mass_sum + float(mass)

print(type + '\t' + str(float(mass_sum / nMeteoriteWithSameType)))
