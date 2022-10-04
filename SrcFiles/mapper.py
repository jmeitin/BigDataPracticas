#!/usr/bin/python

# permisos archivo ==> ls -l ./mapper.py 
# PERMISO DE EJECUCION ==> chmod g+x ./mapper.py 
#EJECUTAR ==> python3 mapper.py < input.txt | sort -k 1,1 -t $'\t' | python3 reducer.py
#CACA ==> python3 ./mapper.py < input.txt | sort -k 1,1 -t $'\t' | ./reducer.py

import sys
import re

for line in sys.stdin:
    words = re.sub(r'\W+', ' ', line).split()
    
    for word in words:
        print(word.lower() + "\t1")