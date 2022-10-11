#!/usr/bin/python

import sys

previousMovieId = None
sumOfRatings = 0
moviesWithSameId = 0

for line in sys.stdin:
    id, rating = line.split()
    
    if id != previousMovieId:
        if previousMovieId is not None:
            print(previousMovieId + '\t' + str(round(float(sumOfRatings/moviesWithSameId), 1)))
        previousMovieId = id
        sumOfRatings = 0
        moviesWithSameId = 0

    moviesWithSameId += 1
    sumOfRatings = sumOfRatings + float(rating)

print(id + '\t' + str(round(float(sumOfRatings/moviesWithSameId), 1)))
