from pyspark import SparkConf, SparkContext
import sys
import re

# spark-submit P1_spark.py word file

conf = SparkConf().setAppName('Grep with Spark')
sc = SparkContext(conf = conf)

word = sys.argv[1].lower()
file = sys.argv[2]

def findWord(line):

    words = re.sub(r'\W+', ' ', line).split()

    for w in words:
        if word == w.lower():
            return True

    return False
    



mobydickRDD = sc.textFile(file)\
    .filter(lambda line: findWord(line)).saveAsTextFile("output")