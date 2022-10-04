from pyspark import SparkConf, SparkContext
import sys
import re

conf = SparkConf().setAppName('WordCount')
sc = SparkContext(conf = conf)

sc.textFile(sys.argv[1])\
  .flatMap(lambda line: re.sub(r'\W+', ' ', line).split())\
  .map(lambda word: (word.lower(), 1))\
  .reduceByKey(lambda a, b: a+b)\
  .saveAsTextFile(sys.argv[2])

