from pyspark import SparkConf, SparkContext
import sys

# spark-submit P1_spark.py word file

conf = SparkConf().setAppName('Grep with Spark')
sc = SparkContext(conf = conf)

word = sys.argv[1]
file = sys.argv[2]

mobydickRDD = sc.textFile(file).filter(lambda line: word in line.lower()).saveAsTextFile("output.txt")