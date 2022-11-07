from pyspark import SparkConf, SparkContext
import sys
import re

# spark-submit P1_spark.py word file

conf = SparkConf().setAppName('Grep with Spark')
sc = SparkContext(conf = conf)

word = sys.argv[1]
file = sys.argv[2]

def makeStringArray(string_list):
    str1 = ""
 
    for ele in string_list:
        str1 += ele + " "
 
    return str1

mobydickRDD = sc.textFile(file).map(lambda line: re.sub(r'\W+', ' ', line).split())\
    .filter(lambda line: line.count(word)).map(lambda line: makeStringArray(line)).saveAsTextFile("output")