# COMMAND --> spark-submit P2_spark.py

# VERSION CON RDDS

# from pyspark import SparkConf, SparkContext
# from operator import add

# conf = SparkConf().setAppName('URLs frecuency')
# sc = SparkContext(conf = conf)

# def getURL(line):
#     url = line.split()[6]
#     return (url, 1)

# def swapKeyValue(pair):
#     return (pair[1], pair[0])

# urlsRDD = sc.textFile('access_log').map(lambda line: getURL(line)).reduceByKey(add)\
#     .map(lambda line: swapKeyValue(line)).sortByKey(False).map(lambda line: swapKeyValue(line)).saveAsTextFile('output')


# VERSION CON DATAFRAMES

from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as Functions

conf = SparkConf().setAppName('URLs frecuency')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

# Lee el csv separando por espacios y elimina todas las columnas menos la url. Y le cambia el nombre
urlsDF = spark.read.option("delimiter", " ").csv("access_log").drop("_c0", "_c1", "_c2", "_c3", "_c4", "_c6", "_c7").withColumnRenamed("_c5", "url")

# Se queda con la url quitandole el 'GET'
urlsDF = urlsDF.withColumn('url', Functions.split(urlsDF['url'], ' ').getItem(1))

# The function DataFrame.groupBy(cols) returns a GroupedData object. 
# In order to convert a GroupedData object back to a DataFrame, 
# you will need to use one of the GroupedData functions such as mean(cols) avg(cols) count().

# Agrupa las urls igual y ordena por numero de frecuencia de mayor a menor
urlsDF = urlsDF.groupBy("url").count().sort("count", ascending = False)

urlsDF.rdd.saveAsTextFile("output")