from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as Functions

# spark-submit P2_spark.py

conf = SparkConf().setAppName('URLs frecuency')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

tabla = spark.read.option("delimiter", " ").csv("access_log").drop("_c0", "_c1", "_c2", "_c3", "_c4", "_c6", "_c7").withColumnRenamed("_c5", "url")

tabla = tabla.withColumn('url', Functions.split(tabla['url'], ' ').getItem(1))

tabla = tabla.groupBy("url").count().sort("count", ascending = False)

tabla.rdd.saveAsTextFile("output")

tabla.show(50, False)


