# COMMAND --> spark-submit P3_spark.py

# VERSION CON DATAFRAMES

from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as Functions

conf = SparkConf().setAppName('Close Avg Price')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

# Lee el csv eliminando las columnas que no interesan
priceDF = spark.read.option("header", "true").csv("GOOGLE.csv").drop("Open", "High", "Low", "Adj Close", "Volume")

# La fecha tiene el formato YY-MM-DD, y lo que enteresa para este ejercicio es YY.
priceDF = priceDF.withColumn('Date', Functions.split(priceDF["Date"], '-').getItem(0))

# Castea la columna con los valores (Close) a float para hacer el avg posteriormente.
priceDF = priceDF.withColumn("Close", priceDF["Close"].cast("float"))

# Agrupa por años haciendo el avg y finalmente ordena por año
priceDF = priceDF.groupBy("Date").mean("Close").sort(priceDF["Date"])

priceDF.rdd.saveAsTextFile("output.txt")

priceDF.show()