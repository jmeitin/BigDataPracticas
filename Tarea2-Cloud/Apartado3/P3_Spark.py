# COMMAND --> spark-submit P3_spark.py

# VERSION CON DATAFRAMES

from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as Functions

conf = SparkConf().setAppName('Close Prive Average')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

tabla = spark.read.option("header", "true").csv("GOOGLE.csv").drop("Open", "High", "Low", "Adj Close", "Volume")

tabla = tabla.withColumn('Date', Functions.split(tabla["Date"], '-').getItem(0))

tabla = tabla.withColumn("Close", Functions.regexp_replace(tabla["Close"], "\\.", ""))

#castear a int para poder hacer los calculso mas adelante
tabla = tabla.withColumn("Close", tabla["Close"].cast("int"))

#calcular  mean(la media)
tabla = tabla.groupBy("Date").mean("Close")

#ordenar la coliumna de fechas
tabla = tabla.sort(tabla["Date"])


#para guardar en un archivo los resultados
tabla.rdd.saveAsTextFile("output.txt")

#para mostrar en la consola los resultados
print(tabla.show(50,False))