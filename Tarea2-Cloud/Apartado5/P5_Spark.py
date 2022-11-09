from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as Functions

conf = SparkConf().setAppName('Avr Mass')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

# Se lee el csv con la cabecera
meteoriteDF = spark.read.option("header", "true").csv("Meteorite_Landings.csv")

meteoriteDF = meteoriteDF.drop("id", "name", "nametype", "fall", "year", "reclat", "reclong", "GeoLocation")

meteoriteDF = meteoriteDF.withColumn("mass (g)", meteoriteDF["mass (g)"].cast("float"))

# Se calcula la media
meteoriteDF = meteoriteDF.groupBy("recclass").mean("mass (g)").sort("recclass")

meteoriteDF.rdd.saveAsTextFile("output")

meteoriteDF.show()