from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import row_number, monotonically_increasing_id
from pyspark.sql.window import Window

conf = SparkConf().setAppName('Avg Ratings')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

# Se lee el csv
ratingsDF = spark.read.option("header", "true").csv("ratings.csv").drop("userId", "timeStamp")

# Se castea a float para calcular el avg posteriormente
ratingsDF = ratingsDF.withColumn("rating", ratingsDF["rating"].cast("float"))

# Se castean los id de las peliculas a int para ordenar el dataframe
ratingsDF = ratingsDF.withColumn("movieId", ratingsDF["movieId"].cast("int"))

# Calculamos la media agrupando por año
ratingsDF = ratingsDF.groupBy("movieId").mean("rating").sort(ratingsDF["movieId"])

# Se agrupa por distintos rangos
range1 = ratingsDF.filter(ratingsDF["avg(rating)"] <= 1)\
	.drop("avg(rating)")\
	.withColumn("Range 1", ratingsDF["movieId"])\
	.drop("movieId")

range2 = ratingsDF.filter((ratingsDF["avg(rating)"] > 1) & (ratingsDF["avg(rating)"] <= 2))\
	.drop("avg(rating)")\
	.withColumn("Range 2", ratingsDF["movieId"])\
	.drop("movieId")

range3 = ratingsDF.filter((ratingsDF["avg(rating)"] > 2) & (ratingsDF["avg(rating)"] <= 3))\
	.drop("avg(rating)")\
	.withColumn("Range 3", ratingsDF["movieId"])\
	.drop("movieId")

range4 = ratingsDF.filter((ratingsDF["avg(rating)"] > 3) & (ratingsDF["avg(rating)"] <= 4))\
	.drop("avg(rating)")\
	.withColumn("Range 4", ratingsDF["movieId"])\
	.drop("movieId")

range5 = ratingsDF.filter((ratingsDF["avg(rating)"] > 4) & (ratingsDF["avg(rating)"] <= 5))\
	.drop("avg(rating)")\
	.withColumn("Range 5", ratingsDF["movieId"])\
	.drop("movieId")

# Se unen todos los dataframes
range1 = range1.withColumn('n_row', row_number().over(Window.orderBy(monotonically_increasing_id())))
range2 = range2.withColumn('n_row', row_number().over(Window.orderBy(monotonically_increasing_id())))
range3 = range3.withColumn('n_row', row_number().over(Window.orderBy(monotonically_increasing_id())))
range4 = range4.withColumn('n_row', row_number().over(Window.orderBy(monotonically_increasing_id())))
range5 = range5.withColumn('n_row', row_number().over(Window.orderBy(monotonically_increasing_id())))

dataframe = range1.join(range2, on=["n_row"])
dataframe = dataframe.join(range3, on=["n_row"])

dataframe = dataframe.join(range4, on=["n_row"])
dataframe = dataframe.join(range5, on=["n_row"]).drop("n_row")

dataframe.rdd.saveAsTextFile("output")