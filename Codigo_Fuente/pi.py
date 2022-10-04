from pyspark import SparkConf, SparkContext
from time import time

conf = SparkConf().setAppName('Pi')
sc = SparkContext(conf = conf)

N = 100_000_000
delta_x = 1.0 / N

t_0 = time()

pi = sc.parallelize(range(N))\
       .map(lambda i: (i + 0.5)*delta_x)\
       .map(lambda x: 4/(1 + x**2))\
       .reduce(lambda a, b: a + b)*delta_x

print("Pi is", pi)
print(time() - t_0, "seconds elapsed for N =", N)
