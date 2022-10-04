from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
import re

# create Spark configuration and context
conf = SparkConf().setAppName("TwitterStreamApp")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# create Streaming Context with window size of 10 sec
ssc = StreamingContext(sc, 10)
# set a checkpoint directory (required by updateStateByKey)
ssc.checkpoint("checkpoint_TwitterApp")
# read data from localhost:9009
dataStream = ssc.socketTextStream("localhost", 9009)

def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

def process_rdd(rdd):
    if not rdd.isEmpty():
        print("--- Aggregated ---")
        print(rdd.takeOrdered(20, lambda x: -x[1]))

# get hashtags, map each one to be a pair of (hashtag,1) and add in the period
hashtags = dataStream.flatMap(lambda line: re.findall(r'#\w+', line))\
    .map(lambda x: (x.lower(), 1))\
    .reduceByKey(lambda x, y: x + y)
hashtags.pprint(100)

# accumulate the state
tags_totals = hashtags.updateStateByKey(aggregate_tags_count)
# process the state generated in each interval as RDD
tags_totals.foreachRDD(process_rdd)

# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()
