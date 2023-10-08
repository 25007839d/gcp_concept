
import findspark
import six

findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[1]').appName('sparkcode').getOrCreate()

#sc.parallelize()
rdd_small = spark.sparkContext.parallelize([3, 1, 12, 6, 8, 10, 14, 19])
print(rdd_small.reduce(lambda x,y:x+y))

