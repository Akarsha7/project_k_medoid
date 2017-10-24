from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from  random import randrange
import random
from operator import add

sc = SparkContext()
sqlContext = SQLContext(sc)
Employee_rdd = sc.textFile("file:///home/abyad001/Downloads/lat_long.csv").map(lambda line: line.split(","))

df = Employee_rdd.toDF(['latitude','longitude'])
df[0].dataType=FloatType()
df[1].dataType=FloatType()
print df.take(10) 
