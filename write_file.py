from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from  random import randrange
import random
from operator import add

sc = SparkContext()
sqlContext = SQLContext(sc)
def reading_file():
        ebird_file = sc.textFile("file:///home/abyad001/ds/areawater.csv")
        rdd = ebird_file.map(lambda p: p.split(","))
        header = rdd.first()
        rdd = rdd.filter(lambda line:line != header)
        df = rdd.map(lambda line: Row(Lat = float(line[2]),Longi=float(line[3]))).toDF()
	f=open("f2.txt",'w')
	line ="hello"
	f.write(line)



reading_file()
