from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from  random import randrange
import random
from operator import add
import time
sc = SparkContext()
sqlContext = SQLContext(sc)
time_sampling=''
time_cal_med=''
f=open("report_new3.txt","a")
sampling_size=0.15

def reading_file():
        ebird_file = sc.textFile("file:///home/abyad001/ds/areawate.csv")
        parts = ebird_file.map(lambda l:l.split(","))
	lat = parts.map(lambda p:(float(p[2]))).todf()
	longi = parts.map(lambda p:(float(p[3]))).todf()
	max_lat=lat.max()
	min_lat = lat.min()
        lat_long = parts.map(lambda p:(float(p[2]),float(p[3])))
	max_longi = longi.max()
	min_longi = longi.min()
        return lat_long,max_lat,min_lat,max_longi,min_longi
lat_long,max_lat,min_lat,max_longi,min_longi=reading_file()
print "jsssssssssssssssssssssssssssssssssssssssssssssssssssdf"+str(max_lat)+str(min_lat)+"\t"+str(max_longi)+"\t"+str(min_longi)

reading_file()
