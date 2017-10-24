from pyspark.sql import functions as F
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
result_str=''
time_sampling=''
time_cal_med=''
sampling_size=0.1
f=open("binning_op","w")
def reading_file():
        ebird_file = sc.textFile("file:///home/abyad001/ds/areawater.csv")
        rdd = ebird_file.map(lambda p: p.split(","))
        header = rdd.first()
        rdd = rdd.filter(lambda line:line != header)
        lat = rdd.map(lambda line: Row(Lat = float(line[2]))).toDF()
	longitude = rdd.map(lambda line:Row(Longitude = float(line[3]))).toDF()
	df = rdd.map(lambda line: Row(Lat = float(line[2]),Longi=float(line[3]))).toDF()
        t1 = time.time()
	return df
        #print  df.take(10)

def create_bin():
	lat_long_rdd = reading_file()
	max_lat=lat_long_rdd.agg(F.min(lat_long_rdd.Longi)).collect()
	f.write(str(max_lat))
create_bin()	
