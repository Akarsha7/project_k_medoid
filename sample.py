from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pandas as pd
import  random
sc = SparkContext()
sqlContext = SQLContext(sc)
def reading_file():
        df = pd.read_csv('/home/abyad001/Downloads/lat_long.csv')
        latitude = df['latitude']
        lat_rdd = sc.parallelize(latitude)
        sampled_data_set = lat_rdd.sample(False,0.1,None)
	print "The number of elemenets sampled"
        print sampled_data_set.count()
reading_file()
