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
        longitude = df['longitude']
        lat_long = []
        float_lat = map(float,latitude)
        float_long = map(float,longitude)
        for i in range(len(latitude)):
                lat_long.append((float_lat[i],float_long[i]))
        lat_long_rdd = sc.parallelize(lat_long)
        return lat_long_rdd

def sampling():
        lat_long_rdd = reading_file()
#       print len(lat_long_rdd)
#       sampling_size = abs((1*lat_long_rdd.count())/100)
        random_latlong = lat_long_rdd.sample(False,0.1,None)
	randomly_chosen_medoids_rdd = random_latlong.sample(False,0.1,None)
	
	rd=randomly_chosen_medoids_rdd.take(4)
	print rd

sampling()
