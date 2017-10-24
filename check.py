from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pandas as pd
from  random import randrange
import random
from operator import add

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
def cal_dist(pt,medoids):
        min_dist = float("inf")
 	print medoids
#	print pt
        for k in xrange(0,len(medoids)):
		#j=k
#		print "mmmmmmmmmmmmmmmmmmm"+str( medoids[k][0])
#		print "point "+str(pt[0])
		distance = abs(medoids[k][0]-pt[0])+abs(medoids[k][1]-pt[1])
                min_dist = min(distance,min_dist)
        return min_dist
def k_med_compute():
        lat_long_points = reading_file()
        total_dist = float("inf")
        flag =0
	selected_medoid = lat_long_points.sample(False,0.008,42).collect()
#	print selected_medoid
        while(flag!=1):
                distances = lat_long_points.map(lambda pt: cal_dist(pt,selected_medoid))
		temp_total_dist = distances.reduce(add)
		if(temp_total_dist<total_dist):
			total_dist = temp_total_dist
			reselect_point = lat_long_points.sample(False,0.002,42).collect()
			random_index = random.randint(0,len(selected_medoid)-1)
			selected_medoid[random_index]=reselect_point
		else:
			flag=1
	print total_dist
k_med_compute()
			



