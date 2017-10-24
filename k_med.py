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
	
def sampling():
	lat_long_rdd = reading_file()
	random_latlong = lat_long_rdd.sample(False,0.1,None)
	return random_latlong
def k_med_sample():
	sampled_data = sampling().collect()
	flag=0
	k_med_points = 4
	total_cost_cluster = 0
	iterations=0
	randomly_chosen_medoids =random.sample(sampled_data,4)
	while(flag==0):
		for i in range(0,len(sampled_data)):
			min_dist = float("inf")
			for k in range(0,len(randomly_chosen_medoids)):
				temp_cost = abs(randomly_chosen_medoids[k][0]-sampled_data[i][0])+abs(randomly_chosen_medoids[k][1]-sampled_data[i][1])
				min_dist = min(temp_cost,min_dist)
			total_cost_cluster = total_cost_cluster+min_dist
		medoid_replacement_index = randrange(0,len(sampled_data))
		if(iterations==0):
			min_cost = total_cost_cluster
			iterations=1
		else:
			if(min_cost>total_cost_cluster):
				min_cost = total_cost_cluster
				index = random.randrange(0,3)
				randomly_chosen_medoids[index]=sampled_data[medoid_replacement_inedx]
			else:
				flag=1
	return randomly_chosen_medoids

def assign_medoid(pt, medoids):
	min_distance = float("inf")
	for k in xrange(0, len(medoids)):
		distance = abs(medoids[k][0]-pt[0])+abs(medoids[k][1]-pt[1])
		min_distance = min(distance, min_distance)
	return min_distance

def sample_all_data():
	selected_medoid = k_med_sample()
	complete_data_set = reading_file()
	total_cluster_cost=0
	distances = complete_data_set.map(lambda pt: assign_medoid(pt, selected_medoid))
	total_cluster_cost = distances.reduce(add)				
	print total_cluster_cost
if __name__=='__main__':
	sample_all_data()
	
