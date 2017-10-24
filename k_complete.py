from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from  random import randrange
from random import randint
import random
from operator import add
import time
result_str=''
sc = SparkContext()
sqlContext = SQLContext(sc)
f=open("complete_k_med_op","a")
def reading_file():
        ebird_file = sc.textFile("file:///home/abyad001/ds/areawater_small.csv")
        rdd = ebird_file.map(lambda p: p.split(","))
        header = rdd.first()
        rdd = rdd.filter(lambda line:line != header)
        df = rdd.map(lambda line:  Row(Lat = float(line[2]),Longi=float(line[3]))).toDF()
	return df
lat_long_pts = reading_file()
lat_long_rdd = lat_long_pts.rdd
def select_points(num_of_points):
	sampling_size = 0.01
	random_latlong = lat_long_rdd.sample(False,sampling_size,None).collect()
	if(num_of_points==4):
		k_med_points = random.sample(random_latlong,4)
	else:
		k_med_points = random.sample(random_latlong,1)
	return k_med_points
def cal_dist(pt,medoids):
	min_dist = float("inf")
	for k in xrange(0,len(medoids)):
		distance = abs(medoids[k][0]-pt[0])+abs(medoids[k][1]-pt[1])
		min_dist = min(distance,min_dist)
	return min_dist

def k_med_compute():
	total_dist = float("inf")
	flag =0
	min_cost = float("inf")
	med_points = select_points(4)
	while(flag!=1):
		temp_dist=lat_long_rdd.map(lambda pt: cal_dist(pt,med_points))
		temp_total_dist = temp_dist.reduce(add)
		if(temp_total_dist<min_cost):
			new_med_points=[] 
                        replace_index = randint(0,3)
                        replace_med_point=select_points(1)
			new_med_points.append(replace_med_point)
			for i in range(len(med_points)):
				if(i!=replace_index):
					new_med_points.append(med_points[i])
			med_point = new_med_points
			min_cost = temp_total_dist
                else:
    	            flag = 1
	return med_points
def assign_medoid(pt, medoids):
        min_distance = float("inf")
        for k in xrange(0, len(medoids)):
                 distance = abs(medoids[k][0]-pt[0])+abs(medoids[k][1]-pt[1])
                 min_distance = min(distance, min_distance)
        return min_distance

def sample_all_data():
        selected_medoid = k_med_compute()
        total_cluster_cost=0
        distances = lat_long_rdd.map(lambda pt: assign_medoid(pt, selected_medoid))
        total_cluster_cost = distances.reduce(add)
	print "fffffffffffffffffffffffffffffffffffffffffffffffffffffff"+str(total_cluster_cost)

if __name__=='__main__':
	t0=time.time()
        sample_all_data()
	'''t1 =time.time()
	time_take= t1-t0
	result_str+=cluster_cost
	result_str+=time_taken
	f.write(result_str)'''
