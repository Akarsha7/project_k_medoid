from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from  random import randrange
import random
from operator import add
import time
import sys
sc = SparkContext()
sqlContext = SQLContext(sc)
result_str=''
time_sampling=''
time_cal_med=''
f=open("result/report_22_oct.txt","a")
sampling_size=0.5
def reading_file():
	t0 = time.time()
	ebird_file = sc.textFile("file:///home/abyad001/ds/areawater.csv")
	rdd = ebird_file.map(lambda p: p.split(","))
	header = rdd.first()
	rdd = rdd.filter(lambda line:line != header)
	df = rdd.map(lambda line: Row(Lat = float(line[2]),Longi=float(line[3]))).toDF()
	t1 = time.time()
	total_time = t1-t0
	result_str="Time taken to read file="+str(total_time)
	f.write(result_str+"\n")
	return  df

def sampling():
	t0 = time.time()
        lat_long_rdd = reading_file()
        random_latlong = lat_long_rdd.sample(False,0.2,None)
        t1 = time.time()
        total_time = t1-t0
	time_sampling="Time taken to sampling file="+str(total_time)
	f.write(time_sampling+'\n')
	return random_latlong
def k_med_sample():
	t0 = time.time()
        sampled_data = sampling().collect()
        flag=0
        k_med_points = 8
        total_cost_cluster = 0
        iterations=0
        randomly_chosen_medoids =random.sample(sampled_data,k_med_points)
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
                                index = random.randrange(0,k_med_points-1)
                                randomly_chosen_medoids[index]=sampled_data[medoid_replacement_inedx]
                        else:
                                flag=1
	t1 = time.time()
        total_time = t1-t0
        time_cal_med="Time taken to calculating medoids="+str(total_time)
	f.write(time_cal_med+"\n")
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
	ds_rdd = complete_data_set.rdd
        total_cluster_cost=0
	t0=time.time()
        distances = ds_rdd.map(lambda pt: assign_medoid(pt, selected_medoid))
        total_cluster_cost = distances.reduce(add)
        print total_cluster_cost
	t1 = time.time()
        total_time = t1-t0
        time_assigning="Time taken to assign all points="+str(total_time)
	cluster_points = "The cluster points selected are"+str(selected_medoid)
	f.write(cluster_points)
	f.write(str(total_cluster_cost))
	f.close()
if __name__=='__main__':
        sample_all_data()
