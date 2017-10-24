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
f=open("result/report_now_formed.txt","w")
sampling_size=0.15
def reading_file():
        ebird_file = sc.textFile("file:///home/abyad001/ds/areawater.csv")
        rdd = ebird_file.map(lambda p: p.split(","))
        header = rdd.first()
        rdd = rdd.filter(lambda line:line != header)
        df = rdd.map(lambda line: Row(Lat = float(line[2]),Longi=float(line[3]))).todf()
        return  df

def sampling():
        lat_long_rdd = reading_file()
	sampling_size=0.05
        random_latlong = lat_long_rdd.sample(False,sampling_size,None)
     #   result_str+=str(sampling_size)+","
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
	for i in randomly_chosen_medoids:
		f.write(str(i[1])+','+str(i[0]))
		f.write('\n')
	f.close()
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
	#result_str+=str(total_cluster_cost)+','
        return result_str
if __name__=='__main__':
	t0=time.time()
	result_str=''
        total_cluster_cost=sample_all_data()
	t1=time.time()
	time_taken = t1-t0
	result_str=str(sampling_size)+","+str(time_taken)+","+str(total_cluster_cost)+"\n"
	f.write(result_str)
	f.close()
