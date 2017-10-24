from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pandas as pd
from  random import randrange
import random
from operator import add

sc = SparkContext()
sqlContext = SQLContext(sc)
def cal_dist(pt,med):
        min_dist = float("inf")
        for k in xrange(len(med)):
#		print "medoid :        sjkdfkkkkkkkkkkhsdkjfhsd;fhdlhdhgfgh"+str((med[k][0]))
#		print "value of k :                   jhsfgdksdghfshdgcjksgdfkljshdfkljhdkgjlh;dfjkgh"+str(k)
                distance = abs(med[k][0]-pt[0])+abs(med[k][1]-pt[1])
#		print "distance jsdfgskadgfsdajgflsdkgjfsdgfjhsd"+str(distance)
                min_dist = min(distance,min_dist)
        return min_dist

def med_cost():
        df = pd.read_csv('/home/abyad001/Downloads/lat_long.csv')
        latitude = df['latitude']
        longitude = df['longitude']
        lat_long = []
        float_lat = map(float,latitude)
        float_long = map(float,longitude)
        for i in range(len(latitude)):
                lat_long.append((float_lat[i],float_long[i]))
        lat_long_points = sc.parallelize(lat_long)
        total_dist = float("inf")
        flag =0
        sel_medoid = list(lat_long_points.sample(False,0.008,42).collect())
	selected_medoid = map(list,sel_medoid)
	print "hasgdffjagdjghdjwhegfdkewgfkrjfgekwjhfklejrhglrhetlghrlfghlefk"+str(lat_long_points.take(10))
        while(flag!=1):
                distances = lat_long_points.map(lambda pt: cal_dist(pt,selected_medoid))
                temp_total_dist = distances.reduce(add)
                if(temp_total_dist<total_dist):
                        total_dist = temp_total_dist
                        reselect_point = lat_long_points.sample(False,0.002,42).collect()
                        random_index = random.randint(0,len(selected_medoid)-1)
			print "aklsdfhgkld;gh;fsdkhgfjdsgh"+str((type(selected_medoid[random_index])))
			selected_medoid[random_index][0]=reselect_point[0][0]
			selected_medoid[random_index][1]=reselect_point[0][1]
 
                else:
                        flag=1
	print total_dist
med_cost()


