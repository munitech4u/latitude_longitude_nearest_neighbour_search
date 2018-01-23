

import numpy as np
from scipy import spatial
import time
import pandas as pd

#reading customers file, having ID, latitude, longitude
path1=str("..../lat_lon_dev_pull.TXT")
cust_ds=spark.read.option("sep","|").csv(path1).toDF("lat","lon","ID")

#reading objects file, object-type, latitude, longitude. Object type can be like coffee shop, bank, railway line etc.
path=str("..../berry_objects.txt")
obj_ds = spark.read.option("sep","|").option("header", "true").csv(path).toDF("obj_type","lat","lon")

#objective- query the objects file for each customer and create count variables based on the nearest neighbours relative to their co-ordinates

from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col

cust_ds = cust_ds.withColumn("lat2", cust_ds["lat"].cast(DoubleType()))
cust_ds = cust_ds.withColumn("lon2", cust_ds["lon"].cast(DoubleType()))

obj_ds = obj_ds.withColumn("lat2", obj_ds["lat"].cast(DoubleType()))
obj_ds = obj_ds.withColumn("lon2", obj_ds["lon"].cast(DoubleType()))


obj_list=obj_ds.select("obj_type").distinct().rdd.map(lambda row : row[0]).collect()

#convert RDD to pandas dataframe in-order to create KD-Trees
obj_pd=obj.toPandas()

d={}
for i in range(len(obj_list)):
    d[i]=obj_list[i]
    
o={}  

start=time.time()

#create KD-tress from objects file and broadcast to all nodes, then query in parallel for all customers. Loop over for all objects
for i in range(len(obj_list)):
        obj_filter=obj_pd[obj_pd['obj_type']==d[i]]
        tree = spatial.cKDTree(obj_filter[['lat2','lon2']],leafsize=16)
        kdt_b = sc.broadcast(tree)
        points_parsed= cust_ds.select('lat2','lon2').rdd.map(lambda l: (l[0],l[1]))
        t = points_parsed.map(lambda x: kdt_b.value.query_ball_point(x,r=0.005))
        t_length = t.map(lambda x: len(x))
        o[i] = t_length.toDF('int')
        

print("Done! Took " + str(round(((time.time())-start)/60)) + " minutes")

#join with customers file to get the ID for all objects count variables
from pyspark.sql.functions import monotonically_increasing_id
df={}
for i in range(len(obj_list)):
    if i==0:
        df[i] = cust_ds.select('ID')
        df[i+1] = df[i].withColumn("idx", monotonically_increasing_id())
     
    else:
        df_tmp = o[i-1].select(col("value").alias("obj"+str(i-1)))
        df_tmp = df_tmp.withColumn("idx", monotonically_increasing_id())
        
        df[i+1] = df[i].join(df_tmp, "idx", "outer")
        df[i].unpersist()

print("done")

