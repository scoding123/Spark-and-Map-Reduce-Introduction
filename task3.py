import sys
from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.rdd import portable_hash
import json
import os
import re
import time
from operator import add
sc = SparkContext()

input_file = sys.argv[1]
output_file = sys.argv[2]
partition_type = str(sys.argv[3])
n_partitions = int(sys.argv[4])
n = int(sys.argv[5])

rdd_info = {"n_partitions": n_partitions}



if partition_type == 'customized':
    
    rdd2 = sc.textFile(input_file,n_partitions).map(json.loads).map(lambda x: (x['business_id'],1)).partitionBy(n_partitions,lambda x: hash(x)%n_partitions)
    
    rdd_info["n_items"] = rdd2.mapPartitions(lambda x: [len(list(x))]).collect()
    
    rdd_info["result"] = rdd2.reduceByKey(lambda i,j: i+j).filter(lambda x: x[1] > n).map(list).collect()


elif partition_type == 'default':
    
    rdd = sc.textFile(input_file).map(json.loads).map(lambda x: (x['business_id'],1))
    
    rdd_info["n_partitions"] = rdd.getNumPartitions()

    rdd_info["n_items"] = rdd.mapPartitions(lambda x: [len(list(x))]).collect()

    rdd2 = rdd.reduceByKey(add)

    rdd_info["result"] = rdd2.filter(lambda x: x[1] > n).map(list).collect()

with open(output_file, "w") as outfile:
    json.dump(rdd_info, outfile)
