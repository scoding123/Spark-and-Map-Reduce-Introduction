import sys
from pyspark import SparkConf, SparkContext
import json
import os
import re
import time
sc = SparkContext()


input_file = sys.argv[1]
output_file = sys.argv[2]
stopwords = sys.argv[3]
y = sys.argv[4]
m = sys.argv[5]
n = sys.argv[6]
output_json = {}


#read input file into RDD
rdd = sc.textFile(input_file)
rdd=rdd.map(json.loads)
rdd = rdd.map(lambda x:(x['user_id'],x['text'],x['date'])).persist()

#part A
output_json['A'] = rdd.count()

#part B
output_json['B'] = year_count = rdd.filter(lambda x: x[2][:4]==str(y)).count()

#part C
output_json['C'] = user_count = rdd.map(lambda x:(x[0])).distinct().count()

#part D
output_json['D'] = rdd.map(lambda x:(x[0],1)).reduceByKey(lambda i,j: i+j).map(lambda x: list(x)).takeOrdered(int(m), key = lambda x : (-x[1],x[0]))

#part E
with open(stopwords) as f:
    stopwrds = f.read().splitlines()
f.close()

exclusion = ['(', '[', ',', '.', '!', '?', ':', ';', ']', ')',' ','']
stopwrds = stopwrds + exclusion

word_count = rdd.flatMap(lambda x: x[1].split(" ")).filter(lambda w: w.lower() not in stopwrds).map(lambda word: (word.lower(), 1)).reduceByKey(lambda a, b: a + b).takeOrdered(int(n), key = lambda x : (-x[1],x[0]))

output_json['E'] = []
for i in range(len(word_count)):
    output_json['E'].append(word_count[i][0])


#writing to output file
with open(output_file, "w") as outfile:
    json.dump(output_json, outfile, sort_keys = True)