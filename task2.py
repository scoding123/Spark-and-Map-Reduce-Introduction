import sys
from pyspark import SparkConf, SparkContext
import json
import os
import re
import time

input_file = sys.argv[1]
input_file_2 = sys.argv[2]
output_file = sys.argv[3]
if_spark = str(sys.argv[4])
n = int(sys.argv[5])

if if_spark.lower() == 'spark':
    #start_time = time.time()

    sc = SparkContext()
    review_rdd = sc.textFile(input_file)
    review_rdd = review_rdd.map(json.loads)
    review_rdd = review_rdd.map(lambda x: (x['business_id'],x['stars'])).persist()

    business_rdd = sc.textFile(input_file_2)
    business_rdd = business_rdd.map(json.loads)
    business_rdd = business_rdd.map(lambda x:(x['business_id'],x['categories'])).persist()

    rdd = review_rdd.filter(lambda x : x[1] is not None).join(business_rdd.filter(lambda y : y[1] is not None))
    rdd = rdd.map(lambda x:(x[1][1],x[1][0])).persist()

    def flatten(pair):
        s, i = pair
        for x in s.split(","):
            yield x.strip(), i

    exploded_rdd = (rdd.flatMap(flatten))

    averaged_out = exploded_rdd.mapValues(lambda x : (x, 1)).reduceByKey(lambda x , y : (x[0] + y[0], x[1] + y[1])).mapValues(lambda x : x[0] / x[1])
    top_categories = averaged_out.takeOrdered(n, key = lambda x : (-x[1],x[0]))

    for i in range(len(top_categories)):
        top_categories[i] = list(top_categories[i])

    result = {}
    result['result'] = top_categories
    with open(output_file, "w") as outfile:
        json.dump(result, outfile)

    #print("--- time taken with spark ---",(time.time() - start_time)/60, 'minutes!')

elif if_spark.lower() == 'no_spark':
    #start_time = time.time()
    import json
    data = []
    with open(input_file) as f:
        for line in f:
            data.append(json.loads(line))

    for i in data:
        del i['review_id']
        del i['user_id']
        del i['text']
        del i['date']

    stars = {}
    bid_count = {}
    for i in data:
        stars.setdefault(i['business_id'],0)
        bid_count.setdefault(i['business_id'],0)
        stars[i['business_id']] += i['stars']
        bid_count[i['business_id']] += 1

    data_1 = []
    with open(input_file_2) as f:
        for line in f:
            data_1.append(json.loads(line))

    for i in data_1:
        del i['name']
        del i['address']
        del i['city']
        del i['state']
        del i['postal_code']
        del i['latitude']
        del i['longitude']
        del i['stars']
        del i['review_count']
        del i['is_open']
        del i['attributes']
        del i['hours']

    for i in data_1:
        if i['categories'] is not None:
            i['categories'] = i['categories'].split(',')
            for j in range(len(i['categories'])):
                i['categories'][j] = i['categories'][j].strip()
        else:
            data_1.remove(i)

    categories = {}
    for i in data_1:
        if i['categories'] is not None:
            for j in i['categories']:
                categories.setdefault(j,[])
                categories[j].append(i['business_id'])
    dict = {}
    for i in categories:

        stars_sum = 0
        total_count = 0
        for j in categories[i]:
            if j in stars:
                dict.setdefault(i,0)
                stars_sum += stars[j]
                total_count += bid_count[j]
        if total_count > 0 and stars_sum > 0:
            dict[i] = float(stars_sum)/float(total_count)

    j = 0
    res = []
    for key, value in sorted(dict.items(), key=lambda item: (-item[1],item[0])):
        if j < n:
            res.append([key,value])
            j+=1

    output = {}
    output['result'] = res
    with open(output_file, "w") as outfile:
        json.dump(output, outfile)

    #print("--- time taken with spark ---",(time.time() - start_time), 'seconds!')
#else:
    #print('Please enter correct arguments!')