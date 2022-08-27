from pyspark import SparkConf, SparkContext
import os
import math

from sqlalchemy import false

WORKDIR = os.path.dirname(os.getcwd())

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(",")
    age = int(fields[2])
    NumFriends = int(fields[3])
    return (age, NumFriends)


lines = sc.textFile(f"file://{WORKDIR}/datasets/fakefriends.csv")
rdd = lines.map(parseLine)
## (33, 385)
## (33, 2)
## (26, 2)
## (55, 221)

totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (
    x[0]+y[0], x[1]+y[1]))
## (33, (385,1))
## (33, (2,1))
## (26, (2,1))
## (55, (221,1 ))
averageByAge = totalsByAge.mapValues(
    lambda x: math.ceil(x[0]/x[1])).sortBy(lambda x: x[1], False)
# -> (33, (387, 2)) -> (33, 387/2) -> sort

results = averageByAge.collect()
for res in results:
    print(res)
