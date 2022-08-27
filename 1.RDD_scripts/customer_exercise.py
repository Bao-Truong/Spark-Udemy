from pyspark import SparkConf, SparkContext
import os

WORKDIR = os.path.dirname(os.getcwd())

conf = SparkConf().setMaster("local").setAppName("CustomerSpending")
sc = SparkContext(conf=conf)


def parseLine(line):
    fs = line.split(",")
    customerID = fs[0]
    spend = float(fs[2])
    return (customerID, spend)


lines = sc.textFile(f"file://{WORKDIR}/datasets/customer-orders.csv")
rdd = lines.map(parseLine)
totalSpend=rdd.reduceByKey(lambda x,y: x+y)
sortBySpending= totalSpend.sortBy(lambda x: x[1])
result=sortBySpending.collect()

for res in result:
    print("userID {} has spent {:.5f} dollars".format(res[0],res[1]))
