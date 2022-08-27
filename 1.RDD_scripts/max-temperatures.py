#  spark-submit max-temperatures.py
from pyspark import SparkConf, SparkContext
import os
WORKDIR = os.path.dirname(os.getcwd())

conf = SparkConf().setMaster("local").setAppName("MaximumTemperature")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(",")
    stationID = fields[0]
    entryPoint = fields[2]
    if("TMAX" not in entryPoint):
        return ()
    temperature = float(fields[3])*0.1 * 1.8 + 32
    return (stationID, temperature)


lines = sc.textFile(f"file://{WORKDIR}/datasets/1800.csv")
rdd = lines.map(parseLine)
filterEmpty = rdd.filter(lambda x: x != ())
maxTemps = filterEmpty.reduceByKey(lambda x, y: max(x, y))
results = maxTemps.collect()
for result in results:
    print(result[0] + ":\t{:.2f}F".format(result[1]))
