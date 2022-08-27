#  spark-submit min-temperatures.py
from pyspark import SparkConf, SparkContext
import os

WORKDIR = os.path.dirname(os.getcwd())

conf = SparkConf().setMaster("local").setAppName("MinTemperature")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(",")
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1*1.8 + 32.0
    return (stationID, entryType, temperature)


lines = sc.textFile(f"file://{WORKDIR}/datasets/1800.csv")
rdd = lines.map(parseLine)
minTemps = rdd.filter(lambda x: "TMIN" in x[1])
stationTemp = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemp.reduceByKey(lambda x, y: min(x, y))
results = minTemps.collect()
for result in results:
    print(result[0] +":\t{:.2f}F".format(result[1]))
