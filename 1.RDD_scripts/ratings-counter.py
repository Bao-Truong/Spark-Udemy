### HOW TO RUN
# spark-submit ratings-counter.py


from pyspark import SparkConf, SparkContext
import collections
import os

WORKDIR=os.path.dirname(os.getcwd())

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile(f"file://{WORKDIR}/datasets/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
print(ratings.first())
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
