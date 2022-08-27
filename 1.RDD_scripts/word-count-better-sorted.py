from pyspark import SparkConf, SparkContext
import re
import os

WORKDIR = os.path.dirname(os.getcwd())

conf = SparkConf().setMaster("local").setAppName("WordCountBetter")
sc = SparkContext(conf=conf)


def normalizeText(txt):
    return re.compile(r"\W+", re.UNICODE).split(txt.lower())


lines = sc.textFile(f"file://{WORKDIR}/datasets/Book")
rdd = lines.flatMap(normalizeText)
countByWord=rdd.map(lambda x: (x, 1)).reduceByKey(
    lambda x, y: x+y).sortBy(lambda x: x[1], False)
results = countByWord.collect()
with open("word-count.txt", "w+") as f:
    for res in results:
        word = res[0].encode('ascii', 'ignore').decode()
        if (word):
            f.write(word+ ":\t\t" + str(res[1])+"\n")
