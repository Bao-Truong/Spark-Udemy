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
countByWords = rdd.countByValue()


for word, count in countByWords.items():
    cleanword=word.encode('ascii',"ignore").decode()
    if(cleanword):
        print(str(cleanword) + ": " + str(count))
