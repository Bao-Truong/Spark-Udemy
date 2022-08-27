from functools import reduce
from itertools import groupby
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import *
import os

WORKDIR = os.path.dirname(os.getcwd())
targetID = 2297

spark = SparkSession.builder.appName(
    "MostPopularSuperHero").master("local[*]").getOrCreate()

schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("Names", StringType(), True)
])

names = spark.read.schema(schema).option("quote", "\"").option("sep", " ").csv(
    f"file://{WORKDIR}/datasets/MarvelNames")

names.printSchema()
names.show(10)

lines = spark.read.text(f"file://{WORKDIR}/datasets/MarvelGraph")

lines.show()

withSchema = lines \
    .withColumn("ID", func.split(lines.value, " ")[0]) \
    .withColumn("Connections", func.split(lines.value, " "))
# .groupby("ID").agg(func.array_distinct(func.concat("Connections")))

withSchema.show()
rdd = withSchema.rdd
print(rdd.collect()[0])


def combine(data1, data2):
    print("awegfwea\n",type(data1))
    return (list(set(list(data1) + list(data2))))


print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
filtedByHero = rdd.filter(lambda x: x[1] == str(
    targetID)).map(lambda x: (x[1], x[2]))
print(filtedByHero.collect()[0])
print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
reduced = filtedByHero.reduceByKey(combine)
print("********************************************************************")
print("********************************************************************")

results=reduced.collect()
with open(f"output_{targetID}.txt","w+") as f:
    for r in results:
        f.write(str(r))
