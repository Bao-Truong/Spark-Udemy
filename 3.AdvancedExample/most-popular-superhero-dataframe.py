from itertools import groupby
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import *
import os

WORKDIR = os.path.dirname(os.getcwd())

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


connections = lines \
    .withColumn("ID", func.split(lines.value, " ")[0]) \
    .withColumn("Connections", func.size( func.split(func.trim( lines.value), " " ) ) -1 ) \
    .groupby("ID").agg(func.sum("Connections").alias("Connections"))

mostPopular = connections.sort(func.col("Connections").desc()).first()

mostPopularName = names.filter(names.ID == mostPopular["ID"]).select("Names").first()

print(mostPopular[0])
print(mostPopular[1])
print(mostPopularName)
