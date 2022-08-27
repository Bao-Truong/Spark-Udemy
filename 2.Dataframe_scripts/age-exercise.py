from pyspark.sql import SparkSession
from pyspark.sql import functions as func
import os

WORKDIR = os.path.dirname(os.getcwd())

spark = SparkSession.builder.appName("SparkSQLExercise").getOrCreate()

people = spark.read.option("header", True).option(
    "inferSchema", True).csv(f"{WORKDIR}/datasets/fakefriends-header.csv")

people.printSchema()
people = people.select("age", "friends")
results = people.groupBy("age").agg(func.round(
    func.avg("friends"), 2).alias("numFriends")).sort("age").collect()
# Alias phai nằm trong agg(.avg().alias) (o),
# ko thể nằm ngoài với .avg().alias (x)
for r in results:
    row = r.asDict()
    print(str(row["age"]) + ": " + str(row["numFriends"]))


spark.stop()
