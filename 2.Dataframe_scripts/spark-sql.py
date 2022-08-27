from pyspark.sql import SparkSession, Row
import os

WORKDIR = os.path.dirname(os.getcwd())
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()


def mapper(line):
    fs = line.split(",")
    return Row(ID=int(fs[0]), name=str(fs[1].encode("utf-8")),
               age=int(fs[2]), numFriends=int(fs[3]))


lines = spark.sparkContext.textFile(
    f"file://{WORKDIR}/datasets/fakefriends.csv")
people = lines.map(mapper)

schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

teenagers = spark.sql(
    "SELECT * FROM people where age >=13 AND age <=19").collect()

for teen in teenagers:
    print(teen)

schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()
