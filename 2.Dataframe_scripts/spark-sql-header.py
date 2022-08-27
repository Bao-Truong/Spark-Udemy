from pyspark.sql import SparkSession
import os
WORKDIR = os.path.dirname(os.getcwd())

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people= spark.read.option("header","true").option("inferSchema","true").csv(f"{WORKDIR}/datasets/fakefriends-header.csv")

people.printSchema()

people.show()

people.select("name").show()

people.filter(people.age < 21).show()

people.groupBy(people.age).count().show()

people.select(people.name, people.age+10).show()

spark.stop()
