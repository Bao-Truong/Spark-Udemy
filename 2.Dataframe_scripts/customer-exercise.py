from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import *
import os

WORKDIR = os.path.dirname(os.getcwd())

spark= SparkSession.builder.appName("CustomerSpending").getOrCreate()
schema=StructType([
    StructField("CustomerID",IntegerType(),True),
    StructField("ItemID",IntegerType(),True),
    StructField("Totalspend",FloatType(),True)
])

df = spark.read.schema(schema).csv(f"{WORKDIR}/datasets/customer-orders.csv")

df.printSchema()

totalSpend= df.select(df.CustomerID, df.Totalspend)
totalByCustomer= totalSpend.groupBy("CustomerID").agg(func.sum("Totalspend").alias("TotalSpend"))

sortedBySpending = totalByCustomer.sort("TotalSpend",ascending=False).show()

spark.stop()
