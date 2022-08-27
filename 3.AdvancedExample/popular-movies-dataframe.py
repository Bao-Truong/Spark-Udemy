from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import *
import os

WORKDIR=os.path.dirname(os.getcwd())

spark = SparkSession.builder.appName("PopularMovies").master("local[*]").getOrCreate()

schema = StructType([
    StructField("userID",IntegerType(),True),
    StructField("movieID",IntegerType(),True),
    StructField("rating",IntegerType(),True),
    StructField("timestamp",LongType(),True)
])

df = spark.read.schema(schema).option("sep","\t").csv(f"file://{WORKDIR}/datasets/ml-100k/u.data")

df.printSchema()
topMovieID= df.select(df.movieID,df.rating).groupBy("movieID").count().orderBy(func.desc("count"))

topMovieID.show(10)

spark.stop()
