from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructField, StructType, IntegerType, LongType
import os

WORKDIR = os.path.dirname(os.getcwd())

spark = SparkSession.builder.appName(
    "PopularMovies").master("local[*]").getOrCreate()

schema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

# Load Boardcast dict for movie name
def loadMovieName():
    moviesName = {}
    with open(f"{WORKDIR}/datasets/ml-100k/u.item", "r", encoding="UTF-8", errors="ignore") as f:
        for line in f:
            parts = line.split("|")
            moviesName[parts[0]] = parts[1]
    return moviesName


moviesName = spark.sparkContext.broadcast(loadMovieName())

df = spark.read.schema(schema).option("sep", "\t").csv(
    f"file://{WORKDIR}/datasets/ml-100k/u.data")

movieCounts = df.groupBy(df.movieID).count()


def lookupName(movieID):
    return moviesName.value[str(movieID)]


lookupNameUDF = func.udf(lookupName)
movieWithName = movieCounts.withColumn("MovieName", lookupNameUDF("movieID"))

movieWithName.orderBy(func.desc("count")).show(10)
