from pyspark.sql import SparkSession, Row
from pyspark.streaming import StreamingContext
from pyspark import SparkContext
from pyspark.sql.functions import regexp_extract
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("Top-url").getOrCreate()

accessLines = spark.readStream.text("logs")

contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

logsDF = accessLines.select(regexp_extract('value', hostExp, 1).alias('host'),
                            regexp_extract('value', timeExp,
                                           1).alias('timestamp'),
                            regexp_extract('value', generalExp,
                                           1).alias('method'),
                            regexp_extract('value', generalExp,
                                           2).alias('endpoint'),
                            regexp_extract('value', generalExp,
                                           3).alias('protocol'),
                            regexp_extract('value', statusExp, 1).cast(
                                'integer').alias('status'),
                            regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))

logsDF = logsDF.withColumn("eventTime", func.current_timestamp())
# topURLcount=logsDF.groupBy("endpoint").count().sort('count', ascending=False)
topURLcount = logsDF.groupBy(func.window(func.col("eventTime"), windowDuration="30 second",
                             slideDuration="10 second"), func.col("endpoint")).count().sort('count', ascending=False)

query = (topURLcount.writeStream.outputMode(
    "complete").format("console").queryName("counts").start())

query.awaitTermination()

spark.stop()
