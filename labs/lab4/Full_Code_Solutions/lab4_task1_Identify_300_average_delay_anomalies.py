from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

spark = SparkSession \
    .builder \
    .master("local") \
    .config("spark.driver.memory", "4g") \
    .appName('ex4_anomalies_detection') \
    .getOrCreate()

all_history_window = Window.partitionBy(F.col('Carrier')).orderBy(F.col('flight_date'))

flights_df = spark.read.parquet('s3a://spark/data/transformed/flights/')

flights_df.cache()

flights_df \
    .withColumn('avg_till_now', F.avg(F.col('arr_delay')).over(all_history_window)) \
    .withColumn('avg_diff_percent', F.abs(F.col('arr_delay') / F.col('avg_till_now'))) \
    .show(1000)

flights_df.unpersist()

spark.stop()
