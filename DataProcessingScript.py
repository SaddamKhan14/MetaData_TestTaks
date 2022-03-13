
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window


PATH = os.getcwd()

# Creating SparkContext
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("MetaData | Take Home Test task") \
    .getOrCreate()

# Performing data load
df = spark \
    .read \
    .option("multiline", "true") \
    .json(os.path.join(PATH, "../input/*.json"))

# Dropping duplicate records
df_1 = df.select(
        col("city").alias("location"),
        col("timestamp").alias("date_time"),
        col("temp").alias("temperature"),
        col("temp_max").alias("max_temperature"),
        col("temp_min").alias("min_temperature")
).dropDuplicates()

# Creating dataset containing the location, date and temperature of the highest temperatures reported by location and month.
windowTemp = Window.partitionBy(["location", "month_only"]).orderBy(col("max_temperature").desc())
df_2 = df.select(
        col("city").alias("location"),
        col("timestamp").alias("date_time"),
        col("temp").alias("temperature"),
        col("temp_max").alias("max_temperature")
)
df_2 = df_2.withColumn('month_only', month(col('date_time')))

df_2 = df_2.withColumn("row",row_number().over(windowTemp)) \
    .filter(col("row") == 1)\
    .drop("row")

# Creating dataset containing the average temperature, min temperature, location of min temperature, and location of max temperature per day
df_3 = df.select(
    col("city").alias("location"),
    col("temp").alias("temperature"),
    col("temp_max").alias("max_temperature"),
    col("temp_min").alias("min_temperature")
)

df_loc_minTemp = df_3.orderBy('min_temperature').limit(1)
df_loc_maxTemp = df_3.orderBy('max_temperature', ascending=False).limit(1)

df_3 = df_3.agg(
    avg(col("temperature").alias("average_temperature")),
    min(col("temperature").alias("minimum_temperature")),
    df_loc_minTemp.select("location").alias("loc_min_temperature"),
    df_loc_maxTemp.select("location").alias("loc_max_temperature"),
)

# Preparing final dataset
df_temp = df_1.union(df_2).dropDuplicates(subset="location")
df_final = df_temp.union(df_3).dropDuplicates(subset="location")

# Persisting final output
df_final\
    .coalesce(1)\
    .write\
    .mode('overwrite')\
    .option('header', 'true')\
    .csv(os.path.join(PATH, "../output"))