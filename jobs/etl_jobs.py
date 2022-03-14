#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
##
# Description:-
#
#       ELT Script contains function definition to read, process and persist data
#
##
# Development date    Developed by       Comments
# ----------------    ------------       ---------
# 12/03/2021          Saddam Khan        Initial version
#
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

import os
import sys
import traceback

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import StructType
from pyspark.sql.window import Window

PATH = os.getcwd()

def extract_data(
        spark: SparkSession,
        schema: StructType,
        logger
) -> DataFrame:
    """
    Explanation: Performing Data Load from source directory having files in JSON format
    :param  spark SparkSession: Spark session object
    :param  schema StructType: StructType schema object
    :param  logger: Logger class object
    :return df DataFrame: Spark DataFrame
    """
    logger.info("Started: Data load from input folder")
    try:
        logger.info("Started validating local file input path")
        if os.path.exists(PATH):
            df = spark\
                .read\
                .option("multiline", "true") \
                .schema(schema) \
                .json(os.path.join(PATH, "../input/*.json"))
        else:
            log_msg = "ERROR: The {} does not exist".format(PATH)
            logger.error(log_msg)
        logger.info("Successfully validated local file input path")
    except (IOError, OSError) as err:
        logger.error("ERROR: occured validate file path")
        print("ERROR: occured validate file path")
        print(err)
        traceback.print_exception(*sys.exc_info())
        sys.exit(0)
    logger.info("Completed: Data load from input folder")

    # caching dataframe object
    df.cache()
    logger.info("Dataframe cached in memory")

    return df


def transform_data(
        df: DataFrame,
        logger
) -> DataFrame:
    """
    Explanation: Performing right Data Type match for raw Data Load assuming schema is of fixed type
    :param  df DataFrame: Spark DataFrame with raw input
    :param  logger: Logger class object
    :return None: Print output percentage on console
    """
    logger.info("Started: Data Transformation on input Dataframe")
    try:
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
    except Exception as err:
        logger.error("ERROR occured while applying transformation on dataframe")
        print("ERROR occured while applying transformation on dataframe")
        print(err)
        traceback.print_exc()
    logger.info("Completed: Data Transformation on input Dataframe")

    return df_final


def load_data(
        df_final: DataFrame,
        connector_config: str,
        jdbc_url: str,
        jdbc_driver: str,
        logger
) -> None:
    """
    Explanation: Write output to RDBMS
    :param  df DataFrame: DataFrame to print
    :param  connector_config Tuple: Data Source Connection String parameters
    :param  jdbc_url String: Data Source Connection String URL
    :param  jdbc_driver String: Data Source Connection String Driver Class
    :param  logger: Logger class object
    :return None :
    """
    logger.info("Started: Persisting dataset to RDBMS")
    database, target_table, user, password = connector_config
    try:
        logger.info("Started writing to data source")
        df_final \
                .write.format("jdbc") \
                .mode("overwrite") \
                .option("url", jdbc_url) \
                .option("dbtable", target_table) \
                .option("user", user) \
                .option("password", password) \
                .option("driver", jdbc_driver) \
                .save()
        logger.info("Successfully written to data source")
    except (IOError, OSError) as err:
        logger.error("ERROR: occured writing to data source validate connection string")
        print("ERROR: occured writing to data source validate connection string")
        print(err)
        traceback.print_exception(*sys.exc_info())
        sys.exit(0)
    logger.info("Completed: Persisting dataset inside RDBMS")

    return None
