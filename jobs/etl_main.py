#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
##
# Description:-
#
#       Driver Script to read, process and persist data
#
##
# Development date    Developed by       Comments
# ----------------    ------------       ---------
# 12/03/2021          Saddam Khan        Initial version
#
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

import configparser
import os

from MetaData_TestTaks.jobs.etl_api_request import weather_api_data_pull
from MetaData_TestTaks.jobs.etl_jobs import extract_data, transform_data, load_data
from MetaData_TestTaks.utils.project_utils import MetaDataUtils
from MetaData_TestTaks.utils.log4j_root_logger import Log4jRootLogger
from pyspark.sql import SparkSession

PATH = os.getcwd()

def main():
    """
        Main ELT script definition
        :return None :
    """

    # Creating SparkContext
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("MetaData | Take Home Test task") \
        .config("spark.dynamicAllocation.minExecutors", "2") \
        .config("spark.dynamicAllocation.maxExecutors", "8") \
        .config("spark.dynamicAllocation.initialExecutors", "4") \
        .config("spark.executor.heartbeatInterval", "200000") \
        .config("spark.network.timeout", 10000000) \
        .config("spark.driver.maxResultSize", "8g") \
        .config("spark.driver.memory", "10g") \
        .config("spark.executor.memory", "10g") \
        .config("spark.executor.cores", "3") \
        .config("spark.cores.max", "4") \
        .getOrCreate()

    # Creating log4j logger for logging
    logger = Log4jRootLogger(spark)

    logger.info("################################################################")
    logger.info("############# Starting Execution of Data Load Job ##############")
    logger.info("################################################################")

    logger.info("Started: ELT Job ")

    # API calls for extracting and loading weather API data
    logger.info("Started: API request to pull data, based on user input i.e. by city or by location")
    weather_api_data_pull()
    logger.info("Completed: API request to pull data, based on user input i.e. by city or by location")

    # Reading schema configs
    logger.info("Started: reading schema configuration from config file")
    config = configparser.ConfigParser()
    config.read(r"{}/../config/config.ini".format(PATH))
    schema_config = config.get('schema', 'landingFileSchema')
    schema = MetaDataUtils.read_schema(schema_config)
    logger.info("Completed: reading schema configuration from config file")

    # Data extraction
    logger.info("Started: data extraction")
    landing_data = extract_data(spark, schema, logger)
    logger.info("Started: data extraction")

    # Data transformation
    logger.info("Started: data transformation")
    data_transformed = transform_data(landing_data, logger)
    logger.info("Started: data transformation")

    # Reading JDBC configs for performing data load to DB
    logger.info("Started: reading JDBC configuration from config file")
    jdbc_url = config.get('jdbc', 'jdbcUrl')
    jdbc_driver = config.get('jdbc', 'jdbcDriver')
    connector_config = config.get('connector', 'connectionStringWeather')
    logger.info("Completed: reading JDBC configuration from config file")

    # Data load
    logger.info("Started: data load")
    load_data(data_transformed, connector_config, jdbc_url, jdbc_driver, logger)
    logger.info("Started: data load")

    logger.info("Completed: ELT Job ")

    # Stopping SparkContext
    spark.stop()
    logger.info("Stopped SparkContext")

    logger.info("################################################################################")
    logger.info("############### Successfully Finished Execution of Data Load Job ###############")
    logger.info("################################################################################")