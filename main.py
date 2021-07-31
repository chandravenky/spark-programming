from pyspark import SparkConf
from pyspark.sql import *
from lib.logger import log4J

from lib.utils import get_spark_app_config

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    #Create driver
    # Start a spark session
    # Set configuration
    # Local multi-threaded architecture with 3 threads

    conf = get_spark_app_config()

    spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()

    logger = log4J(spark)

    logger.info("Starting HelloSpark")
    #any processing code

    conf_out = spark.sparkContext.getConf() #Read spark configs
    logger.info(conf_out.toDebugString())

    logger.info("Finished HelloSpark")

    #Stop driver after work is done
    spark.stop()

    print('Hello Spark')

