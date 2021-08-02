from pyspark import SparkConf
from pyspark.sql import *
from lib.logger import log4J
import sys

from lib.utils import get_spark_app_config, load_survey_df, count_by_country

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

    #Log an error if filename not available
    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    logger.info("Starting HelloSpark")
    #any processing code

    # Read spark configs
    #conf_out = spark.sparkContext.getConf()
    #logger.info(conf_out.toDebugString())

    survey_df = load_survey_df(spark, sys.argv[1])

    #repartition the dataframe
    partitioned_survey_df = survey_df.repartition(2)

    #spark transformations
    transformed_survey_df = count_by_country(partitioned_survey_df)

    logger.info(transformed_survey_df.collect())

    transformed_survey_df.show()

    logger.info("  Finished HelloSpark")

    input("Press Enter")
    #Stop driver after work is done
    spark.stop()

    print('Hello Spark')

