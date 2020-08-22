import logging
import time
import json
from sys import exit
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

# Schema for incoming data
schema = StructType() \
    .add("crime_id", StringType(), False) \
    .add("original_crime_type_name", StringType(), False) \
    .add("report_date", StringType(), False) \
    .add("call_date", StringType(), False) \
    .add("offense_date", StringType(), False) \
    .add("call_time", StringType(), False) \
    .add("call_date_time", StringType(), False) \
    .add("disposition", StringType(), False) \
    .add("address", StringType()) \
    .add("city", StringType()) \
    .add("state", StringType()) \
    .add("agency_id", StringType()) \
    .add("address_type", StringType()) \
    .add("common_location", StringType())

def run_spark_job(spark):

    try:
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "san.francisco.crime.stats.pd.calls") \
            .option("maxOffsetsPerTrigger", 200) \
            .option("startingOffsets", "earliest") \
            .option("backpressure.enabled", True) \
            .load()
        logger.info("Stream reading initiated successfully...")
    except Exception as e:
        logger.warning("Error reading stream")
        logger.error(f"{e}")

    # Printing schema of incoming data for debugging
    logger.info("Incoming stream schema...")
    df.printSchema()

    kafka_df = df.selectExpr("CAST(value AS STRING)")

    # Parse single String value column into DF representing schema defined
    service_table = kafka_df \
        .select(psf.from_json(psf.col('value'), schema).alias("DF")) \
        .select("DF.*")

    # Select relevant columns required for Crime Type aggregation
    crime_table = service_table.select("original_crime_type_name"
                          , "disposition"
                          , psf.to_timestamp("call_date_time") \
                               .alias("call_timestamp"))

    # Count number of incidents reported for given Crime Type 
    # with sliding window and watermarking
    agg_df = crime_table \
            .withWatermark("call_timestamp", "5 minutes") \
            .groupBy(
                psf.window(crime_table.call_timestamp
                    , "60 minutes"
                    , "15 minutes")
              , "original_crime_type_name") \
            .count() \
            .orderBy(['window','count'], ascending=[0,0])

    # Stream output to Console
    query = agg_df \
           .writeStream \
           .outputMode("complete") \
           .format("console") \
           .queryName("Windowed Aggregation by Crime Type") \
           .trigger(processingTime='15 seconds') \
           .option("truncate", False) \
           .start()
           
    # Get the path to radio code json file
    radio_code_json_filepath = f"{Path(__file__).parents[0]}/radio_code.json"

    # Read JSON file
    try:
        radio_code_df = spark.read.json(radio_code_json_filepath
                                      , multiLine=True)
        logger.info("Successfully read Radio Code JSON file...")
    except Exception as e:
        logger.warning("Error reading Radio Code JSON file...")
        logger.error(f"{e}")

    # Rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    #JOIN Crime Type DF and Radio Code DF on disposition column
    crime_radio_code_join_df = crime_table \
                    .join(radio_code_df
                        , "disposition"
                        , "left_outer")
    
    join_query = crime_radio_code_join_df \
            .writeStream \
            .format("console") \
            .queryName("Windowed Radio Code Join") \
            .trigger(processingTime='15 seconds') \
            .option("truncate", False) \
            .start()

    # Wait for ANY query to terminate
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # Create Spark in Standalone mode
    try:    
        spark = SparkSession \
            .builder \
            .master("local[*]") \
            .appName("SanFranciscoCrimeKafkaSparkStructuredStreaming") \
            .config('spark.ui.port', 3000) \
            .config('spark.driver.memory', '4g') \
            .config('spark.executor.memory', '4g') \
            .config('spark.dynamicAllocation.enabled', True) \
            .config('spark.shuffle.service.enabled', True) \
            .getOrCreate()

#        spark.sparkContext.setLogLevel("WARN")
        logger.info("Instantiated SparkSession")
    except Exception as e:
        logger.warning("Failed to instantiate SparkSession")
        logger.error(f"{e}")
        logger.info("Shutting down applicaiton...")
        exit()

    try:
        run_spark_job(spark)
    except KeyboardInterrupt:
        logger.warning("Shutting down due to Keyboard Interruption")

    spark.stop()
