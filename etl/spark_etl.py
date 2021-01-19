from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, col, current_timestamp, to_date
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.types import TimestampType, DateType


"""
    Spark job on Amazon EMR 
    Writes sensor reading data to parquet files on S3-based data lake
"""

def create_spark_session():
    """
        Create or retrieve a Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_sensor_data(spark, input_data, output_data):
    """
        Process sensor data files.
        Compute mean temperature, humidity and light readings for each sensor and reading date.
        Write data to parquet files.
    """
    
    # get filepath to sensor readings data    
    # sensor_data = input_data + 'redshift_processed/*/*/*/*/*.json'
    sensor_data = input_data + '2021/*/*/*/*.json'

    # read sensor data
    print("----- reading sensor data files -----")
    df = spark.read.json(sensor_data)

    # create reading date column from epoch timestamp column
    df = df.withColumn("reading_date", to_date(col("timestamp").cast(dataType=TimestampType())))

    # group by sensor id and reading date
    # create columns for mean values for body temp, motion and rumination per sensor and date
    df = df.groupBy("sensor_id", "reading_date") \
    .agg(mean("body_temperature").alias("avg_temp"), mean("motion").alias("avg_motion"), mean("rumination").alias("avg_rumination")) \
    .orderBy("sensor_id", "reading_date")  

    # create columns for partioning parquet files
    df = df.withColumn("sensor_id_partition", df["sensor_id"]) 
    df = df.withColumn("reading_date_partition", df["reading_date"]) 

    # write daily sensor metrics to parquet files
    # partition files by sensor id and reading date
    print("----- writing sensor table parquet files -----")
    fields = ["sensor_id", "reading_date", "avg_temp", "avg_motion", "avg_rumination","sensor_id_partition", "reading_date_partition"]
    sensor_table = df.selectExpr(fields).dropDuplicates()
    sensor_table.write.mode('overwrite').partitionBy("sensor_id_partition", "reading_date_partition").parquet(output_data)          

    df.printSchema()
    df.show()

    count = df.count()
    print("count: " + str(count))     


def main():
    spark = create_spark_session()
    input_data = "s3://cowculate/"
    output_data = "s3://cowculate/data_lake/"
    
    process_sensor_data(spark, input_data, output_data)    


if __name__ == "__main__":
    main()    