# way to define spark context
from pyspark.sql import SparkSession
from pyspark.sql.functions import col ,date_format, concat, round, abs,expr,row_number
from pyspark.sql.types import TimestampType, LongType
from pyspark.sql import Window
import sys
import json
from google.cloud import storage

if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("NYC_Taxi") \
        .getOrCreate()
    sc = spark.sparkContext

    # Make sure an argument is passed
    if len(sys.argv) < 2:
        raise SystemExit("Usage: spark-submit script.py <gcs_parquet_path>")

    gcs_path = sys.argv[1]   # first argument after script name
    row_number_count = int(sys.argv[2])
    print(f"Reading parquet from: {gcs_path}")

    #gcs_path = get_file_path()
    df_nyc_taxi = spark.read.parquet(gcs_path)

    df_nyc_taxi = df_nyc_taxi.withColumn(
            "tpep_pickup_datetime",
            col("tpep_pickup_datetime").cast(TimestampType())
        )\
        .withColumn(
            "tpep_dropoff_datetime",
            col("tpep_dropoff_datetime").cast(TimestampType()),
        ).withColumn(
            "tpep_pickup_datetime_id",
                concat(
                    date_format(col("tpep_pickup_datetime"),'yyyy'),
                    date_format(col("tpep_pickup_datetime"),'MM'),
                    date_format(col("tpep_pickup_datetime"), 'dd')
                ).cast(LongType())
        )\
        .withColumn("tpep_dropoff_datetime_id",
            concat(
                date_format(col("tpep_dropoff_datetime"),'yyyy'),
                date_format(col("tpep_dropoff_datetime"),'MM'),
                date_format(col("tpep_dropoff_datetime"), 'dd')
            ).cast(LongType()))
    
    if 'cbd_congestion_fee' in df_nyc_taxi.columns:
        df_nyc_taxi = df_nyc_taxi.withColumn('cbd_congestion_fee', abs(col('cbd_congestion_fee')))

    df_nyc_taxi = df_nyc_taxi.withColumn('fare_amount', abs(col('fare_amount')))\
        .withColumn('Airport_fee', abs(col('Airport_fee')))\
        .withColumn('congestion_surcharge', abs(col('congestion_surcharge')))\
        .withColumn('total_amount', abs(col('total_amount')))\
        .withColumn('improvement_surcharge', abs(col('improvement_surcharge')))\
        .withColumn('tolls_amount', abs(col('tolls_amount')))\
        .withColumn('mta_tax', abs(col('mta_tax')))\
        .withColumn('extra', abs(col('extra')))
    
    df_nyc_taxi = df_nyc_taxi.withColumn("total_amount_and_tip",round(col("tip_amount")+col("total_amount"),2))


    
    w = Window.orderBy(col("tpep_dropoff_datetime_id"))
    df_nyc_taxi = df_nyc_taxi.withColumn("id", row_number().over(w) + (row_number_count - 1))

    row_number_count = row_number_count+ df_nyc_taxi.count()
    payload = {"row_count": row_number_count}


    json_data = json.dumps(payload)
    bucket_name = 'batch_data_gcs'
    destination_blob_name = 'row_count/row_count.json'
    # Create GCS client
    client = storage.Client()
    bucket = client.bucket(bucket_name=bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Upload JSON string
    blob.upload_from_string(json_data, content_type="application/json")

    sfOption = {
        "sfURL": "UVVDXUO-UT91998.snowflakecomputing.com",
        "sfUser": "YOUSEFALAA1476",
        "sfPassword": "Snowflake123456@",
        "sfDatabase": "NYC_TAXI",
        "sfSchema": "TRIPS",
        "sfWarehouse": "COMPUTE_WH",
        "sfRole": "ACCOUNTADMIN", # Optional
        "ocspFailOpen": "true",
    }


    TARGET_DB     = "NYC_TAXI"
    TARGET_SCHEMA = "TRIPS"
    TARGET_TABLE  = "YELLOW_TRIPS"
    FQTN = f"{TARGET_DB}.{TARGET_SCHEMA}.{TARGET_TABLE}"

    # Explicit DDL for database, schema, and table
    ddl = f"""
        USE {TARGET_DB};
        USE SCHEMA {TARGET_SCHEMA};
        CREATE TABLE IF NOT EXISTS {FQTN} (
            VendorID               INTEGER,
            tpep_pickup_datetime   TIMESTAMP_NTZ,
            tpep_dropoff_datetime  TIMESTAMP_NTZ,
            passenger_count        INTEGER,
            trip_distance          DOUBLE,
            RatecodeID             INTEGER,
            store_and_fwd_flag     STRING,
            PULocationID           INTEGER,
            DOLocationID           INTEGER,
            payment_type           INTEGER,
            fare_amount            DOUBLE,
            extra                  DOUBLE,
            mta_tax                DOUBLE,
            tip_amount             DOUBLE,
            tolls_amount           DOUBLE,
            improvement_surcharge  DOUBLE,
            total_amount           DOUBLE,
            congestion_surcharge   DOUBLE,
            Airport_fee            DOUBLE,
            cbd_congestion_fee     DOUBLE,
            tpep_pickup_datetime_id BIGINT,
            tpep_dropoff_datetime_id BIGINT,
            id        BIGINT NOT NULL
        );
    """

    # Write the DataFrame
    (df_nyc_taxi.write
        .format("net.snowflake.spark.snowflake")
        .options(**sfOption)   # your Snowflake connection dict
        .option("dbtable", FQTN)
        .option("preactions", ddl)   # will run all CREATE statements before insert
        .mode("Append")             # or "overwrite" if you want to reload data
        .save())




