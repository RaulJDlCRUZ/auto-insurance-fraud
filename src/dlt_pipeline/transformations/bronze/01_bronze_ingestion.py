import pyspark.sql.functions as F
# import dlt as dp # Uncomment this line if you are using Databricks Delta Live Tables

# Also uncomment/toogle the @dp.table decorators if you are using Delta Live Tables
# and cloudFiles for auto ingestion. If you are running this as a standalone Spark job, you can leave the decorators commented out.

from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from delta import configure_spark_with_delta_pip

"""
Con format("json") estándar en modo streaming, Spark no infiere el esquema automáticamente
a diferencia de cloudFiles que sí lo hace.
Lo definimos explícitamente antes de leer.
"""

claims_schema = StructType([
    StructField("claim_id", StringType(), True),
    StructField("policy_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("accident_type", StringType(), True),
    StructField("accident_location_type", StringType(), True),
    StructField("days_to_report", IntegerType(), True),
    StructField("n_parties_involved", IntegerType(), True),
    StructField("witnesses", StringType(), True),
    StructField("injury_level", StringType(), True),
    StructField("police_report_filed", IntegerType(), True),
    StructField("outside_business_hours", IntegerType(), True),
    StructField("claimed_amount_eur", DoubleType(), True),
    StructField("body_shop_id", StringType(), True),
    StructField("has_third_party_injury", IntegerType(), True),
    StructField("third_party_same_insurer", IntegerType(), True),
    StructField("telematics_anomaly", IntegerType(), True),
    StructField("claim_channel", StringType(), True),
])

labels_schema = StructType([
    StructField("claim_id", StringType(), True),
    StructField("is_fraud", IntegerType(), True),
    StructField("label_available_date", StringType(), True),
])

builder = (
    SparkSession.builder.appName("bronze_ingestion")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

BRONZE_PATH = "pipelines/bronze"


def add_partition_columns(df):

    return (
        df
        .withColumn("year",
            regexp_extract("source_file", r"/(\d{4})/", 1)
        )
        .withColumn("month",
            regexp_extract("source_file", r"/\d{4}/(\d{2})/", 1)
        )
    )


def add_audit_columns(df):
    return (
        df.withColumn("source_file", input_file_name())
          .withColumn("ingestion_timestamp", current_timestamp())
    )


def ingest_policies():
    df = (
        spark.read
        .option("header", True)
        .csv("data/context/policies.csv")
    )

    df = add_audit_columns(df)
    df = add_partition_columns(df)

    df.write.format("delta") \
        .mode("overwrite") \
        .save(f"{BRONZE_PATH}/policies")


def ingest_claims():
    # df = (
    #     spark.readStream
    #     .format("cloudFiles")
    #     .option("cloudFiles.format", "json")
    #     .option("cloudFiles.schemaLocation", f"{BRONZE_PATH}/_schemas/claims")
    #     .load("data/events/claims/*/*/")
    # )
    df = (
        spark.readStream
        .format("json")
        .schema(claims_schema)
        .option("maxFilesPerTrigger", 1)
        .load("data/events/claims/*/*/")
    )
    df = add_audit_columns(df)
    df = add_partition_columns(df)
    (
        df.writeStream
        .format("delta")
        .partitionBy("year", "month")
        .option("checkpointLocation", f"{BRONZE_PATH}/_checkpoints/claims")
        .outputMode("append")
        .start(f"{BRONZE_PATH}/claims")
    )


def ingest_labels():
    # df = (
    #     spark.readStream
    #     .format("cloudFiles")
    #     .option("cloudFiles.format", "json")
    #     .option("cloudFiles.schemaLocation", f"{BRONZE_PATH}/_schemas/labels")
    #     .load("data/events/labels/*/*/")
    # )
    df = (
        spark.readStream
        .format("json")
        .schema(labels_schema)
        .option("maxFilesPerTrigger", 1)
        .load("data/events/labels/*/*/")
    )
    df = add_audit_columns(df)
    df = add_partition_columns(df)
    (
        df.writeStream
        .format("delta")
        .partitionBy("year", "month")
        .option("checkpointLocation", f"{BRONZE_PATH}/_checkpoints/labels")
        .outputMode("append")
        .start(f"{BRONZE_PATH}/labels")
    )


def ingest_claims_buffer():
    # df = (
    #     spark.readStream
    #     .format("cloudFiles")
    #     .option("cloudFiles.format", "json")
    #     .option("cloudFiles.schemaLocation", f"{BRONZE_PATH}/_schemas/claims_buffer")
    #     .load("data/source_buffer/*/*/claims/")
    # )
    df = (
        spark.readStream
        .format("json")
        .schema(claims_schema)
        .option("maxFilesPerTrigger", 1)
        .load("data/source_buffer/claims/*/*/")
    )

    df = add_audit_columns(df)
    df = add_partition_columns(df)
    (
        df.writeStream
        .format("delta")
        .option(
            "checkpointLocation",
            f"{BRONZE_PATH}/_checkpoints/claims_buffer"
        )
        .outputMode("append")
        .start(f"{BRONZE_PATH}/claims_buffer")
    )


if __name__ == "__main__":
    ingest_policies()
    print(f"Ingest Policies OK")
    ingest_claims()
    print(f"Ingest Claims OK")
    ingest_labels()
    print(f"Ingest Labels OK")
    ingest_claims_buffer()
    print(f"Ingest Claims (Buffer) OK")

    spark.streams.awaitAnyTermination(timeout=900)
