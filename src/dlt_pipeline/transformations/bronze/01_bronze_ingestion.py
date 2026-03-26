import pyspark.sql.functions as F
# import dlt as dp # Uncomment this line if you are using Databricks Delta Live Tables

from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp, regexp_extract
from delta import configure_spark_with_delta_pip

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

    df.write.format("delta") \
        .mode("overwrite") \
        .save(f"{BRONZE_PATH}/policies")


def ingest_claims():

    df = (
        spark.readStream
        .format("json")
        .option("maxFilesPerTrigger", 1)
        .load("data/events/*/*/claims/")
    )

    df = add_audit_columns(df)

    (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", f"{BRONZE_PATH}/_checkpoints/claims")
        .outputMode("append")
        .start(f"{BRONZE_PATH}/claims")
    )


def ingest_labels():

    df = (
        spark.readStream
        .format("json")
        .option("maxFilesPerTrigger", 1)
        .load("data/events/*/*/labels/")
    )

    df = add_audit_columns(df)

    (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", f"{BRONZE_PATH}/_checkpoints/labels")
        .outputMode("append")
        .start(f"{BRONZE_PATH}/labels")
    )


def ingest_claims_buffer():

    df = (
        spark.readStream
        .format("json")
        .option("maxFilesPerTrigger", 1)
        .load("data/source_buffer/*/*/claims/")
    )

    df = add_audit_columns(df)

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
    ingest_claims()
    ingest_labels()
    ingest_claims_buffer()

    spark.streams.awaitAnyTermination()
