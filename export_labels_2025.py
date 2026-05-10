from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

spark = configure_spark_with_delta_pip(
    SparkSession.builder.appName("export-labels")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
).getOrCreate()

df = (
    spark.read
         .option("recursiveFileLookup", "true")
         .json("data/source_buffer/labels/2025")
         .withColumn("label_available_date", F.to_timestamp("label_available_date"))
)

print(f"Rows: {df.count():,}")
df.printSchema()

df.write.format("parquet").mode("overwrite").save("pipelines/export/labels_2025")

import os
size = sum(
    os.path.getsize(os.path.join(r, f))
    for r, _, files in os.walk("pipelines/export/labels_2025")
    for f in files if f.endswith(".parquet")
)
print(f"Size: {size / 1024**2:.1f} MB")
spark.stop()