# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## 00 · Preparación del *source buffer*
# MAGIC
# MAGIC Esta libreta es un **paso de inicialización único** que construye el directorio `source_buffer` bajo el volumen `landing_zone` a partir de las tablas Delta ya existentes en el metastore.
# MAGIC
# MAGIC El `source_buffer` es el origen de datos que la libreta `10_Simulation.py` consume de forma incremental para simular la llegada continua de transacciones y etiquetas al sistema de producción. Sin este buffer, la simulación no tiene datos de los que alimentarse.
# MAGIC
# MAGIC ### ¿Qué construye esta libreta?
# MAGIC
# MAGIC ```
# MAGIC /Volumes/{catalog}/{database}/landing_zone/
# MAGIC └── source_buffer/
# MAGIC     ├── transactions/
# MAGIC     │   └── {year}/{month}/data.json   ← desde fraud_inference_spine
# MAGIC     └── labels/
# MAGIC         └── {year}/{month}/data.json   ← desde fraud_spine
# MAGIC ```
# MAGIC
# MAGIC ### Fuentes
# MAGIC
# MAGIC | Destino | Tabla fuente | Notas |
# MAGIC |---|---|---|
# MAGIC | `source_buffer/transactions` | `fraud_inference_spine` | `claim_timestamp` → `timestamp`. Sin `is_fraud`. |
# MAGIC | `source_buffer/labels` | `fraud_spine` | Solo `transaction_id`, `is_fraud`, `label_available_date`. |
# MAGIC
# MAGIC ### Idempotencia
# MAGIC
# MAGIC Si `source_buffer/transactions` ya contiene ficheros, la libreta **aborta** sin modificar nada.
# MAGIC Para forzar una reconstrucción completa, borra manualmente el directorio `source_buffer` del volumen y vuelve a ejecutar.
# MAGIC
# MAGIC ### Parámetro `buffer_start_date`
# MAGIC
# MAGIC Solo se incluyen registros con `claim_timestamp >= buffer_start_date`. Debe coincidir con el inicio del período de simulación (típicamente el día siguiente al último registro del conjunto de test del modelo champion).
# MAGIC
# MAGIC > **Nota**: Esta libreta se ejecuta **una sola vez** antes de lanzar el pipeline de simulación. No forma parte del pipeline programado.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Importaciones y configuración

# COMMAND ----------

exec(open("07_Utils.py").read(), globals())

# COMMAND ----------

import json
from datetime import datetime
from pathlib import Path

from pyspark.sql import functions as F

# COMMAND ----------

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
landing_zone_path       = Path("/") / "Volumes" / catalog / database / "landing_zone"
source_buffer_tx_path   = landing_zone_path / "source_buffer" / "transactions"
source_buffer_lbl_path  = landing_zone_path / "source_buffer" / "labels"

# ---------------------------------------------------------------------------
# Parameter: first date to include in the buffer.
# Set this to the day after the last record in your champion model's test set
# so the buffer only contains data the model has never seen during training.
#
# Format: "YYYY-MM-DD"
# ---------------------------------------------------------------------------
dbutils.widgets.text("buffer_start_date", "2024-01-01")
buffer_start_date = dbutils.widgets.get("buffer_start_date")

print(f"Catalog          : {catalog}")
print(f"Database         : {database}")
print(f"Buffer start date: {buffer_start_date}")
print(f"Transactions path: {source_buffer_tx_path}")
print(f"Labels path      : {source_buffer_lbl_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Guardia de idempotencia
# MAGIC
# MAGIC Antes de escribir nada se comprueba si el buffer ya existe. Si hay ficheros en `source_buffer/transactions`, la libreta se detiene para evitar sobreescrituras accidentales.

# COMMAND ----------

def _buffer_already_exists(path: Path) -> bool:
    """
    Return True if `path` contains at least one file on the volume.
    Uses dbutils.fs.ls, which raises an exception if the path does not exist.
    """
    try:
        files = dbutils.fs.ls(str(path))
        # ls returns the partition directories; check recursively one level deeper
        for entry in files:
            sub = dbutils.fs.ls(entry.path)
            for item in sub:
                if item.name.endswith(".json"):
                    return True
        return False
    except Exception:
        # Path does not exist yet — safe to proceed
        return False


if _buffer_already_exists(source_buffer_tx_path):
    raise Exception(
        f"Source buffer already exists at {source_buffer_tx_path}.\n"
        "To rebuild it, delete the 'source_buffer' directory from the volume and re-run this notebook."
    )

print("No existing buffer found. Proceeding with construction.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Construcción del buffer de transacciones
# MAGIC
# MAGIC Se lee `fraud_inference_spine` desde el metastore, se filtra por `buffer_start_date`, y se serializa en particiones `year/month/data.json` en formato *newline-delimited JSON*.
# MAGIC
# MAGIC Transformaciones aplicadas:
# MAGIC * `claim_timestamp` → `timestamp`: nombre que espera `10_Simulation.py` en `F.max("timestamp")` y en `row["timestamp"]`.
# MAGIC * Las columnas `year` y `month` ya están presentes en `fraud_inference_spine` y se usan directamente para la partición de destino.
# MAGIC * `year` y `month` se **excluyen del JSON escrito** porque `10_Simulation.py` los reconstruye desde el path del fichero via `_metadata.file_path`.

# COMMAND ----------

TX_COLUMNS_TO_WRITE = [
    "timestamp",          # renombrado desde claim_timestamp
    "claim_id",
    "policy_id",
    "claimed_amount_eur",
    "accident_type",
    "accident_location_type",
    "days_to_report",
    "n_parties_involved",
    "witnesses",
    "injury_level",
    "police_report_filed",
    "outside_business_hours",
    "has_third_party_injury",
    "third_party_same_insurer",
    "telematics_anomaly",
    "claim_channel",
    "body_shop_id",
    "label_available_date",
]

# COMMAND ----------

tx_df = (
    spark.table(f"{catalog}.{database}.fraud_inference_spine")
    .filter(F.col("claim_timestamp") >= F.lit(buffer_start_date))
    .withColumnRenamed("claim_timestamp", "timestamp")
    # Cast timestamp to ISO string so json.dumps serialises it cleanly
    .withColumn("timestamp",            F.date_format("timestamp",            "yyyy-MM-dd'T'HH:mm:ss"))
    .withColumn("label_available_date", F.date_format("label_available_date", "yyyy-MM-dd'T'HH:mm:ss"))
    # Keep year/month as partition keys but not in the JSON payload
    .select(TX_COLUMNS_TO_WRITE + ["year", "month"])
)

total_tx = tx_df.count()
print(f"Transactions to buffer: {total_tx:,}")
tx_df.show(3, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1. Escritura particionada en el volumen
# MAGIC
# MAGIC Las transacciones se agrupan por `(year, month)` y se escriben en un único fichero `data.json` por partición.
# MAGIC El fichero usa formato *newline-delimited JSON* (un registro por línea), que es el mismo formato que `10_Simulation.py` genera al inyectar lotes.

# COMMAND ----------

def _write_json_volume(dest_path: str, records: list) -> None:
    """
    Write `records` to `dest_path` on the Unity Catalog Volume in
    newline-delimited JSON format (one JSON object per line).
    Uses dbutils.fs.put which works on Volumes in Databricks Serverless.
    """
    lines = "\n".join(json.dumps(record, default=str) for record in records)
    dbutils.fs.put(dest_path, lines, overwrite=True)


# Collect to driver — the buffer is a bounded, historical dataset so this is safe.
# For very large datasets (>50 M rows) consider writing via Spark directly.
tx_rows = tx_df.toPandas().to_dict("records")

# Group by (year, month) partition
tx_partitions: dict = {}
for row in tx_rows:
    key = (str(row["year"]), str(row["month"]))
    tx_partitions.setdefault(key, []).append(row)

written_tx = 0
for (year, month), rows in sorted(tx_partitions.items()):
    # Remove partition columns from JSON payload
    payload = [{k: v for k, v in r.items() if k not in ("year", "month")} for r in rows]
    dest = str(source_buffer_tx_path / year / month / "data.json")
    _write_json_volume(dest, payload)
    written_tx += len(payload)
    print(f"  Written {len(payload):>6,} transactions → {year}/{month}/data.json")

print(f"\nTotal transactions written: {written_tx:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Construcción del buffer de etiquetas
# MAGIC
# MAGIC Se lee `fraud_spine` desde el metastore, se filtra por `buffer_start_date` (usando `label_available_date` como referencia temporal), y se serializa en particiones `year/month/data.json`.
# MAGIC
# MAGIC Transformaciones aplicadas:
# MAGIC * Solo se retienen `claim_id` (renombrado a `transaction_id`), `is_fraud` y `label_available_date`.
# MAGIC * Las filas con `label_available_date IS NULL` se **excluyen**: representan casos aún no resueltos por el equipo de revisión y no deben estar en el buffer.
# MAGIC * `year` y `month` se derivan de `label_available_date` (ya que `fraud_spine` no tiene columna `month`), se usan para la partición y se excluyen del JSON payload.
# MAGIC
# MAGIC > **Nota sobre `transaction_id`**: `10_Simulation.py` usa `row["transaction_id"]` como clave de deduplicación en `copied_label_ids`. Por consistencia, `claim_id` se renombra a `transaction_id` aquí.

# COMMAND ----------

LBL_COLUMNS_TO_WRITE = [
    "transaction_id",       # renombrado desde claim_id
    "is_fraud",
    "label_available_date",
]

# COMMAND ----------

lbl_df = (
    spark.table(f"{catalog}.{database}.fraud_spine")
    .filter(F.col("label_available_date").isNotNull())
    .filter(F.col("label_available_date") >= F.lit(buffer_start_date))
    .withColumnRenamed("claim_id", "transaction_id")
    .withColumn("label_available_date", F.date_format("label_available_date", "yyyy-MM-dd'T'HH:mm:ss"))
    # Derive partition columns from label_available_date
    .withColumn("year",  F.year(F.to_timestamp("label_available_date",  "yyyy-MM-dd'T'HH:mm:ss")).cast("string"))
    .withColumn("month", F.month(F.to_timestamp("label_available_date", "yyyy-MM-dd'T'HH:mm:ss")).cast("string"))
    .select(LBL_COLUMNS_TO_WRITE + ["year", "month"])
)

total_lbl = lbl_df.count()
print(f"Labels to buffer: {total_lbl:,}")
lbl_df.show(3, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1. Escritura particionada en el volumen

# COMMAND ----------

lbl_rows = lbl_df.toPandas().to_dict("records")

lbl_partitions: dict = {}
for row in lbl_rows:
    key = (str(row["year"]), str(row["month"]))
    lbl_partitions.setdefault(key, []).append(row)

written_lbl = 0
for (year, month), rows in sorted(lbl_partitions.items()):
    payload = [{k: v for k, v in r.items() if k not in ("year", "month")} for r in rows]
    dest = str(source_buffer_lbl_path / year / month / "data.json")
    _write_json_volume(dest, payload)
    written_lbl += len(payload)
    print(f"  Written {len(payload):>6,} labels → {year}/{month}/data.json")

print(f"\nTotal labels written: {written_lbl:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verificación del buffer construido
# MAGIC
# MAGIC Se releeeen ambos buffers con Spark para confirmar que los ficheros son legibles, tienen el esquema esperado y el recuento cuadra con lo escrito.

# COMMAND ----------

print("=" * 60)
print("VERIFICATION")
print("=" * 60)

# --- Transactions ---
tx_verify = (
    spark.read
         .json(str(source_buffer_tx_path / "*" / "*" / "data.json"))
         .withColumn("_year",  F.element_at(F.split(F.col("_metadata.file_path"), "/"), -3))
         .withColumn("_month", F.element_at(F.split(F.col("_metadata.file_path"), "/"), -2))
)
tx_count = tx_verify.count()
tx_min_ts, tx_max_ts = tx_verify.agg(F.min("timestamp"), F.max("timestamp")).first()

print(f"\n[Transactions]")
print(f"  Records read back : {tx_count:,}")
print(f"  Timestamp range   : {tx_min_ts}  →  {tx_max_ts}")
print(f"  Partitions found  : {len(tx_partitions)}")
tx_verify.printSchema()

# --- Labels ---
lbl_verify = (
    spark.read
         .json(str(source_buffer_lbl_path / "*" / "*" / "data.json"))
)
lbl_count = lbl_verify.count()
lbl_min_lad, lbl_max_lad = lbl_verify.agg(F.min("label_available_date"), F.max("label_available_date")).first()

print(f"\n[Labels]")
print(f"  Records read back : {lbl_count:,}")
print(f"  LAD range         : {lbl_min_lad}  →  {lbl_max_lad}")
print(f"  Partitions found  : {len(lbl_partitions)}")
lbl_verify.printSchema()

# --- Assertion ---
assert tx_count == written_tx,  f"Transaction count mismatch: wrote {written_tx}, read {tx_count}"
assert lbl_count == written_lbl, f"Label count mismatch: wrote {written_lbl}, read {lbl_count}"

print("\n✓ Buffer verification passed. Ready to run 10_Simulation.py.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Confirmar que los directorios se crearon

# COMMAND ----------

dbutils.fs.ls("/Volumes/workspace/auto_insurance_fraud/landing_zone/source_buffer/")
# Deberías ver: transactions/, labels/

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Conclusiones y siguientes pasos
# MAGIC
# MAGIC ### ¿Qué ha hecho esta libreta?
# MAGIC
# MAGIC 1. **Construido `source_buffer/transactions`** desde `fraud_inference_spine`: registros post `buffer_start_date`, `claim_timestamp` renombrado a `timestamp`, particionados por `year/month`.
# MAGIC 2. **Construido `source_buffer/labels`** desde `fraud_spine`: solo `transaction_id`, `is_fraud` y `label_available_date`, excluyendo etiquetas nulas, particionados por `year/month` derivado de `label_available_date`.
# MAGIC 3. **Verificado** que ambos buffers son legibles por Spark y los recuentos cuadran.
# MAGIC
# MAGIC ### ¿Qué sigue?
# MAGIC
# MAGIC Con el buffer construido, ya puedes:
# MAGIC
# MAGIC 1. **Ejecutar `10_Simulation.py` manualmente** con `hours_to_inject = 12` para validar que las transacciones y etiquetas se inyectan correctamente en `events/`.
# MAGIC 2. **Registrar el Job `Credit Card Fraud Simulation Pipeline`** en Databricks con la cadencia `0 30 0/2 * * ?` y el parámetro `hours_to_inject = 12`.
# MAGIC
# MAGIC > Esta libreta **no debe incluirse** en el pipeline programado. Es un paso de inicialización de un solo uso.