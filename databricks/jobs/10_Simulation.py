# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## 1. Introducción
# MAGIC
# MAGIC > _Adaptación por Raúl Jiménez de la obra original de Juan Carlos Alfaro Jiménez, para el caso de uso de "Aseguradora de vehículos"_
# MAGIC
# MAGIC Esta libreta simula la **llegada incremental de transacciones y etiquetas de fraude** al sistema de producción, replicando el comportamiento de un entorno real donde los datos llegan de forma continua desde sistemas externos.
# MAGIC
# MAGIC El mecanismo consiste en leer los archivos `data.json` almacenados en la carpeta **`source_buffer`** bajo el volumen **`landing_zone`**, que contienen los datos del futuro aún no procesados, y escribir sus registros en la estructura de directorios de **`events`**, respetando la **partición por año y mes**. Cada ejecución se escribe como un fichero independiente, de forma que el **`Auto Loader`** detecta cada fichero nuevo y lo ingiere de forma incremental sin necesidad de modificar ficheros existentes.
# MAGIC
# MAGIC Las transacciones se inyectan en **orden cronológico estricto**. Para cada ventana temporal inyectada, se copian también todas las etiquetas cuyo **`label_available_date`** cae dentro de esa misma ventana, simulando el **retraso real** con el que los equipos de revisión confirman los casos de fraude.
# MAGIC
# MAGIC La cantidad de datos inyectados en cada ejecución se controla mediante un único parámetro configurable:
# MAGIC
# MAGIC * **`hours_to_inject`**: número de horas de datos del *buffer* que se inyectan en cada ejecución. Por ejemplo, `hours_to_inject = 3` inyecta todas las transacciones del *buffer* cuyo `timestamp` cae dentro de las 3 horas siguientes al último punto de continuación.
# MAGIC
# MAGIC La simulación es **idempotente**: antes de comenzar, localiza automáticamente el **`timestamp` máximo** ya presente en `events/transactions` y omite todas las filas anteriores o iguales a ese valor, por lo que puede relanzarse sin duplicar datos en caso de fallo.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. Importaciones y configuración

# COMMAND ----------

exec(open("07_Utils.py").read(), globals())

# COMMAND ----------

import json
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F

# COMMAND ----------

# Base paths on the volume
landing_zone_path = Path("/") / "Volumes"/ catalog / database / "landing_zone"
source_buffer_tx_path = landing_zone_path / "source_buffer" / "transactions"
source_buffer_lbl_path = landing_zone_path / "source_buffer" / "labels"
events_tx_path = landing_zone_path / "events" / "transactions"
events_lbl_path = landing_zone_path / "events" / "labels"

# Hours of transaction data to inject in this run
dbutils.widgets.text("hours_to_inject", "3")
hours_to_inject = dbutils.widgets.get("hours_to_inject")
hours_to_inject = int(hours_to_inject)

print(f"Source buffer (transactions): {source_buffer_tx_path}")
print(f"Source buffer (labels): {source_buffer_lbl_path}")
print(f"Events (transactions): {events_tx_path}")
print(f"Events (labels): {events_lbl_path}")
print(f"Hours to inject: {hours_to_inject}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2. Punto de continuación
# MAGIC
# MAGIC Antes de comenzar la simulación se determina desde qué punto debe continuar, consultando el `timestamp` máximo ya presente en `events/transactions` mediante una lectura distribuida con **`Spark`**. Todas las transacciones del *buffer* con `timestamp` estrictamente posterior a ese valor serán las candidatas a copiar.

# COMMAND ----------

def _find_latest_events_timestamp():
    """
    Query the maximum `timestamp` already present in `events/transactions`
    and return it as a `datetime` object.

    Returns `None` when no transactions have been copied yet.
    """
    try:
        max_ts = (
            spark.read
                 .json(str(events_tx_path / "*" / "*" / "*.json"))
                 .agg(F.max("timestamp"))
                 .first()[0]
        )
        return datetime.fromisoformat(max_ts) if max_ts else None
    except Exception:
        return None


resume_from = _find_latest_events_timestamp()

if resume_from is None:
    print("No prior transactions found. Simulation will start from the beginning of the buffer.")
else:
    print(f"Resuming from timestamp: {resume_from.isoformat()}")
    print("Only transactions strictly after this timestamp will be copied.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 3. Carga y ordenación del *buffer* de transacciones
# MAGIC
# MAGIC Se leen todos los archivos `data.json` de `source_buffer/transactions` mediante una lectura distribuida con **`Spark`**, se fusionan en una única lista ordenada cronológicamente por **`timestamp`** y se filtran las filas ya procesadas según el punto de continuación determinado en la sección anterior.

# COMMAND ----------

def _load_buffer(base_path):
    """
    Extract `_year` and `_month` from the partition path of each record
    and return the full buffer as a flat list of dicts for in-memory processing.
    """
    return (
        spark.read
             .json(str(base_path / "*" / "*" / "data.json"))
             .withColumn("_year",  F.element_at(F.split(F.col("_metadata.file_path"), "/"), -3))
             .withColumn("_month", F.element_at(F.split(F.col("_metadata.file_path"), "/"), -2))
             .toPandas()
             .to_dict("records")
    )


all_tx = _load_buffer(source_buffer_tx_path)
for row in all_tx:
    row["_ts"] = datetime.fromisoformat(str(row["timestamp"]))
all_tx.sort(key = lambda row: row["_ts"])
print(f"Total rows in buffer : {len(all_tx):,}")

if resume_from is not None:
    cutoff = resume_from + timedelta(hours = hours_to_inject)
    pending_tx = [row for row in all_tx if resume_from < row["_ts"] <= cutoff]
else:
    cutoff = all_tx[0]["_ts"] + timedelta(hours = hours_to_inject)
    pending_tx = [row for row in all_tx if row["_ts"] <= cutoff]

print(f"Rows in buffer: {len(all_tx):,}")
print(f"Rows selected for injection: {len(pending_tx):,}")
print(f"Rows skipped (past or future): {len(all_tx) - len(pending_tx):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 4. Carga del *buffer* de etiquetas
# MAGIC
# MAGIC Se leen todos los archivos `data.json` de `source_buffer/labels` con la misma estrategia que las transacciones. Solo se consideran las etiquetas cuyo **`label_available_date`** no es nulo, ya que las restantes corresponden a casos aún no resueltos por los equipos de revisión.

# COMMAND ----------

all_lbl = _load_buffer(source_buffer_lbl_path)
print(f"Total rows in label buffer: {len(all_lbl):,}")

for row in all_lbl:
    lad = row.get("label_available_date")
    row["_lad"] = datetime.fromisoformat(lad) if lad else None

available_lbl = [row for row in all_lbl if row["_lad"] is not None]
available_lbl.sort(key = lambda row: row["_lad"])

# Track which labels have already been copied
copied_label_ids = set()

print(f"Labels with valid available date: {len(available_lbl):,}")
print(f"Labels with null available date (skipped): {len(all_lbl) - len(available_lbl):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 5. Inyección de datos
# MAGIC
# MAGIC Cada ejecución inyecta todas las transacciones del *buffer* cuyo `timestamp` cae dentro de la ventana temporal definida por **`hours_to_inject`** horas a partir del último punto de continuación. Las transacciones se agrupan por partición de destino `(year, month)` y se escriben en un **fichero independiente** en formato *newline-delimited* `.json`, de forma que el **`Auto Loader`** lo detecta y lo ingiere sin necesidad de modificar ficheros existentes. Las etiquetas cuyo **`label_available_date`** cae dentro de esa misma ventana se copian simultáneamente, simulando el retraso real con el que los equipos de revisión confirman los casos de fraude.

# COMMAND ----------

def _write_json(dest_path, records):
    """
    Write `records` to `dest_path` on the volume in newline-delimited
    `.json`s format (one record per line).
    """
    lines = "\n".join(json.dumps(record, default = str) for record in records)
    dbutils.fs.put(dest_path, lines, overwrite = True)


def _clean_row(row):
    """
    Remove internal metadata keys added during loading before writing to disk.
    """
    return {key: value for key, value in row.items() if not key.startswith("_")}

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 5.1. Copia de etiquetas por ventana temporal
# MAGIC
# MAGIC Por cada lote de transacciones, se seleccionan las etiquetas cuyo **`label_available_date`** cae dentro de la ventana temporal del lote y que aún no han sido copiadas en iteraciones anteriores. Las etiquetas se agrupan por partición de destino `(year, month)` y se escriben en **`events/labels`** como un fichero independiente por lote.

# COMMAND ----------

# en la primera ejecución window_start es el timestamp de la primera transacción del lote, que puede dejar fuera labels cuyo label_available_date es anterior a esa primera transacción
def _copy_labels(window_start, window_end, batch_timestamp, is_first_run=False):
    """
    Copy all labels whose `label_available_date` falls within
    `[window_start, window_end]` and have not yet been copied.

    Returns the number of labels written.
    """
    to_copy = [
        row for row in available_lbl
        if (row["_lad"] <= window_end if is_first_run else window_start <= row["_lad"] <= window_end)
        and row["transaction_id"] not in copied_label_ids
    ]
    if not to_copy:
        return 0

    # Group by destination partition (year, month)
    partitions = {}
    for row in to_copy:
        key = (row["_year"], row["_month"])
        partitions.setdefault(key, []).append(row)

    for (year, month), rows in partitions.items():
        dest_path = str(events_lbl_path / year / month / f"batch_{batch_timestamp}.json")
        _write_json(dest_path, [_clean_row(r) for r in rows])
        for row in rows:
            copied_label_ids.add(row["transaction_id"])

    return len(to_copy)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 5.2. Ejecución
# MAGIC
# MAGIC Las transacciones de cada lote se agrupan por partición de destino `(year, month)` y se escriben en **`events/transactions`** como un fichero `.json` independiente.

# COMMAND ----------

if not pending_tx:
    print("No pending transactions for this time window.")
else:
    n_pending = len(pending_tx)
    batch_ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    header = f"Injecting {n_pending:,} transactions covering {hours_to_inject} hour(s) from {pending_tx[0]['_ts']} to {pending_tx[-1]['_ts']}."
    separator = "-" * len(header)

    print(header)
    print(separator)

    # 1. Copy transactions, grouped by (year, month) partition
    tx_partitions = {}
    for row in pending_tx:
        key = (row["_year"], row["_month"])
        tx_partitions.setdefault(key, []).append(row)

    for (year, month), rows in tx_partitions.items():
        dest_path = str(events_tx_path / year / month / f"batch_{batch_ts}.json")
        _write_json(dest_path, [_clean_row(r) for r in rows])

    # 2. Copy labels whose available date falls within the injection window
    n_lbl = _copy_labels(pending_tx[0]["_ts"], pending_tx[-1]["_ts"], batch_ts, is_first_run=(resume_from is None))

    print(separator)
    print("Injection complete.")
    print(f"Transactions injected: {n_pending:,}")
    print(f"Labels injected: {n_lbl:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 6. Conclusiones y siguientes pasos
# MAGIC
# MAGIC ### ¿Qué hace esta libreta?
# MAGIC
# MAGIC 1. **Continuación idempotente**: Antes de comenzar, consulta el `timestamp` máximo ya presente en `events/transactions` mediante una lectura distribuida con `Spark`, omitiendo todas las filas anteriores o iguales a ese valor para evitar duplicados en ejecuciones repetidas.
# MAGIC 2. **Inyección por ventana temporal**: Las transacciones se procesan en **orden cronológico estricto** y se escriben en `events` respetando la partición `year/month`, generando un fichero independiente por ejecución que el **`Auto Loader`** detecta e ingiere de forma incremental. El parámetro **`hours_to_inject`** controla cuántas horas de datos del buffer se inyectan en cada ejecución.
# MAGIC 3. **Propagación simultánea de etiquetas**: Para cada ejecución, se copian las etiquetas cuyo **`label_available_date`** cae dentro de la ventana temporal inyectada, simulando el retraso real de confirmación de fraude.
# MAGIC
# MAGIC ### ¿Cuándo ejecutar esta libreta?
# MAGIC
# MAGIC Durante las primeras pruebas, esta libreta se ejecuta **manualmente** desde el entorno de desarrollo para validar el flujo completo de extremo a extremo. Una vez validado, se integra como tarea `Run_Simulation` en el trabajo **`Credit Card Fraud Simulation Pipeline`**, que se ejecuta cada hora para inyectar automáticamente nuevas instancias en `events/` antes de que el pipeline `Medallion` las ingiera.
# MAGIC
# MAGIC ### ¿Qué sigue?
# MAGIC
# MAGIC Tras cada ejecución, el trabajo **`Credit Card Fraud Feature Pipeline`** (o su *scheduler* programado) ingiere los nuevos ficheros a través del pipeline `Medallion`, publica las características actualizadas en el *online feature store* y genera predicciones sobre las transacciones recién llegadas mediante el trabajo **`Credit Card Fraud Inference Pipeline`**, que se encarga de la inferencia, el enriquecimiento de etiquetas y la actualización del monitor de producción.