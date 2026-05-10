# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Inferencia en producción y enriquecimiento de etiquetas reales
# MAGIC
# MAGIC **Autor**: Juan Carlos Alfaro Jiménez
# MAGIC
# MAGIC > **Adaptado** por Raúl Jiménez de la Cruz para el dominio de detección de fraudes de seguros de automóvil
# MAGIC
# MAGIC Esta libreta implementa dos responsabilidades complementarias que cierran el ciclo de monitorización del modelo en producción.
# MAGIC
# MAGIC La primera es la **inferencia en lote**: carga el *pipeline* `Spark MLlib` registrado bajo el alias `champion` en `Unity Catalog`, lee las transacciones que han llegado desde la última ejecución y que aún no tienen predicción en `gold_fraud_inference_enriched`, las enriquece con las características del cliente mediante el `PiT` *join* del *feature store* y las transforma con `.transform()` de forma distribuida sobre el clúster de `Databricks`.
# MAGIC
# MAGIC La segunda es el **enriquecimiento de etiquetas**: los fraudes se confirman con retraso una vez que el equipo de revisión cierra cada caso. Cuando una etiqueta real llega a `bronze_labels`, se propaga a `gold_fraud_inference_enriched` mediante un `MERGE` incremental idempotente. Esta tabla, que combina las características de cada transacción, la predicción del modelo y la etiqueta real confirmada, es el dato de entrada de `Databricks Lakehouse Monitoring` para calcular métricas de rendimiento y equidad en producción.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. Importaciones y configuración

# COMMAND ----------

# MAGIC %pip install databricks-feature-engineering>=0.13.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

exec(open("07_Utils.py").read(), globals())

# COMMAND ----------

exec(open("08_Utils.py").read(), globals())

# COMMAND ----------

exec(open("09_Utils.py").read(), globals())

# COMMAND ----------

# MAGIC %md
# MAGIC > Aquí adapto ligeramente la variable `transaction_id_column` para que su valor sea, realmente, `"claim_id"`, que es nuestro verdadero dominio del proyecto, pero mantenemos intacta la libreta.

# COMMAND ----------

transaction_id_column = claim_id_column # "claim_id"

# COMMAND ----------

from datetime import timedelta

from databricks.feature_engineering import FeatureEngineeringClient

from delta.tables import DeltaTable

import mlflow
import mlflow.spark
from mlflow import MlflowClient

from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

# COMMAND ----------

notebook_path_raw = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook = Path(notebook_path_raw).name

print(f"Project: {project}, team: {team}, environment: {environment}")
print(f"Notebook: {notebook}")
print(f"Champion model: {uc_model_name}")
print(f"Spine table: {spine_table}")
print(f"Inference enriched table: {inference_enriched_table}")
print(f"Fraud labels table: {fraud_labels_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2. Carga del modelo `champion`
# MAGIC
# MAGIC Se carga el *pipeline* `Spark MLlib` registrado bajo el alias `champion` en `Unity Catalog`. La versión concreta se persiste en `gold_fraud_inference_enriched` para garantizar la trazabilidad completa entre cada predicción y el modelo que la generó.

# COMMAND ----------

mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")

client = MlflowClient()
champion_version = client.get_model_version_by_alias(name = uc_model_name, alias = "champion")
champion_model_version = champion_version.version

pipeline_model = mlflow.spark.load_model(f"models:/{uc_model_name}@champion")

print(f"Champion model loaded: {uc_model_name}")
print(f"Version: {champion_model_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 3. Inicialización de la tabla de inferencias enriquecidas
# MAGIC
# MAGIC La tabla `gold_fraud_inference_enriched` debe existir antes de poder leer las transacciones ya puntuadas en la sección siguiente. Si es la primera ejecución, se crea vacía con el esquema correcto derivado de un único ejemplo de predicción. En ejecuciones posteriores esta sección no tiene ningún efecto.
# MAGIC
# MAGIC Una vez creada, hay que configurar el monitor de `Databricks Lakehouse Monitoring` desde el `Catalog Explorer` siguiendo los pasos descritos en la sección de conclusiones antes de programar la ejecución automática de esta libreta.

# COMMAND ----------

table_exists = spark.catalog.tableExists(inference_enriched_table)

if not table_exists:
    (
        spark.table(baseline_table_name)
             .limit(0)
             .write
             .format("delta")
             .mode("overwrite")
             .option("overwriteSchema", "true")
             .saveAsTable(inference_enriched_table)
    )

    spark.sql(
        f"ALTER TABLE {inference_enriched_table} "
        f"SET TBLPROPERTIES ("
        f"'delta.enableChangeDataFeed' = 'true', "
        f"'project' = '{project}', "
        f"'team' = '{team}')"
    )

    spark.sql(f"""
        ALTER TABLE {inference_enriched_table}
        SET TAGS (
            'project' = '{project}',
            'team' = '{team}'
        )
    """)

    spark.sql(
        f"COMMENT ON TABLE {inference_enriched_table} IS "
        f"'Production inference table. Contains raw transaction features, "
        f"model predictions and confirmed fraud labels. "
        f"Monitored by `Databricks Lakehouse Monitoring` against `gold_fraud_test_baseline`.'"
    )

    print(f"Table created: {inference_enriched_table}")
else:
    print(f"Table already exists: {inference_enriched_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 4. Lectura de transacciones pendientes de predicción
# MAGIC
# MAGIC Las transacciones de producción son aquellas posteriores al `champion_test_end_date`, la fecha máxima de datos que vio el `champion` actual durante alguna de sus fases (entrenamiento, validación o prueba). Este valor se lee de la etiqueta `test_end_date` de la versión del `champion` en `Unity Catalog`, escrita en `08_Production.ipynb` durante la promoción. Se usa esta fecha en lugar de recalcularla desde `gold_fraud_training_dataset` para blindar el *pipeline* contra posibles descoordinaciones: si la tabla de entrenamiento se actualiza antes de que termine un ciclo completo de reentrenamiento, el valor recalculado quedaría adelantado respecto al que realmente vio el `champion`, dejando transacciones sin puntuar. Se filtra `gold_fraud_inference_spine` por esa fecha y se excluyen mediante un `LEFT ANTI JOIN` las transacciones que ya tienen predicción en `gold_fraud_inference_enriched`. La operación es idempotente: si la libreta falla y se relanza no se duplican predicciones.

# COMMAND ----------

champion_test_end_date = champion_version.tags.get("test_end_date")

production_start_date = (
    datetime.strptime(champion_test_end_date, "%Y-%m-%d") + timedelta(days = 1)
).strftime("%Y-%m-%d")

already_scored_df = (
    spark.table(inference_enriched_table)
         .select(transaction_id_column)
)

new_spine_df = (
    spark.table(spine_table)
         .filter(F.col(date_column) >= production_start_date)
         .join(already_scored_df, on = transaction_id_column, how = "left_anti")
)

n_new = new_spine_df.count()
print(f"Production cutoff date: {production_start_date}")
print(f"New transactions pending prediction: {n_new:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 5. Enriquecimiento con el *feature store*
# MAGIC
# MAGIC Las características del cliente no viajan con la transacción: en producción se recuperan en tiempo real desde el *online feature store*. En este entorno se simulan mediante un `PiT` *join* contra `gold_customer_profile` y `gold_customer_aggregations_inference`, replicando exactamente el enriquecimiento que realizó `05_Training_Dataset_Generation` durante el entrenamiento. Esto garantiza que no hay *training-serving skew*: el *pipeline* recibe exactamente el mismo conjunto de características con el que fue ajustado.

# COMMAND ----------

fe = FeatureEngineeringClient()

inference_set = fe.create_training_set(
    df = new_spine_df,
    feature_lookups = feature_lookups,
    label = None
)

new_transactions_df = inference_set.load_df()

# Columnas de agregacion temporal que pueden ser null para policyholders
# sin historial en la ventana. El VectorAssembler rechaza nulls.
# Consistente con el fillna(0) aplicado en el pipeline de entrenamiento.

_agg_fillna_cols = [
    "num_claims_1h", "total_amount_1h", "avg_amount_1h", "num_telematics_1h",
    "num_claims_24h", "total_amount_24h", "avg_amount_24h", "num_telematics_24h",
    "num_claims_7d", "total_amount_7d", "avg_amount_7d", "num_telematics_7d",
    "num_fraud_confirmed_7d",
    "num_claims_30d", "total_amount_30d", "avg_amount_30d", "num_telematics_30d",
    "num_fraud_confirmed_30d", "num_unique_shops_30d",
    "amount_ratio_24h_vs_30d", "fraud_rate_30d",
]
new_transactions_df = new_transactions_df.fillna(0, subset=_agg_fillna_cols)

print(f"Enriched transactions: {new_transactions_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 6. Predicción e inserción en la tabla enriquecida
# MAGIC
# MAGIC Se aplica `.transform()` del *pipeline* `Spark MLlib` sobre el `DataFrame` de transacciones nuevas. Se extrae la probabilidad de fraude como columna escalar desde el vector de probabilidades, y se añaden el instante de inferencia y la versión del modelo. La columna `is_fraud` se fuerza a nulo porque la etiqueta real no es conocida en el momento de la predicción.
# MAGIC
# MAGIC El `MERGE` inserta únicamente las filas cuyo `transaction_id` no existe aún en la tabla enriquecida.

# COMMAND ----------

if n_new > 0:
    # Base columns equal to exactly what came from the feature-store enriched spine,
    # minus the label (which we add fresh as null).
    # This guarantees the output schema matches baseline table.
    base_columns = [column for column in new_transactions_df.columns if column != label_column]

    scored_df = (
        pipeline_model
        .transform(new_transactions_df)
        .withColumn(
            prob_fraud_column,
            vector_to_array(F.col(probability_column)).getItem(1)
        )
        .withColumn(inference_timestamp_col, F.current_timestamp())
        .withColumn(model_version_col, F.lit(champion_model_version))
        .withColumn(label_column, F.lit(None).cast("long"))
        .withColumn(prediction_column, F.col(prediction_column).cast("long"))
        .select(
            *base_columns,
            label_column,
            prediction_column,
            prob_fraud_column,
            model_version_col,
            inference_timestamp_col
        )
    )

    n_fraud_pred = scored_df.filter(F.col(prediction_column) == 1).count()
    n_legit_pred = scored_df.filter(F.col(prediction_column) == 0).count()

    (
        DeltaTable.forName(spark, inference_enriched_table)
                  .alias("target")
                  .merge(
                      scored_df.alias("source"),
                      f"target.{transaction_id_column} = source.{transaction_id_column}"
                  )
                  .whenNotMatchedInsertAll()
                  .execute()
    )

    print(f"Inserted {n_new:,} new predictions into {inference_enriched_table}")
    print(f"Predicted fraud: {n_fraud_pred:,} ({100 * n_fraud_pred / n_new:.2f}%)")
    print(f"Predicted legit: {n_legit_pred:,} ({100 * n_legit_pred / n_new:.2f}%)")
else:
    print("No new transactions to score.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 7. Propagación de etiquetas reales
# MAGIC
# MAGIC Se leen de `bronze_labels` únicamente las etiquetas de transacciones de producción (posteriores a `production_start_date`) cuyo `transaction_id` no tiene todavía una etiqueta confirmada en `gold_fraud_inference_enriched`. El resultado esperado es cero etiquetas pendientes en las primeras ejecuciones, ya que los fraudes se confirman con retraso una vez que el equipo de revisión cierra cada caso. El `MERGE` actualiza `is_fraud` en las filas que ya existen en la tabla. Las filas sin etiqueta confirmada permanecen con `is_fraud` nulo hasta que se confirmen en una ejecución posterior.

# COMMAND ----------

already_labelled_df = (
    DeltaTable.forName(spark, inference_enriched_table)
              .toDF()
              .filter(F.col(label_column).isNotNull())
              .select(transaction_id_column)
)

pending_labels_df = (
    spark.table(fraud_labels_table)
         .filter(F.col(label_available_date_column) >= production_start_date)
         .join(already_labelled_df, on = transaction_id_column, how = "left_anti")
         .select(transaction_id_column, label_column)
)

n_pending_total = pending_labels_df.count()
n_pending_fraud = pending_labels_df.filter(F.col(label_column) == 1).count()
n_pending_legit = pending_labels_df.filter(F.col(label_column) == 0).count()

print(f"Labels pending propagation: {n_pending_total:,}")
print(f"Fraud: {n_pending_fraud:,}")
print(f"Legit: {n_pending_legit:,}")

# COMMAND ----------

(
    DeltaTable.forName(spark, inference_enriched_table)
              .alias("target")
              .merge(
                  pending_labels_df.alias("source"),
                  f"target.{transaction_id_column} = source.{transaction_id_column}"
              )
              .whenMatchedUpdate(
                  set = {f"target.{label_column}": f"source.{label_column}"}
              )
              .execute()
)

print(f"MERGE completed. {n_pending_total:,} labels propagated to {inference_enriched_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 8. Conclusiones y siguientes pasos
# MAGIC
# MAGIC ### ¿Qué hace esta libreta?
# MAGIC
# MAGIC 1. **Carga del modelo `champion`**: Recupera el pipeline `Spark MLlib` directamente desde el alias `champion` de `Unity Catalog` y la versión concreta para trazabilidad.
# MAGIC 2. **Inicialización de la tabla**: Crea `gold_fraud_inference_enriched` vacía con el esquema correcto si es la primera ejecución, copiando la estructura de `gold_fraud_test_baseline` que garantiza compatibilidad con el monitor, y activa `delta.enableChangeDataFeed` para `Databricks Lakehouse Monitoring`.
# MAGIC 3. **Lectura de transacciones pendientes**: Filtra `gold_fraud_inference_spine` para considerar únicamente las transacciones posteriores a `production_start_date` (el día siguiente a `test_end_date`, último dato visto por el modelo) e identifica mediante un `LEFT ANTI JOIN` las que todavía no tienen predicción en `gold_fraud_inference_enriched`. La operación es idempotente.
# MAGIC 4. **Enriquecimiento con el *feature store***: Replica el `PiT` *join* de entrenamiento contra `gold_customer_profile` y `gold_customer_aggregations_inference` con las mismas `feature_names` definidas en `09_Utils.py`, garantizando que no hay *training-serving skew*.
# MAGIC 5. **Inferencia en lote distribuida**: Aplica `.transform()` del pipeline `Spark MLlib` sobre el lote de transacciones nuevas e inserta el resultado en `gold_fraud_inference_enriched` con `is_fraud` nulo.
# MAGIC 6. **Propagación de etiquetas**: Actualiza `is_fraud` en `gold_fraud_inference_enriched` para las transacciones cuya etiqueta real ha llegado a `bronze_labels`, filtrando también por `production_start_date`.
# MAGIC
# MAGIC ### ¿Por qué es necesaria?
# MAGIC
# MAGIC `Databricks Lakehouse Monitoring` necesita las etiquetas reales para calcular métricas de rendimiento en producción (*AUC-PR*, *F1-score*, precisión, exhaustividad) y métricas de equidad sobre los grupos de interés. Sin este paso, el monitor solo puede calcular *data drift* de características pero no detectar degradación real del modelo ni sesgos en subgrupos.
# MAGIC
# MAGIC ### ¿Cuándo se ejecuta?
# MAGIC
# MAGIC #### Primera ejecución (manual)
# MAGIC
# MAGIC 1. Ejecutar la libreta manualmente para que se cree e inicialice `gold_fraud_inference_enriched` con el esquema correcto.
# MAGIC 2. Navegar a la tabla en `Catalog Explorer`, abrir la pestaña `Quality` y hacer clic en `Create monitor`.
# MAGIC 3. Configurar el monitor con los siguientes parámetros: tipo de problema `Classification`, columna temporal `inference_timestamp`, identificador de modelo `model_version`, columna de predicción `prediction`, columna de etiqueta `is_fraud` y tabla de referencia `gold_fraud_test_baseline`.
# MAGIC 4. Configurar las expresiones de *slice* como se detalla a continuación y hacer clic en `Create`.
# MAGIC
# MAGIC #### Ejecuciones posteriores (automáticas)
# MAGIC
# MAGIC Como tarea `Run_Inference_And_Label_Enrichment` en el trabajo `Credit Card Fraud Inference Pipeline`, que corre cada dos horas con un desfase de una hora respecto al trabajo `Credit Card Fraud Feature Pipeline`, para garantizar que las etiquetas confirmadas disponibles en `bronze_labels` se propaguen a `gold_fraud_inference_enriched` en cada ciclo con las características más recientes disponibles.
# MAGIC
# MAGIC #### Expresiones de *slice* para el monitor
# MAGIC
# MAGIC `Databricks Lakehouse Monitoring` calcula todas las métricas de rendimiento y *drift* tanto sobre el total de transacciones como, de forma independiente, sobre cada *slice* declarado. Esto permite detectar degradaciones que solo afectan a un subgrupo concreto aunque las métricas globales sigan siendo buenas.
# MAGIC
# MAGIC Hay dos tipos de *slices* relevantes para este problema:
# MAGIC
# MAGIC ##### *Slices* operativos
# MAGIC
# MAGIC Subgrupos definidos por características de la transacción que concentran la mayor parte del fraude. El monitor calcula métricas de rendimiento separadas para cada uno, lo que permite detectar si el modelo se degrada antes en las transacciones de mayor riesgo:
# MAGIC
# MAGIC | Expresión | Justificación |
# MAGIC |---|---|
# MAGIC | `outside_business_hours = 1`  | Siniestros fuera de horario laboral concentran mayor tasa de fraude. |
# MAGIC | `telematics_anomaly = 1`      | Anomalías telemáticas son el indicador de riesgo más fuerte del dominio. |
# MAGIC | `has_third_party_injury = 1`  | Involucrar lesionados de terceros es el patrón más frecuente en fraude organizado. |
# MAGIC
# MAGIC ##### *Slices* de equidad
# MAGIC
# MAGIC Subgrupos definidos por atributos demográficos del cliente. `Databricks Lakehouse Monitoring` calcula automáticamente métricas de equidad (igualdad de oportunidades, paridad predictiva y paridad estadística) para cada uno cuando el tipo de problema es `Classification` y hay columna de etiqueta. El objetivo es detectar si el modelo genera tasas de falsos positivos desproporcionadas en algún grupo, lo que supondría que ciertos clientes tienen sus transacciones legítimas bloqueadas con más frecuencia que otros:
# MAGIC
# MAGIC | Expresión | Justificación |
# MAGIC |---|---|
# MAGIC | `gender`      | Detecta si la tasa de falsos positivos difiere entre géneros. |
# MAGIC | `age_group`   | Detecta si el modelo es más agresivo con clientes jóvenes o mayores. |
# MAGIC | `region`      | Detecta disparidad geográfica en las métricas entre comunidades autónomas. |
# MAGIC | `region_type` | Detecta si hay diferencia de rendimiento entre zonas urbanas y rurales. |
# MAGIC
# MAGIC Las métricas de equidad generadas automáticamente por el monitor para cada uno de estos *slices* son la **igualdad de oportunidades** (diferencia en tasa de verdaderos positivos entre grupos), la **paridad predictiva** (diferencia en precisión entre grupos) y la **paridad estadística** (diferencia en tasa de predicciones positivas entre grupos). Estas métricas están disponibles en la tabla `gold_fraud_inference_enriched_drift_metrics` generada por el monitor.