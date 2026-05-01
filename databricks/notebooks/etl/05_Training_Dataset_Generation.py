# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Generación del conjunto de datos de entrenamiento
# MAGIC
# MAGIC **Autor**: Juan Carlos Alfaro Jiménez
# MAGIC
# MAGIC > _Adaptado por Raúl Jiménez para el caso de uso de "Aseguradora de vehículos"_
# MAGIC
# MAGIC El objetivo de esta libreta es construir el conjunto de datos de entrenamiento para el modelo de detección de fraude en siniestros de automoción. Para ello, se combina la tabla `fraud_spine` (que contiene el esqueleto de siniestros etiquetados) con las dos tablas de características de la capa `Gold` (`customer_profile` y `customer_aggregations`), usando la `API` del `Feature Store` de `Databricks`.
# MAGIC
# MAGIC El resultado final se guarda como tabla `Delta` estática en `Unity Catalog` bajo el nombre `fraud_training_dataset`. Esta tabla cumple tres funciones críticas en la arquitectura:
# MAGIC
# MAGIC * **Conjunto de datos de entrenamiento reproducible**: el cruce temporal entre la *spine* y las tablas de características (la operación más costosa del *pipeline*) se realiza una única vez. Los experimentos posteriores leen directamente desde esta tabla `Delta`, sin recalcular nada.
# MAGIC * **Fotografía congelada de los datos**: las tablas `Gold` son «vivas» y pueden actualizarse. Al guardar el conjunto de entrenamiento en `Delta`, la versión exacta de los datos que vio el modelo queda registrada de forma inmutable, lo que permite reproducir cualquier entrenamiento meses después.
# MAGIC * **Referencia de *baseline* para monitorización**: esta misma tabla se usará en las libretas de monitorización como perfil de referencia para detectar *data drift* cuando el modelo esté en producción.
# MAGIC
# MAGIC Esta libreta **no contiene lógica de transformación propia**. Todo el trabajo de cruce y enriquecimiento lo realiza internamente `create_training_set`, que delega en `Spark` la ejecución del *point-in-time* (`PiT`) *join* de forma distribuida.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. Importación de librerías y configuración
# MAGIC
# MAGIC La primera celda instala el paquete `databricks-feature-engineering`, necesario para acceder a `create_training_set` y a los objetos `FeatureLookup`.
# MAGIC
# MAGIC A continuación se importan los componentes necesarios y se definen los nombres completamente cualificados de todas las tablas involucradas:
# MAGIC
# MAGIC * `fraud_spine`: la tabla de partida del cruce. Contiene el `claim_id`, el `policy_id`, el `claim_timestamp` de cada siniestro, los detalles del mismo y la etiqueta `is_fraud`.
# MAGIC * `customer_profile`: tabla de características estáticas y lentamente cambiantes del tomador de la póliza (perfil demográfico, tipo de cobertura, vehículo, etc.). Tiene estructura `SCD Type 2` con columnas `__START_AT` y `__END_AT`.
# MAGIC * `customer_aggregations`: tabla de características comportamentales calculadas con ventana deslizante (número de siniestros en los últimos 30 días, importe medio, tasa de fraude histórica, etc.).
# MAGIC * `fraud_training_dataset`: la tabla de salida donde se guardará el conjunto de entrenamiento enriquecido.
# MAGIC
# MAGIC También se inicializa el cliente `FeatureEngineeringClient`.

# COMMAND ----------

# MAGIC %pip install databricks-feature-engineering>=0.13.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
from datetime import datetime, timezone
from pyspark.sql.functions import col, count, max, round, when

# COMMAND ----------

catalog = "workspace"
database = "auto_insurance_fraud"

fraud_spine_table = f"{catalog}.{database}.fraud_spine"
customer_profile_table = f"{catalog}.{database}.customer_profile"
customer_aggregations_table = f"{catalog}.{database}.customer_aggregations"

fraud_training_dataset_table = f"{catalog}.{database}.fraud_training_dataset"

fe = FeatureEngineeringClient()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2. Carga de la *spine*
# MAGIC
# MAGIC La *spine* es el punto de partida de todo el proceso. Es la tabla que determina **qué filas** compondrán el conjunto de entrenamiento y **en qué instante** se evalúa cada cruce. Sin la *spine*, la `API` `create_training_set` no sabe ni cuántas filas debe generar ni qué marcas temporales debe usar para el `PiT` *join*.
# MAGIC
# MAGIC La *spine* debe contener obligatoriamente:
# MAGIC
# MAGIC * La clave de unión con las tablas de características: en nuestro caso, `policy_id`.
# MAGIC * La marca temporal del evento: `claim_timestamp`. Esta columna es la que se pasa como `timestamp_lookup_key` en los `FeatureLookup` y define el `AS OF` de cada cruce.
# MAGIC * La etiqueta de supervisión: `is_fraud`. Esta columna es la variable objetivo que el modelo deberá aprender a predecir.
# MAGIC
# MAGIC Se muestra el esquema y una muestra de la *spine* para verificar que tiene la estructura esperada antes de lanzar el cruce.
# MAGIC
# MAGIC > **Nota sobre la *spine* seleccionada**: se usa `fraud_spine` (siniestros con `is_fraud IS NOT NULL`) y no `fraud_inference_spine`. Ambas tablas comparten esquema por diseño de la capa `Gold`, pero `fraud_spine` contiene solo los registros etiquetados, que son los que el modelo puede usar para aprender. Los registros sin etiqueta se reservan en `fraud_inference_spine` para la fase de inferencia en producción.

# COMMAND ----------

spine_df = spark.table(fraud_spine_table)

print(f"Spine rows: {spine_df.count():,}")
print(f"Spine columns: {len(spine_df.columns)}")
spine_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 3. Definición de los `FeatureLookup`
# MAGIC
# MAGIC Un `FeatureLookup` es el objeto que le indica a `create_training_set` cómo enriquecer cada fila de la *spine* con características procedentes de una tabla del `Feature Store`. Por cada tabla de características que queramos incorporar, se crea un `FeatureLookup` independiente.
# MAGIC
# MAGIC Cada `FeatureLookup` tiene cuatro parámetros fundamentales:
# MAGIC
# MAGIC * **`table_name`**: nombre completamente cualificado de la tabla de características en `Unity Catalog`.
# MAGIC * **`feature_names`**: lista de columnas de esa tabla que se quieren añadir al conjunto de datos de entrenamiento. Si se omite este parámetro, se incorporan todas las columnas de la tabla.
# MAGIC * **`lookup_key`**: columna o lista de columnas que se usa como clave de unión primaria entre la *spine* y la tabla de características. En nuestro caso, `policy_id`.
# MAGIC * **`timestamp_lookup_key`**: columna de la *spine* que actúa como referencia temporal. Aquí es donde se ejecuta el `PiT` *join*: se buscará, para cada fila de la *spine*, el registro más reciente de la tabla de características cuyo *timestamp* sea **anterior o igual** al valor de esta columna. Esto garantiza el comportamiento `AS OF` y previene cualquier fuga de datos del futuro (*data leakage*).
# MAGIC
# MAGIC > **Nota arquitectónica clave**: no es necesario indicar a `FeatureLookup` cómo se llama la columna temporal en `customer_profile` (es decir, `__START_AT`). El `Feature Store` lo sabe automáticamente porque esa columna se registró con el modificador `TIMESERIES` en la clave primaria al crear la tabla en la capa `Gold`. El resultado es el comportamiento `AS OF` correcto: para cada siniestro, se recupera la versión del perfil del tomador que era válida exactamente en el instante del evento.
# MAGIC
# MAGIC > **Nota sobre nulos esperados en `customer_aggregations`**: el 76.6% de los tomadores tienen un único siniestro en todo el histórico de 10 años, por lo que las columnas de ventana temporal (`num_claims_*`, `total_amount_*`, etc.) serán `NULL` para la mayoría de filas. Esto es un resultado correcto y esperado del `PiT` *join*: el `Feature Store` no fabrica historial inexistente. La imputación de estos nulos se delega al *pipeline* de preprocesado en la libreta de entrenamiento, donde se aplicará la estrategia documentada en `07_Utils.py` (imputación a 0 para conteos, imputación a la mediana para ratios y promedios).

# COMMAND ----------

entity_key = "policy_id"       # Shared join key: links the claim to the policyholder in both tables
timestamp_key = "claim_timestamp"  # Spine timestamp column for the AS OF join

# Static or slowly-changing policyholder and vehicle profile features (SCD Type 2)
profile_feature_names = [
    # Policyholder demographics
    "policyholder_age",
    "age_group",
    "gender",
    "region",
    "region_type",
    "occupation",

    # Policy details
    "coverage_type",
    "payment_frequency",
    "annual_premium_eur",
    "bonus_malus_years",
    "policy_tenure_days",
    "premium_per_day",

    # Behaviour flags
    "has_telematics",
    "multi_policy",

    # Vehicle attributes
    "vehicle_make",
    "vehicle_type",
    "vehicle_year",
    "is_electric",
    "annual_mileage_km",
    "vehicle_age",
    "vehicle_age_group",
    "vehicle_value_eur",

    # Derived risk indicators
    "high_mileage_flag",
    "risk_score_static",
]

profile_lookup = FeatureLookup(
    table_name = customer_profile_table,
    feature_names = profile_feature_names,
    lookup_key = entity_key,
    timestamp_lookup_key = timestamp_key
)

# Behavioural aggregations over rolling windows
# NULL on empty windows is expected and intentional (see note above).
aggregation_feature_names = [
    # 1-hour window (very short-term velocity)
    "num_claims_1h",
    "total_amount_1h",
    "avg_amount_1h",
    "num_telematics_1h",

    # 24-hour window (intra-day behaviour)
    "num_claims_24h",
    "total_amount_24h",
    "avg_amount_24h",
    "num_telematics_24h",

    # 7-day window (weekly pattern)
    "num_claims_7d",
    "total_amount_7d",
    "avg_amount_7d",
    "num_telematics_7d",
    "num_fraud_confirmed_7d",

    # 30-day window (monthly baseline)
    "num_claims_30d",
    "total_amount_30d",
    "avg_amount_30d",
    "num_telematics_30d",
    "num_fraud_confirmed_30d",
    "num_unique_shops_30d",

    # Cross-window ratio (outlier detector)
    # NULL when no claims in the 24-hour window; imputed to median in the ML pipeline.
    "amount_ratio_24h_vs_30d",
    "fraud_rate_30d",
]

aggregations_lookup = FeatureLookup(
    table_name = customer_aggregations_table,
    feature_names = aggregation_feature_names,
    lookup_key = entity_key,
    timestamp_lookup_key = timestamp_key
)

# Final list passed to create_training_set
feature_lookups = [profile_lookup, aggregations_lookup]

print(f"Profile features: {len(profile_feature_names)}")
print(f"Aggregation features: {len(aggregation_feature_names)}")
print(f"Total feature columns: {len(profile_feature_names) + len(aggregation_feature_names)}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 4. Creación del conjunto de datos de entrenamiento con `create_training_set`
# MAGIC
# MAGIC `fe.create_training_set` es la función central de esta libreta. Recibe la *spine*, la lista de `FeatureLookup` y el nombre de la columna que contiene la etiqueta de supervisión, y devuelve un objeto `TrainingSet`.
# MAGIC
# MAGIC El `TrainingSet` es un objeto lógico: en este punto todavía no se ha ejecutado ningún cálculo. La `API` construye internamente el plan de ejecución del `PiT` *join*, pero la materialización real en un `DataFrame` de `Spark` no ocurre hasta que se llama a `.load_df()` en la siguiente celda.
# MAGIC
# MAGIC El parámetro `label` le indica a `create_training_set` cuál es la columna objetivo.
# MAGIC
# MAGIC El parámetro `exclude_columns` elimina columnas que no deben formar parte del conjunto de entrenamiento. Se excluyen:
# MAGIC
# MAGIC * `label_available_date`: fecha en la que la etiqueta de fraude quedó confirmada por el departamento de peritaje. No está disponible en el momento de la predicción (un siniestro nuevo aún no ha sido auditado) y su inclusión generaría *target leakage*.
# MAGIC * `gold_spine_timestamp`, `gold_agg_timestamp`, `gold_profile_timestamp`: marcas de auditoría del *pipeline* medallón, sin valor predictivo.
# MAGIC * `__START_AT`, `__END_AT`, `is_current`: columnas de control `SCD Type 2` de `customer_profile`, incorporadas automáticamente por el `Feature Store` durante el *join* y que no deben entrar en el vector de características.
# MAGIC * `year`, `month`: columnas de partición de la capa `Gold`, redundantes con `claim_timestamp`.

# COMMAND ----------

label = "is_fraud"

exclude_columns = [
    "label_available_date",
    "gold_spine_timestamp",
    "gold_agg_timestamp",
    "gold_profile_timestamp",
    "__START_AT",
    "__END_AT",
    "is_current",
    "year",
    "month",
]

# Build the training dataset logical plan. The following line does not trigger
# any computation. Instead, it returns an object that encodes the spine dataset
# to start from, the feature lookups to join along with their point-in-time
# semantics, the column acting as the label, and the specific columns to drop
# before materialization.
training_dataset = fe.create_training_set(
    df = spine_df,
    feature_lookups = feature_lookups,
    label = label,
    exclude_columns = exclude_columns
)

print("Training dataset logical plan created, but no data materialized yet.")
print(f"Label column: {label}")
print(f"Excluded columns: {exclude_columns}")

# COMMAND ----------

# Materialize the training dataset.
# This is the step that actually executes the PiT joins across the cluster.
training_df = training_dataset.load_df()

# Verify the result: total rows and final column set
print(f"Training dataset rows: {training_df.count():,}")
print(f"Training dataset columns: {len(training_df.columns)}")
training_df.printSchema()

# COMMAND ----------

# Quick visual inspection of the enriched dataset
training_df.limit(5).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 5. Validación de calidad antes de guardar
# MAGIC
# MAGIC Antes de persistir el conjunto de entrenamiento en `Delta`, se realizan tres comprobaciones de calidad sobre el `DataFrame` materializado:
# MAGIC
# MAGIC * **Nulos en características**: se contabilizan los valores faltantes por columna. Los nulos en columnas de ventana temporal (`num_claims_*`, `total_amount_*`, `avg_amount_*`, `amount_ratio_*`, `fraud_rate_30d`) son esperados dado que el 76.6% de tomadores carece de historial previo. Los nulos en columnas de perfil (`policyholder_age`, `gender`, etc.) indican casos en los que el `PiT` *join* no encontró un registro de `customer_profile` válido para esa fecha, lo que puede ocurrir en siniestros muy antiguos o con huecos en el historial `SCD`.
# MAGIC * **Consistencia del volumen**: se verifica que el número de filas del `DataFrame` enriquecido coincida exactamente con el de la *spine* original. Una discrepancia indicaría un producto cartesiano (más filas) o una pérdida de registros en el cruce (menos filas), ambos sintomáticos de un error en la configuración de las claves de *join*.
# MAGIC * **Balance de clases**: se analiza la distribución de `is_fraud` para cuantificar el desequilibrio real del conjunto. El valor esperado, basado en la distribución conocida de `fraud_spine`, es aproximadamente el 7.9% de fraude sobre los registros etiquetados.

# COMMAND ----------

feature_columns = [
    column for column in training_df.columns
    if column not in ["claim_id", "policy_id", "claim_timestamp", "is_fraud"]
]

# COMMAND ----------

# Check 1: null count per feature column
null_counts_row = training_df.select([
    count(when(col(column).isNull(), column)).alias(column) for column in feature_columns
]).collect()[0]

for column in feature_columns:
    null_val = null_counts_row[column]
    pct = 100.0 * null_val / training_df.count() if training_df.count() > 0 else 0.0
    print(f"{column}: {null_val:,} nulls ({pct:.1f}%)")

# COMMAND ----------

# Check 2: row count consistency
spine_count = spine_df.count()
training_count = training_df.count()

print(f"Spine dataset rows: {spine_count:,}")
print(f"Training dataset rows: {training_count:,}")

if spine_count == training_count:
    print("[OK] Row count matches the spine. No duplication or data loss detected.")
else:
    diff = training_count - spine_count
    print(f"[!!] Row count mismatch: delta = {diff:+,}. Investigate the join keys.")

# COMMAND ----------

# Check 3: class balance
print("Class balance:")
class_balance_rows = (
    training_df.groupBy("is_fraud")
               .count()
               .withColumn(
                   "pct", round(col("count") / training_count * 100, 2)
               )
               .orderBy("is_fraud")
               .collect()
)

for row in class_balance_rows:
    value = "NULL" if row["is_fraud"] is None else row["is_fraud"]
    print(f"Label {value}: {row['count']:,} rows ({row['pct']}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 6. Persistencia en `Delta` y registro de metadatos en `Unity Catalog`
# MAGIC
# MAGIC Una vez validado el `DataFrame`, y tras **filtrar los siniestros sin etiqueta** (`is_fraud IS NULL`, que corresponden a partes aún en proceso de peritaje), se escribe en `Unity Catalog` como tabla `Delta` estática. El modo `overwrite` reemplaza el contenido de la tabla en cada ejecución, pero **no destruye el historial**: `Delta Lake` conserva todas las versiones anteriores de forma automática.
# MAGIC
# MAGIC Para garantizar trazabilidad inequívoca entre datos y modelo se persisten cuatro metadatos como propiedades de la tabla:
# MAGIC
# MAGIC * **`ml.delta_semantic_version`**: contador de negocio incrementado automáticamente. `0` es el conjunto inicial, `1` el primero de producción, etc. Es el valor que la libreta de entrenamiento registrará junto con los hiperparámetros en `MLflow`.
# MAGIC * **`ml.delta_physical_version`**: versión interna de `Delta` en el momento exacto del `WRITE`, capturada **antes** de que las operaciones `SET TBLPROPERTIES` y `COMMENT` la incrementen. Se usa para reproducir el *snapshot* exacto de datos mediante *time travel*.
# MAGIC * **`ml.data_max_date`**: fecha máxima de `claim_timestamp` en el conjunto actual. En el siguiente ciclo de reentrenamiento se convierte en `ml.data_previous_max_date` para anclar automáticamente la ventana de validación.
# MAGIC * **`ml.data_previous_max_date`**: fecha máxima del ciclo anterior, capturada antes de sobreescribir. Define el corte del *test window* en la libreta de entrenamiento: todo lo que hay entre esta fecha y `ml.data_max_date` constituye el conjunto de prueba del siguiente ciclo.

# COMMAND ----------

def _get_next_semantic_version(table_name):
    """
    Read the current `ml.delta_semantic_version` from the table properties and
    return the next semantic version. Returns `0` if the table does not exist yet
    or the property has not been set.
    """
    if not spark.catalog.tableExists(table_name):
        return 0
    properties_df = spark.sql(f"SHOW TBLPROPERTIES {table_name}")
    version_row = properties_df.filter("key = 'ml.delta_semantic_version'").first()
    return int(version_row["value"]) + 1 if version_row else 0


def _get_current_max_date(table_name, date_column):
    """
    Read `ml.data_max_date` from the current table properties and return it as
    a string. Returns `None` if the table does not exist yet or the property has
    not been set. This value becomes `ml.data_previous_max_date` in the next cycle.
    """
    if not spark.catalog.tableExists(table_name):
        return None
    properties_df = spark.sql(f"SHOW TBLPROPERTIES {table_name}")
    max_date_row = properties_df.filter("key = 'ml.data_max_date'").first()
    return max_date_row["value"] if max_date_row else None

# COMMAND ----------

print(spark.catalog.currentCatalog())
print(spark.catalog.currentDatabase())

# COMMAND ----------

spark.sql("SHOW TABLES IN workspace.auto_insurance_fraud").show(truncate=False)

# COMMAND ----------

display(spark.sql("SHOW TABLES IN workspace.auto_insurance_fraud"))

# COMMAND ----------

# Filter out claims that lack a supervision label.
# is_fraud IS NULL means the claim is still under investigation by the claims
# department and has not been audited yet. A supervised learning algorithm
# cannot learn from unlabelled data.
# Note: these unlabelled records are intentionally preserved in fraud_inference_spine
# for use during production inference, following the LEFT JOIN design of the Gold layer.
clean_training_df = training_df.filter("is_fraud IS NOT NULL")
clean_training_count = clean_training_df.count()

# Resolve the next semantic version and capture the current maximum date
# before writing so both values reflect the previous cycle.
delta_semantic_version = _get_next_semantic_version(fraud_training_dataset_table)
data_previous_max_date = _get_current_max_date(fraud_training_dataset_table, "claim_timestamp")

# Persist the enriched training set as a static Delta table.
# Using overwrite so the table always reflects the latest generation run.
# Delta Lake automatically retains all previous versions for time travel.
(
    clean_training_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("delta.enableChangeDataFeed", "true")
    .saveAsTable(fraud_training_dataset_table)
)

# Capture the physical Delta version immediately after the write and before
# any ALTER TABLE increments it. This is the version used for time travel in
# the training notebook to guarantee the read points to the data snapshot and
# not to a metadata operation.
delta_physical_version = int(
    spark.sql(f"DESCRIBE HISTORY {fraud_training_dataset_table}")
    .select("version")
    .first()[0]
)

# Capture the max date of the new data for use in the next cycle
data_max_date = (
    clean_training_df
    .agg(max(col("claim_timestamp")).alias("max_date"))
    .collect()[0]["max_date"]
    .strftime("%Y-%m-%d")
)

print(f"Training dataset saved to: {fraud_training_dataset_table}")
print(f"Semantic version: {delta_semantic_version}")
print(f"Physical version: {delta_physical_version}")
print(f"Data maximum date: {data_max_date}")
print(f"Data previous maximum date: {data_previous_max_date}")

# COMMAND ----------

generated_at = datetime.now(timezone.utc).isoformat()

# SET TAGS: surface metadata in the Catalog Explorer for human inspection.
# Tags belong to the data, not to a training run, and are visible
# in the Unity Catalog UI but cannot be read via SHOW TBLPROPERTIES.
spark.sql(f"""
    ALTER TABLE {fraud_training_dataset_table}
    SET TAGS (
        'delta_semantic_version' = '{delta_semantic_version}',
        'delta_physical_version' = '{delta_physical_version}',
        'data_max_date' = '{data_max_date}',
        'data_previous_max_date' = '{data_previous_max_date}',
        'spine_table' = '{fraud_spine_table}',
        'feature_table_1' = '{customer_profile_table}',
        'feature_table_2' = '{customer_aggregations_table}',
        'label_col' = 'is_fraud',
        'num_rows' = '{clean_training_count}',
        'num_features' = '{len(feature_columns)}',
        'generated_at' = '{generated_at}'
    )
""")

# SET TBLPROPERTIES: persist the same metadata as table properties so that
# the training notebook can resolve the delta version programmatically
# via SHOW TBLPROPERTIES to keep a clean data → model traceability chain.
# The "ml." prefix avoids collisions with internal Delta properties.
spark.sql(f"""
    ALTER TABLE {fraud_training_dataset_table}
    SET TBLPROPERTIES (
        'ml.delta_semantic_version' = '{delta_semantic_version}',
        'ml.delta_physical_version' = '{delta_physical_version}',
        'ml.data_max_date' = '{data_max_date}',
        'ml.data_previous_max_date' = '{data_previous_max_date}',
        'ml.spine_table' = '{fraud_spine_table}',
        'ml.feature_table_1' = '{customer_profile_table}',
        'ml.feature_table_2' = '{customer_aggregations_table}',
        'ml.label_col' = 'is_fraud',
        'ml.num_rows' = '{clean_training_count}',
        'ml.num_features' = '{len(feature_columns)}',
        'ml.generated_at' = '{generated_at}'
    )
""")

# Add a human-readable description to the table
table_description = (
    "This managed table acts as the static training dataset for the auto insurance "
    "fraud detection model. It is a point-in-time (PiT) snapshot joining fraud_spine "
    "with policyholder profiles (customer_profile, SCD Type 2) and rolling-window "
    "behavioural aggregations (customer_aggregations), pre-filtered to exclude "
    "unlabelled claims still under investigation."
)
spark.sql(f"COMMENT ON TABLE {fraud_training_dataset_table} IS '{table_description}'")

print(f"Unity Catalog metadata set on: {fraud_training_dataset_table}")
print(f"Semantic version: {delta_semantic_version}")
print(f"Physical version: {delta_physical_version}")
print(f"Data maximum date: {data_max_date}")
print(f"Data previous maximum date: {data_previous_max_date}")
print(f"Number of rows: {clean_training_count:,}")
print(f"Number of features: {len(feature_columns)}")
print(f"Generated at: {generated_at}")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 7. Verificación del historial de versiones `Delta`
# MAGIC
# MAGIC La última sección muestra el historial de operaciones de la tabla recién creada y verifica que el *time travel* apunta al *snapshot* de datos correcto.
# MAGIC
# MAGIC Cada vez que se sobrescriben los datos o se modifican los metadatos, `Delta` registra internamente una nueva versión numérica incremental. El historial refleja la secuencia exacta de operaciones de esta ejecución:
# MAGIC
# MAGIC * **Versión física `N`**: `WRITE`; los datos reales, la única versión relevante para el modelo.
# MAGIC * **Versión física `N + 1`**: `SET TBLPROPERTIES`; los metadatos `ml.*` con los tags.
# MAGIC * **Versión física `N + 2`**: `SET TBLPROPERTIES`; las propiedades de tabla.
# MAGIC * **Versión física `N + 3`**: `SET TBLPROPERTIES`; el comentario de la tabla.
# MAGIC
# MAGIC Por este motivo, `ml.delta_physical_version` se captura entre el `WRITE` y el primer `ALTER TABLE`, garantizando que siempre apunta a los datos y nunca a una operación de metadatos.

# COMMAND ----------

# Retrieve and display the internal version history.
history_df = spark.sql(f"DESCRIBE HISTORY {fraud_training_dataset_table}")
history_rows = history_df.collect()

for row in history_rows:
    for col_name in history_df.columns:
        val = row[col_name]
        if val is not None and val != {} and val != "":
            print(f"{col_name}: {val}")
    print()

# COMMAND ----------

try:
    properties_df = spark.sql(f"SHOW TBLPROPERTIES {fraud_training_dataset_table}")

    semantic_version_row = properties_df.filter("key = 'ml.delta_semantic_version'").first()
    physical_version_row = properties_df.filter("key = 'ml.delta_physical_version'").first()
    max_date_row = properties_df.filter("key = 'ml.data_max_date'").first()
    previous_max_date_row = properties_df.filter("key = 'ml.data_previous_max_date'").first()

    delta_semantic_version = int(semantic_version_row["value"]) if semantic_version_row else None
    delta_physical_version = int(physical_version_row["value"]) if physical_version_row else None
    data_max_date = max_date_row["value"] if max_date_row else None
    data_previous_max_date = previous_max_date_row["value"] if previous_max_date_row else None

    print(f"Semantic version: {delta_semantic_version}")
    print(f"Physical version: {delta_physical_version}")
    print(f"Data maximum date: {data_max_date}")
    print(f"Previous maximum date: {data_previous_max_date}")

    # Time travel: load the exact snapshot of data, which points to the exact
    # write snapshot regardless of subsequent ALTER TABLE operations.
    df_past = (
        spark.read
             .format("delta")
             .option("versionAsOf", delta_physical_version)
             .table(fraud_training_dataset_table)
    )
    print(f"Rows loaded via time travel: {df_past.count():,}")

except Exception as e:
    print(f"Failed to retrieve the version or load the data. Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 8. Conclusiones y siguientes pasos
# MAGIC
# MAGIC ### ¿Qué hemos visto?
# MAGIC
# MAGIC En esta libreta hemos construido el conjunto de datos de entrenamiento usando la `API` del `Feature Store` de `Databricks`:
# MAGIC
# MAGIC 1. La *spine* (`fraud_spine`) define qué filas componen el conjunto de datos y en qué instante temporal se evalúa cada cruce. Contiene únicamente los siniestros etiquetados (`is_fraud IS NOT NULL`), que son los que el modelo puede usar para aprender. Los siniestros aún bajo investigación se preservan en `fraud_inference_spine` para la fase de inferencia.
# MAGIC 2. Los objetos `FeatureLookup` declaran cómo enriquecer cada fila de la *spine*. El parámetro `timestamp_lookup_key` activa el `PiT` *join* con comportamiento `AS OF`: para `customer_profile` (SCD Type 2), se recupera la versión del perfil del tomador válida exactamente en el instante del siniestro; para `customer_aggregations`, se recuperan las métricas de ventana calculadas antes de ese instante. Esto elimina por diseño cualquier fuga de datos del futuro.
# MAGIC 3. Los nulos en columnas de ventana temporal son esperados: el 76.6% de tomadores no tiene historial previo de siniestros, por lo que sus ventanas están vacías. La imputación se delega al *pipeline* de preprocesado en `07_Utils.py` (0 para conteos, mediana para ratios y promedios).
# MAGIC 4. Los metadatos se registran en dos lugares complementarios: como `TAGS` (visibles en el `Catalog Explorer`) y como `TBLPROPERTIES` (legibles programáticamente). La separación `ml.delta_semantic_version` (referencia de negocio para `MLflow`) vs. `ml.delta_physical_version` (puntero exacto para *time travel*) garantiza que cualquier ciclo de entrenamiento sea reproducible meses después.
# MAGIC
# MAGIC ### ¿Cuándo volver a ejecutar esta libreta?
# MAGIC
# MAGIC * **Primera vez**: para generar el conjunto de entrenamiento inicial y lanzar el primer ciclo de experimentación.
# MAGIC * **Cuando el sistema de monitorización detecte *drift* significativo**: el *job* de reentrenamiento lanzará esta libreta con datos más recientes y sobrescribirá la tabla. `Delta Lake` conservará la versión anterior en su historial.
# MAGIC * **Cuando se amplíe el conjunto de características**: si se añaden nuevas tablas a la capa `Gold`, habrá que actualizar los objetos `FeatureLookup` y regenerar el conjunto de datos.
# MAGIC * **Nunca de forma manual en producción**: esta libreta está diseñada para ejecutarse como tarea dentro de un *job* automatizado. La ejecución manual solo tiene sentido durante el desarrollo.
# MAGIC
# MAGIC ### ¿Qué sigue?
# MAGIC
# MAGIC Con el conjunto de entrenamiento disponible en `fraud_training_dataset`, las dos líneas de trabajo que siguen pueden avanzar en paralelo:
# MAGIC
# MAGIC 1. **Fase de modelado**: las libretas de experimentación (`07_MLflow_Experimentation`) consumirán directamente la tabla estática. Se consultará `ml.delta_physical_version` para *time travel* y se registrará `ml.delta_semantic_version` en `MLflow` junto con los hiperparámetros y métricas, garantizando la trazabilidad exacta entre el *snapshot* de datos y cada modelo entrenado.
# MAGIC
# MAGIC 2. **Configuración del *baseline* de monitorización**: `fraud_training_dataset` se usará como perfil de referencia para detectar *data drift* en producción. A medida que el modelo opere, las características evaluadas en cada nuevo siniestro (leídas desde `fraud_inference_spine`) se compararán estadísticamente con este *baseline*. Si el *drift* supera el umbral configurado, el sistema generará una alerta para evaluar si procede regenerar el conjunto de datos y lanzar un nuevo ciclo de reentrenamiento.