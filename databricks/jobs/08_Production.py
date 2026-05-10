# Databricks notebook source
# MAGIC %md
# MAGIC # Evaluación en producción y promoción del modelo
# MAGIC
# MAGIC **Autor**: Juan Carlos Alfaro Jiménez
# MAGIC
# MAGIC > _Adaptado por Raúl Jiménez para el caso de uso de "Aseguradora de vehículos"_
# MAGIC
# MAGIC Esta libreta cubre la segunda fase del ciclo `MLOps` profesional: **evaluación del modelo candidato sobre el conjunto de prueba y decisión de promoción**.
# MAGIC
# MAGIC El flujo de aliases que gobierna este proceso es el siguiente:
# MAGIC
# MAGIC | Alias | Significado | ¿Quién lo asigna? |
# MAGIC |---|---|---|
# MAGIC | **`candidate`** | Mejor modelo salido del *grid search*, pendiente de evaluación en prueba | `07_MLflow_Experimentation` |
# MAGIC | **`challenger`** | Candidato reentrenado sobre entrenamiento y validación en evaluación activa, compitiendo por reemplazar al `champion` | Esta libreta |
# MAGIC | **`champion`** | Modelo reentrenado sobre el histórico completo y aprobado para producción | Esta libreta |
# MAGIC | **`retired`** | Antiguo `champion` desplazado por un modelo mejor | Esta libreta |
# MAGIC | **`rejected`** | `challenger` que no superó al `champion` en prueba | Esta libreta |
# MAGIC
# MAGIC ### ¿Por qué evaluar sobre el conjunto de prueba aquí y no antes?
# MAGIC
# MAGIC El conjunto de prueba es un recurso **de un solo uso**: contiene las transacciones más recientes del conjunto de datos, nunca vistas durante el entrenamiento ni la validación. Cualquier decisión tomada observando los datos de prueba (aunque sea indirectamente) introduciría un sesgo de selección. Por este motivo, el conjunto de prueba solo se usa en esta libreta, que se ejecuta únicamente cuando ya existe un candidato validado y listo para comparar.
# MAGIC
# MAGIC Para garantizar una comparación justa, el `challenger` se retrenará previamente sobre entrenamiento y validación (los mismos datos que el `champion` vio en su ciclo), antes de ser evaluado. Una vez tomada la decisión de promoción, el modelo ganador se retrenará sobre el histórico completo (entrenamiento, validación y prueba) antes de ser registrado como `champion`, ya que la evaluación ha cumplido su función y el conjunto de prueba deja de estar reservado.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Importaciones y configuración

# COMMAND ----------

exec(open("07_Utils.py").read(), globals())

# COMMAND ----------

exec(open("08_Utils.py").read(), globals())

# COMMAND ----------

from datetime import datetime, timezone

from pathlib import Path

import mlflow
import mlflow.spark
from mlflow import MlflowClient

from pyspark.ml.functions import vector_to_array

from pyspark.sql import functions as F

# COMMAND ----------

notebook_path_raw = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook = Path(notebook_path_raw).name

print(f"Project: {project}, team: {team}, environment: {environment}")
print(f"Notebook: {notebook}, user: {current_user}")
print(f"Experiment path: {mlflow_experiment_path}")
print(f"Unity Catalog model name: {uc_model_name}")
print(f"Training notebook: {training_notebook_path} (timeout: {training_timeout_seconds} seconds)")
print(f"Evaluation notebook: {evaluation_notebook_path} (timeout: {evaluation_timeout_seconds} seconds)")
print(f"Databricks file system temporary directory: {os.environ['MLFLOW_DFS_TMP']}")
print(f"Monitoring baseline table: {baseline_table_name}")

# COMMAND ----------

setup_mlflow_warnings()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2. Configuración de `MLflow`

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### `Tracking Server` y `Model Registry`

# COMMAND ----------

tracking_uri = "databricks"
mlflow.set_tracking_uri(tracking_uri)

registry_uri = "databricks-uc"
mlflow.set_registry_uri(registry_uri)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Activación del experimento y cliente

# COMMAND ----------

mlflow.set_experiment(mlflow_experiment_path)

client = MlflowClient()
experiment = mlflow.get_experiment_by_name(mlflow_experiment_path)
experiment_id = experiment.experiment_id

print(f"Tracking URI: {tracking_uri}")
print(f"Registry URI: {registry_uri}")
print(f"Experiment identifier: {experiment_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 3. Registro de conjuntos de datos como entidades `MLflow`
# MAGIC
# MAGIC Al igual que en la fase de experimentación, se envuelven los tres conjuntos en objetos `mlflow.data.SparkDataset` para documentar el linaje completo en la pestaña `Inputs` de la ejecución de producción.

# COMMAND ----------

mlflow_train_ds = mlflow.data.from_spark(
    train_df,
    table_name = training_table,
    name = "train"
)
mlflow_validation_ds = mlflow.data.from_spark(
    validation_df,
    table_name = training_table,
    name = "validation"
)
mlflow_test_ds = mlflow.data.from_spark(
    test_df,
    table_name = training_table,
    name = "test"
)

print(f"Train rows: {train_df.count():,}")
print(f"Validation rows: {validation_df.count():,}")
print(f"Test rows: {test_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 4. Promoción de `candidate` a `challenger`
# MAGIC
# MAGIC El primer paso operativo es promocionar el modelo candidato a `challenger`. Se recuperan de las etiquetas de la versión los hiperparámetros exactos que seleccionó el *grid search*: son los que se usarán en los dos reentrenamientos posteriores, garantizando que ninguna decisión nueva afecte al modelo que competirá en producción.
# MAGIC
# MAGIC Una vez extraídos los metadatos, se intercambia el alias `candidate` por `challenger` en `Unity Catalog`, manteniendo el registro limpio y evitando estados ambiguos.

# COMMAND ----------

candidate_version = client.get_model_version_by_alias(name = uc_model_name, alias = "candidate")
candidate_metadata = extract_candidate_metadata(candidate_version)
challenger_hyperparams = build_challenger_hyperparams(candidate_metadata)

print(f"Candidate version {candidate_metadata['version_number']} promoted to 'challenger'")
print(f"Challenger run identifier: {candidate_metadata['candidate_run_id']}")
print(f"Validation threshold: {candidate_metadata['best_threshold_val']}")
print(f"Regularization parameter: {candidate_metadata['reg_param']}")
print(f"Elastic net parameter: {candidate_metadata['elastic_net_param']}")

# COMMAND ----------

promote_candidate_to_challenger(client, uc_model_name, candidate_metadata["version_number"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 5. Comprobación del modelo `champion` en producción
# MAGIC
# MAGIC El patrón `champion` contra `challenger` es el estándar de la industria para realizar despliegues seguros. Consiste en no sustituir nunca un modelo en producción (`champion`) a menos que el nuevo modelo (`challenger`) demuestre empíricamente un rendimiento superior bajo las mismas condiciones de evaluación.
# MAGIC
# MAGIC `get_champion_metadata` consulta `Unity Catalog` e intenta recuperar el modelo que actualmente ostenta el alias `champion`. Si lo encuentra, devuelve sus metadatos, en particular su `run_id` y su **umbral de validación original**: es crítico usar el umbral histórico del `champion` para que la comparación en la fase de pruebas sea justa. Si el alias no existe (lo que ocurrirá la primera vez que se ejecute este proyecto en un entorno nuevo) devuelve `None`, lo que permite al pipeline saltarse la fase de comparación y coronar al `challenger` directamente.

# COMMAND ----------

champion_metadata = get_champion_metadata(client, uc_model_name)
champion_exists = champion_metadata is not None

if champion_exists:
    print(f"Champion found: version {champion_metadata['version_number']}")
    print(f"Champion production run identifier: {champion_metadata['production_run_id']}")
else:
    print("No champion found. Challenger will be promoted directly.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 6. Reentrenamiento del `challenger` sobre el conjunto de entrenamiento y validación
# MAGIC
# MAGIC Antes de evaluar el `challenger` sobre el conjunto de prueba, lo reentrenamos usando **todos los datos que no son de prueba**; es decir, el conjunto de entrenamiento más el de validación unidos. **Este paso es metodológicamente necesario porque el modelo que compite en prueba debe haber aprendido de todos los datos disponibles antes del corte temporal de prueba.** El modelo que salió del *grid search* solo vio el conjunto de entrenamiento. El conjunto de validación, aunque no se usó para ajustar los pesos, sí influyó indirectamente en la selección de hiperparámetros. Reentrenando con ambos, el modelo aprovecha todo el dato histórico disponible sin contaminar el conjunto de prueba.
# MAGIC
# MAGIC Los hiperparámetros del reentrenamiento son exactamente los mismos que seleccionó el *grid search*; no se toma ninguna decisión nueva. El conjunto de prueba sigue sin tocarse.

# COMMAND ----------

production_note = f"""### Production evaluation run

This run orchestrates the champion-versus-challenger evaluation for the `{uc_model_name}` model.

The challenger (candidate version `{candidate_metadata['version_number']}`) is retrained on the
training and validation datasets, evaluated on the held-out test dataset, and compared against the current
champion. The winning model is then retrained on the full historical dataset before being registered
as the new `champion`.

* **Selection metric**: `{selection_metric}`.
* **Challenger version**: `{candidate_metadata['version_number']}`.
* **Champion version**: `{champion_metadata['version_number'] if champion_exists else 'none (cold start)'}`.
"""

# COMMAND ----------

production_run = mlflow.start_run(run_name = production_run_name)
production_run_id = production_run.info.run_id

try:
    mlflow.set_tags({
        "mlflow.note.content": production_note,
        "project": project,
        "team": team,
        "task": task,
        "algorithm": algorithm_family,
        "run_type": "production_evaluation",
        "notebook": notebook,
        "framework": framework,
        "environment": environment,
        "label_column": label_column,
        "selection_metric": selection_metric,
        "challenger_version": str(candidate_metadata["version_number"]),
        "champion_version": str(champion_metadata["version_number"]) if champion_exists else "none"
    })
    mlflow.log_input(mlflow_train_ds, context = "training")
    mlflow.log_input(mlflow_validation_ds, context = "validation")
    mlflow.log_input(mlflow_test_ds, context = "test")
    train_val_result = run_training_job(
        training_notebook_path,
        training_timeout_seconds,
        challenger_hyperparams,
        "train_val",
        training_max_retries
    )
except Exception:
    mlflow.set_tag("failure_stage", "challenger_training")
    mlflow.end_run(status = "FAILED")
    raise

print(f"Production run identifier: {production_run_id}")
print("Challenger retrained on training and validation successfully.")

# COMMAND ----------

log_pipeline_model(
    train_val_result["model_save_path"],
    train_val_result["input_example_path"],
    train_val_result["output_example_path"],
    "challenger_model"
)
challenger_model_eval_uri = f"runs:/{production_run_id}/challenger_model"
print(f"Challenger model URI: {challenger_model_eval_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 7. Evaluación del `challenger` sobre el conjunto de prueba
# MAGIC
# MAGIC Se invoca `07_Evaluation_Job.ipynb` para evaluar el `challenger` reentrenado sobre entrenamiento y validación contra el conjunto de prueba completo. El modelo ya está registrado en la ejecución de producción abierta en la sección anterior, por lo que el `Evaluation_Job` puede cargarlo mediante su `URI` canónica de `MLflow`.

# COMMAND ----------

try:
    challenger_eval = run_evaluation_job(
        evaluation_notebook_path,
        evaluation_timeout_seconds,
        challenger_model_eval_uri,
        "test",
        candidate_metadata["evaluation_tag"],
        evaluation_max_retries
    )
    challenger_test_metrics = challenger_eval["eval_metrics"]
except Exception:
    mlflow.set_tag("failure_stage", "challenger_evaluation")
    mlflow.end_run(status = "FAILED")
    raise

print(f"Challenger test AUC-PR: {challenger_test_metrics['auc_pr']:.4f}")
print(f"Challenger test AUC-ROC: {challenger_test_metrics['auc_roc']:.4f}")
print(f"Challenger test F1-score: {challenger_test_metrics['f1']:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 8. Evaluación del `champion` sobre el conjunto de prueba
# MAGIC
# MAGIC Si existe un modelo `champion` en producción, se evalúa con el mismo procedimiento y sobre el mismo conjunto de prueba para garantizar una comparación justa. Esta evaluación se omite si el `challenger` es el primer modelo del proyecto.

# COMMAND ----------

if champion_exists:
    try:
        champion_eval = run_evaluation_job(
            evaluation_notebook_path,
            evaluation_timeout_seconds,
            champion_metadata["model_artifact_uri"],
            "test",
            champion_metadata["evaluation_tag"],
            evaluation_max_retries
        )
        champion_test_metrics = champion_eval["eval_metrics"]
        print(f"Champion test AUC-PR: {champion_test_metrics['auc_pr']:.4f}")
        print(f"Champion test AUC-ROC: {champion_test_metrics['auc_roc']:.4f}")
        print(f"Champion test F1-score: {champion_test_metrics['f1']:.4f}")
    except Exception:
        mlflow.set_tag("failure_stage", "champion_evaluation")
        mlflow.end_run(status = "FAILED")
        raise
else:
    champion_test_metrics = None
    print("Skipping champion evaluation. No champion exists.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 9. Decisión de promoción
# MAGIC
# MAGIC Se compara el `test_auc_pr` del `challenger` con el del `champion` actual. El `challenger` debe superar **estrictamente** al `champion` para ser promovido. En caso de empate, el `champion` conserva su posición, siguiendo el principio de que un modelo nuevo debe demostrar una mejora real antes de sustituir al que ya opera en producción.

# COMMAND ----------

challenger_wins, decision_reason = make_promotion_decision(
    challenger_test_metrics,
    champion_test_metrics,
    selection_metric
)

print(f"Decision: {'CHALLENGER PROMOTED' if challenger_wins else 'CHAMPION HOLDS'}")
print(f"Reason: {decision_reason}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 10. Reentrenamiento final y actualización de aliases en `Unity Catalog`
# MAGIC
# MAGIC Si el `challenger` gana, se realiza un reentrenamiento final sobre el histórico completo (entrenamiento, validación y prueba) antes de registrarlo como `champion`. Este paso garantiza que el modelo desplegado haya aprendido de las tácticas de fraude más recientes, incluyendo las del conjunto de prueba que ya ha cumplido su función evaluativa.
# MAGIC
# MAGIC Una vez registrada la nueva versión, se escribe su descripción en `Unity Catalog` con las métricas de prueba y los hiperparámetros clave, de forma que cualquier versión `champion` sea autoexplicativa sin necesidad de consultar el run de `MLflow`. Los cambios de alias se aplican a continuación sobre esta versión, no sobre la del `challenger` evaluado.

# COMMAND ----------

champion_version_description = f"""
### Champion version (alias: `champion`)

Promoted after outperforming the previous champion on the held-out test dataset.

#### Regularization hyperparameters

* `reg_param`: `{candidate_metadata['reg_param']}`
* `elastic_net_param`: `{candidate_metadata['elastic_net_param']}`

#### Test metrics

* **AUC-PR**: `{challenger_test_metrics['auc_pr']:.4f}`
* **AUC-ROC**: `{challenger_test_metrics['auc_roc']:.4f}`
* **F1-score**: `{challenger_test_metrics['f1']:.4f}`

**Decision threshold (from validation)**: `{candidate_metadata['best_threshold_val']}`
**Previous champion version**: `{champion_metadata['version_number'] if champion_exists else 'none (cold start)'}`
"""

# COMMAND ----------

if challenger_wins:
    try:
        full_refit_result = run_training_job(
            training_notebook_path,
            training_timeout_seconds,
            challenger_hyperparams,
            "train_val_test",
            training_max_retries
        )
        log_pipeline_model(
            full_refit_result["model_save_path"],
            full_refit_result["input_example_path"],
            full_refit_result["output_example_path"],
            "champion_model"
        )
        champion_model_uri = f"runs:/{production_run_id}/champion_model"
        uc_model_version_final = mlflow.register_model(model_uri = champion_model_uri, name = uc_model_name)
        final_version_number = uc_model_version_final.version
        client.update_model_version(name = uc_model_name, version = final_version_number, description = champion_version_description)
        print(f"Full refit completed. New version: {final_version_number}")
    except Exception:
        mlflow.set_tag("failure_stage", "full_refit")
        mlflow.end_run(status = "FAILED")
        raise

# COMMAND ----------

try:
    apply_promotion_aliases(
        client = client,
        uc_model_name = uc_model_name,
        challenger_wins = challenger_wins,
        challenger_version_number = candidate_metadata["version_number"],
        final_version_number = final_version_number if challenger_wins else None,
        champion_version_number = champion_metadata["version_number"] if champion_exists else None,
        challenger_test_metrics = challenger_test_metrics,
        challenger_validation_threshold = candidate_metadata["best_threshold_val"],
        challenger_hyperparams = challenger_hyperparams,
        champion_exists = champion_exists,
        production_run_id = production_run_id,
        candidate_run_id = candidate_metadata["candidate_run_id"],
        test_end_date = test_end_date
    )
except Exception:
    mlflow.set_tag("failure_stage", "alias_update")
    mlflow.end_run(status = "FAILED")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 11. Escritura del *baseline* para monitorización
# MAGIC
# MAGIC Cuando el `challenger` gana, se escribe el conjunto de prueba con las predicciones del `champion` como tabla `Delta` en `Unity Catalog`. Esta tabla actúa como *baseline* del monitor de `Databricks Lakehouse Monitoring`, permitiendo comparar tanto la distribución de características como las métricas de rendimiento en producción contra el mismo referente que se usó para la decisión de promoción.
# MAGIC
# MAGIC El conjunto de prueba es el *baseline* correcto por dos razones: es el único conjunto sobre el que existen métricas imparciales (las mismas guardadas como `test_auc_pr` en el *tag* de la versión `champion`), y contiene las transacciones temporalmente más recientes, que son el mejor *proxy* de la distribución real esperada en producción. Las columnas internas de `Spark ML` (`features`, `rawPrediction` y `probability`) se descartan antes de persistir, conservando únicamente las características originales, la etiqueta y las columnas de predicción.

# COMMAND ----------

if challenger_wins:
    try:
        # mismo fillna(0) que en 07_Training_Job y 07_Evaluation_Job
        cols_to_fill = [
            "total_amount_1h", "num_telematics_1h",
            "total_amount_24h", "num_telematics_24h",
            "total_amount_7d", "num_telematics_7d", "num_fraud_confirmed_7d",
            "total_amount_30d", "num_telematics_30d", "num_fraud_confirmed_30d"
        ]
        test_df_clean = test_df.fillna(0, subset=cols_to_fill)
        champion_pipeline = mlflow.spark.load_model(
            f"runs:/{production_run_id}/champion_model"
        )
        (
            champion_pipeline
            .transform(test_df_clean) # aplicar cambio de antes
            .withColumn(
                prob_fraud_column,
                vector_to_array(F.col(probability_column)).getItem(1)
            )
            .withColumn(
                prediction_column,
                F.col(prediction_column).cast("long")
            )
            .withColumn(
                model_version_col,
                F.lit(final_version_number)
            )
            .withColumn(
                inference_timestamp_col,
                F.current_timestamp()
            )
            .select(
                *test_df.columns,
                prediction_column,
                prob_fraud_column,
                model_version_col,
                inference_timestamp_col
            )
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(baseline_table_name)
        )
        print(f"Test baseline written to {baseline_table_name}")
    except Exception:
        mlflow.set_tag("failure_stage", "baseline_write")
        mlflow.end_run(status = "FAILED")
        raise
else:
    print("Champion held. Baseline not updated.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 11.1. Registro de metadatos en `Unity Catalog`
# MAGIC
# MAGIC Al igual que `gold_fraud_training_dataset`, la tabla `gold_fraud_test_baseline` recibe metadatos de trazabilidad como etiquetas visibles en la interfaz de `Unity Catalog` y como propiedades de tabla legibles programáticamente. Esto garantiza que cualquier consulta futura sobre el *baseline* pueda identificar inequívocamente qué modelo generó esas predicciones, en qué ciclo de producción y sobre qué partición temporal de los datos.

# COMMAND ----------

if challenger_wins:
    generated_at = datetime.now(timezone.utc).isoformat()
    baseline_count = (
        spark.table(baseline_table_name)
             .count()
    )
    baseline_physical_version = int(
        spark.sql(f"DESCRIBE HISTORY {baseline_table_name}")
             .select("version")
             .first()[0]
    )
    spark.sql(f"""
        ALTER TABLE {baseline_table_name}
        SET TAGS (
            'production_run_id' = '{production_run_id}',
            'champion_version' = '{final_version_number}',
            'test_auc_pr' = '{challenger_test_metrics["auc_pr"]:.4f}',
            'test_auc_roc' = '{challenger_test_metrics["auc_roc"]:.4f}',
            'test_f1' = '{challenger_test_metrics["f1"]:.4f}',
            'best_threshold_val' = '{candidate_metadata["best_threshold_val"]}',
            'num_rows' = '{baseline_count}',
            'delta_physical_version' = '{baseline_physical_version}',
            'generated_at' = '{generated_at}'
        )
    """)
    spark.sql(f"""
        ALTER TABLE {baseline_table_name}
        SET TBLPROPERTIES (
            'ml.production_run_id' = '{production_run_id}',
            'ml.champion_version' = '{final_version_number}',
            'ml.test_auc_pr' = '{challenger_test_metrics["auc_pr"]:.4f}',
            'ml.test_auc_roc' = '{challenger_test_metrics["auc_roc"]:.4f}',
            'ml.test_f1' = '{challenger_test_metrics["f1"]:.4f}',
            'ml.best_threshold_val' = '{candidate_metadata["best_threshold_val"]}',
            'ml.num_rows' = '{baseline_count}',
            'ml.delta_physical_version' = '{baseline_physical_version}',
            'ml.generated_at' = '{generated_at}'
        )
    """)
    table_description = f"""Static baseline table for `Databricks Lakehouse Monitoring`. \
Contains champion model (version {final_version_number}) predictions on the held-out test dataset. \
Test AUC-PR: {challenger_test_metrics['auc_pr']:.4f}. \
Generated by production run `{production_run_id}`."""
    spark.sql(
        f"COMMENT ON TABLE {baseline_table_name} IS '{table_description}'"
    )
    print(f"Baseline metadata set on: {baseline_table_name}")
    print(f"Champion version: {final_version_number}")
    print(f"Physical version: {baseline_physical_version}")
    print(f"Test AUC-PR: {challenger_test_metrics['auc_pr']:.4f}")
    print(f"Test AUC-ROC: {challenger_test_metrics['auc_roc']:.4f}")
    print(f"Test F1-score: {challenger_test_metrics['f1']:.4f}")
    print(f"Rows: {baseline_count:,}")
    print(f"Generated at: {generated_at}")
else:
    print("Champion held. Baseline metadata not updated.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 12. Cierre del registro en `MLflow`
# MAGIC
# MAGIC Se completan las métricas y artefactos de la ejecución de producción abierta y se cierra formalmente con `mlflow.end_run()`.

# COMMAND ----------

log_production_metrics(
    challenger_metrics = challenger_test_metrics,
    champion_metrics = champion_test_metrics,
    challenger_run_id = candidate_metadata["candidate_run_id"],
    champion_run_id = champion_metadata["production_run_id"] if champion_exists else None,
    challenger_wins = challenger_wins,
    decision_reason = decision_reason
)

log_evaluation_artifacts(challenger_eval, "challenger")
if champion_exists:
    log_evaluation_artifacts(champion_eval, "champion")

mlflow.end_run()

print(f"Production evaluation run: {production_run_id}")
print(f"Decision: {'CHALLENGER PROMOTED TO CHAMPION' if challenger_wins else 'CHAMPION HELD'}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 13. Limpieza de artefactos temporales

# COMMAND ----------

cleanup_temporary_artifacts(uc_volume_path)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 14. Conclusiones y siguientes pasos
# MAGIC
# MAGIC ### ¿Qué hemos visto?
# MAGIC
# MAGIC En esta libreta hemos completado la segunda fase del ciclo `MLOps` profesional:
# MAGIC
# MAGIC 1. **Gestión de aliases**: Hemos implementado un flujo estricto de aliases en `Unity Catalog` (`candidate` → `challenger` → `champion`/`rejected`, antiguo `champion` → `retired`) que documenta inequívocamente el estado de cada versión del modelo en cada momento del ciclo de vida.
# MAGIC 2. **Reentrenamiento justo del `challenger`**: Antes de evaluar en prueba, el `challenger` se retrenó sobre entrenamiento y validación, los mismos datos que el `champion` vio en su ciclo. Esto garantiza que la comparación es simétrica y no penaliza al nuevo modelo por haber entrenado con menos datos.
# MAGIC 3. **Evaluación aislada**: Delegamos la evaluación de cada modelo a `07_Evaluation_Job.ipynb` mediante `dbutils.notebook.run()`, evitando el error de caché de `Spark Connect` y garantizando que cada modelo se evalúa en una sesión completamente limpia.
# MAGIC 4. **Decisión reproducible**: La comparación se basa exclusivamente en `test_auc_pr`, la métrica más robusta para problemas con fuerte desbalance de clases. La decisión queda registrada en `MLflow` junto con las métricas completas de ambos modelos, formando un registro auditable de cada ciclo de producción.
# MAGIC 5. **Conjunto de prueba intocable**: El conjunto de prueba solo se usó aquí, nunca durante la fase de experimentación del `07`, preservando su integridad como estimador imparcial del rendimiento real.
# MAGIC 6. **Reentrenamiento final sobre el histórico completo**: Cuando el `challenger` gana, el modelo registrado como `champion` no es el evaluado en prueba sino uno reentrenado sobre entrenamiento, validación y prueba. Esto garantiza que el sistema en producción haya aprendido de las tácticas de fraude más recientes antes del despliegue, mitigando el *concept drift*.
# MAGIC 7. ***Baseline* para monitorización**: Cuando el `challenger` gana, el conjunto de prueba transformado por el `champion` se persiste en `Unity Catalog` como tabla `Delta` (`gold_fraud_test_baseline`), incluyendo todas las características originales, la etiqueta y las columnas de predicción. Esta tabla actúa como referente del monitor de `Databricks Lakehouse Monitoring`, permitiendo calcular tanto *data drift* de características como métricas de rendimiento en producción, comparadas contra el mismo estándar que determinó la promoción del modelo.
# MAGIC
# MAGIC ### ¿Cuándo volver a ejecutar esta libreta?
# MAGIC
# MAGIC * **Tras cada ciclo de reentrenamiento**: Esta libreta es el paso final del *pipeline* automatizado `05 → 07 → 08`. Se ejecuta siempre que el sistema de monitorización detecte *drift* y genere un nuevo modelo candidato.
# MAGIC * **Nunca de forma manual en producción**: Su ejecución manual debe reservarse para desarrollo y depuración. En producción, es un nodo del *job* de `Databricks`.
# MAGIC
# MAGIC ### ¿Qué sigue?
# MAGIC
# MAGIC Con el alias `champion` asignado a la versión del reentrenamiento completo en `Unity Catalog`, el *job* de `Databricks` que orquesta este *pipeline* detectará el cambio y enviará la notificación correspondiente al equipo responsable para que apruebe la actualización del *endpoint* de *model serving*.