# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Evaluación universal de modelos
# MAGIC
# MAGIC **Autor**: Juan Carlos Alfaro Jiménez
# MAGIC
# MAGIC Esta libreta actúa como un **evaluador agnóstico e independiente** y no contiene ninguna llamada a `MLflow`. Su única responsabilidad es recibir la ruta de un modelo físico ya entrenado (`PipelineModel`), aplicarlo sobre la partición de datos que se le solicite (entrenamiento, validación o prueba), calcular las métricas de rendimiento y generar las figuras de diagnóstico. Finalmente, guarda los artefactos resultantes en un volumen de `Unity Catalog`.
# MAGIC
# MAGIC ### ¿Por qué desacoplar la evaluación de la orquestadora?
# MAGIC
# MAGIC El mismo límite de memoria de `Spark Connect` que nos obligó a aislar el entrenamiento en `07_Training_Job.ipynb` aplica igualmente a la fase de inferencia: cargar múltiples modelos pesados y ejecutar transformaciones distribuidas directamente en la sesión de la libreta orquestadora consumiría el límite estricto de **1 `GB` por sesión**, provocando el fallo `ML_CACHE_SIZE_OVERFLOW_EXCEPTION`.
# MAGIC
# MAGIC La solución es idéntica: cada evaluación se delega a esta libreta secundaria mediante `dbutils.notebook.run()`. Esto levanta una **sesión de `Spark Connect` completamente limpia y nueva**, garantizando que la caché del servidor se libere por completo al terminar la ejecución, sin importar cuántos modelos estemos comparando en nuestro flujo `MLOps`.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. Importaciones y carga de utilidades compartidas
# MAGIC
# MAGIC El *script* `07_Utils.py` actúa como nuestra caja de herramientas central. A diferencia de la libreta de entrenamiento, en esta sesión **sí haremos un uso intensivo de toda la lógica matemática de evaluación y de las funciones de generación de figuras de diagnóstico** (curvas *PR*, curvas *ROC*, matrices de confusión y curvas de calibración) que dejamos allí encapsuladas.
# MAGIC
# MAGIC A continuación, importamos las librerías necesarias **exclusivamente** para la carga del modelo, la inferencia y la serialización de resultados.

# COMMAND ----------

exec(open("07_Utils.py").read(), globals())

# COMMAND ----------

import gc
import json
from pathlib import Path

import mlflow.spark

from sklearn.metrics import classification_report

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2. Recepción de parámetros
# MAGIC
# MAGIC Al igual que en `07_Training_Job.ipynb`, la libreta orquestadora inyecta todos los valores mediante el parámetro `arguments` de `dbutils.notebook.run()`, y todos llegan como cadenas de texto que deben convertirse explícitamente:
# MAGIC
# MAGIC * **`model_artifact_uri`**: `URI` del artefacto del modelo.
# MAGIC * **`evaluation_dataset`**: Partición sobre la que se evaluará el modelo: `train`, `validation` o `test`.
# MAGIC * **`evaluation_tag`**: Identificador único de esta evaluación, usado para nombrar los directorios de artefactos en el volumen.

# COMMAND ----------

dbutils.widgets.text("model_artifact_uri", "")
dbutils.widgets.text("evaluation_dataset", "train")
dbutils.widgets.text("evaluation_tag", "train")

model_artifact_uri = dbutils.widgets.get("model_artifact_uri")
evaluation_dataset = dbutils.widgets.get("evaluation_dataset")
evaluation_tag = dbutils.widgets.get("evaluation_tag")

print(f"Model artifact URI: {model_artifact_uri}")
print(f"Evaluation dataset: {evaluation_dataset}")
print(f"Evaluation tag: {evaluation_tag}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 3. Obtención del conjunto de evaluación
# MAGIC
# MAGIC Gracias a la importación inicial de nuestro *script* de utilidades, las tres particiones temporales del proyecto (`train_df`, `validation_df`, `test_df`) ya se encuentran pre-cargadas en el entorno global de la libreta.
# MAGIC
# MAGIC A diferencia del proceso de entrenamiento (que fusiona particiones para maximizar el aprendizaje), esta libreta actúa como un juez estricto. Utilizando el parámetro `evaluation_dataset`, seleccionará dinámicamente la partición correspondiente a la fase del ciclo de vida en curso:
# MAGIC
# MAGIC * **`train`**: Evalúa sobre el conjunto de entrenamiento. Se utiliza en la fase de experimentación para calcular la brecha de generalización junto con la evaluación sobre validación.
# MAGIC * **`validation`**: Evalúa sobre el conjunto de validación. Es el modo estándar durante el *grid search* para seleccionar el mejor modelo sin contaminar el conjunto de prueba.
# MAGIC * **`test`**: Evalúa sobre el conjunto de prueba. Se reserva exclusivamente para la comparación final entre `challenger` y `champion` en la fase de producción.

# COMMAND ----------

if evaluation_dataset == "train":
    dataset_description = "Evaluating on the strict temporal training partition."
    eval_df = train_df
elif evaluation_dataset == "validation":
    dataset_description = "Evaluating on the validation partition."
    eval_df = validation_df
else:
    dataset_description = "Evaluating on the held-out test partition."
    eval_df = test_df

print(dataset_description)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 4. Carga del modelo y evaluación
# MAGIC
# MAGIC Se carga el `PipelineModel` desde el almacén de artefactos de `MLflow` usando la `URI` recibida por *widget*. A continuación, se aplica sobre la partición seleccionada por `evaluation_dataset` para calcular las métricas de rendimiento correspondientes a esta fase del ciclo de vida.

# COMMAND ----------

pipeline_model = mlflow.spark.load_model(model_artifact_uri)
lr_fitted = pipeline_model.stages[-1]

eval_predictions = pipeline_model.transform(eval_df)
eval_metrics = compute_metrics(eval_predictions)

print(f"AUC-PR: {eval_metrics['auc_pr']:.4f}")
print(f"AUC-ROC: {eval_metrics['auc_roc']:.4f}")
print(f"F1-score: {eval_metrics['f1']:.4f}")
print(f"Recall: {eval_metrics['recall']:.4f}")
print(f"Precision: {eval_metrics['precision']:.4f}")
print(f"Accuracy: {eval_metrics['accuracy']:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 5. Conversión a `pandas` y umbral de decisión óptimo
# MAGIC
# MAGIC Se convierte el resultado de la evaluación a `pandas` para poder calcular el umbral de decisión óptimo mediante `find_best_threshold`. Este umbral es el valor de corte sobre la probabilidad estimada de fraude que maximiza el *F1-score* sobre la partición evaluada.
# MAGIC
# MAGIC En la fase de experimentación, este valor se calcula sobre el conjunto de validación y se registra como referencia para la comparación `champion` contra `challenger`. En la fase de producción, se reporta sobre el conjunto de prueba con carácter meramente informativo (el umbral operativo del modelo es siempre el que se optimizó sobre validación).

# COMMAND ----------

eval_pandas_df = to_pandas_predictions(eval_predictions)

y_eval = eval_pandas_df[label_column].values
p_eval = eval_pandas_df[prob_fraud_column].values
pred_eval = eval_pandas_df[prediction_column].values

best_threshold, best_f1_score = find_best_threshold(y_eval, p_eval)

print(f"Best threshold: {best_threshold:.2f}")
print(f"Best F1-score: {best_f1_score:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 6. Generación y guardado de figuras
# MAGIC
# MAGIC Se generan las figuras de diagnóstico sobre la partición evaluada y se guardan como archivos `.png` en el volumen temporal de `Unity Catalog`.

# COMMAND ----------

eval_tmp_path = str(Path(uc_volume_path) / "evaluations" / evaluation_tag)
figures_local_path = str(Path(eval_tmp_path) / "figures")
dbutils.fs.mkdirs(figures_local_path)

save_diagnostic_figure(
    fig_pr_curve(y_eval, p_eval, eval_metrics["auc_pr"], f"PR — {evaluation_tag} ({evaluation_dataset})"),
    figures_local_path,
    "pr_curve.png"
)
save_diagnostic_figure(
    fig_roc_curve(y_eval, p_eval, eval_metrics["auc_roc"], f"ROC — {evaluation_tag} ({evaluation_dataset})"),
    figures_local_path,
    "roc_curve.png"
)
save_diagnostic_figure(
    fig_confusion_matrix(y_eval, pred_eval, f"Confusion matrix — {evaluation_tag} ({evaluation_dataset})"),
    figures_local_path,
    "confusion_matrix.png"
)
save_diagnostic_figure(
    fig_calibration_curve(y_eval, p_eval, f"Calibration — {evaluation_tag} ({evaluation_dataset})"),
    figures_local_path,
    "calibration_curve.png"
)
save_diagnostic_figure(
    fig_threshold_sweep(y_eval, p_eval, f"Threshold sweep — {evaluation_tag} ({evaluation_dataset})"),
    figures_local_path,
    "threshold_sweep.png"
)

print("All diagnostic figures generated and saved successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 7. Informe de clasificación
# MAGIC
# MAGIC La función `classification_report` genera una tabla detallada con *precision*, *recall* y *F1-score* desglosados por clase. El informe se guarda como fichero de texto plano (`.txt`) en el volumen de `Unity Catalog`.

# COMMAND ----------

target_names = ["Legit", "Fraud"]

report_path = str(Path(eval_tmp_path) / "classification_report.txt")

with open(report_path, "w") as fh:
    fh.write(classification_report(y_eval, pred_eval, target_names = target_names))

print(f"Classification report successfully saved to {report_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 8. Liberación de memoria y retorno del resultado
# MAGIC
# MAGIC Mismo patrón que en `07_Training_Job.ipynb`, devolviendo en este caso las métricas de evaluación y las rutas de los artefactos generados sobre la partición seleccionada.

# COMMAND ----------

del eval_predictions, pipeline_model, lr_fitted
gc.collect()

result = {
    "evaluation_tag": evaluation_tag,
    "evaluation_dataset": evaluation_dataset,
    "eval_metrics": eval_metrics,
    "figures_local_path": figures_local_path,
    "report_path": report_path,
    "best_threshold": best_threshold,
    "best_f1_at_threshold": best_f1_score
}

print(f"Exiting notebook and returning results for evaluation: {evaluation_tag}")
dbutils.notebook.exit(json.dumps(result))