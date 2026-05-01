# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Experimentación con `MLflow`
# MAGIC
# MAGIC **Autor**: Juan Carlos Alfaro Jiménez
# MAGIC
# MAGIC Esta libreta cubre la primera fase del ciclo `MLOps` profesional para el proyecto de detección de fraude en siniestros de automoción: **búsqueda de hiperparámetros usando los conjuntos de entrenamiento y validación**.
# MAGIC
# MAGIC El entrenamiento de cada modelo se delega en `07_Training_Job.ipynb` y la evaluación de métricas y generación de artefactos en `07_Evaluation_Job.ipynb`. Ambas se ejecutan en sesiones de `Spark Connect` completamente nuevas a través de `dbutils.notebook.run()`. Esta libreta se encarga exclusivamente de la instrumentación de `MLflow`.
# MAGIC
# MAGIC ### Capacidades de `MLflow` cubiertas en esta libreta
# MAGIC
# MAGIC | Categoría | Funcionalidades |
# MAGIC |---|---|
# MAGIC | **Experimento** | `set_experiment` y `set_experiment_tag` |
# MAGIC | ***Autologging*** | `autolog` para captura automática de metadatos del clúster |
# MAGIC | **Ejecuciones anidadas** | Ejecución padre de búsqueda de hiperparámetros contiene ejecuciones hijo por combinación |
# MAGIC | **Hiperparámetros** | `log_params` para hiperparámetros del preprocesado y clasificador |
# MAGIC | **Métricas con `step`** | `log_metric` para la curva de convergencia del optimizador |
# MAGIC | **Métricas vinculadas** | `log_metrics` con `model_id` + `dataset` |
# MAGIC | **Etiquetas de sistema** | proyecto, equipo, algoritmo, entorno y versiones |
# MAGIC | **Linaje del conjunto de datos** | `data.from_spark` + `log_input` |
# MAGIC | **Artefactos de figura** | `log_artifact`: *AUC-PR*, *AUC-ROC*, matrices de confusion, etc. |
# MAGIC | **Artefactos de texto** | `log_text`: informes de clasificación de `scikit-learn` |
# MAGIC | **Artefactos `.json`** | `log_dict`: resumen completo de hiperparámetros + métricas |
# MAGIC | **Artefactos `.csv`** | `log_artifact`: coeficientes ordenados por valor absoluto |
# MAGIC | **Búsqueda** | `search_runs`, `get_metric_history`, etc. |
# MAGIC | **`Unity Catalog`** | `register_model` con alias `candidate`, etiquetas y descripción |
# MAGIC | **Inspección** | `list_artifacts`, `get_run`, etc. |

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. Importaciones y configuración
# MAGIC
# MAGIC Se ejecuta `07_Utils.py` con la configuración compartida entre ambas libretas y se importan las librerías de `MLflow` necesarias para la gestión del ciclo de vida del modelo.

# COMMAND ----------

exec(open("07_Utils.py").read(), globals())

# COMMAND ----------

import itertools
import json
import logging
import os
import warnings
from datetime import datetime, timezone
from pathlib import Path

import mlflow
import mlflow.sklearn
from mlflow import MlflowClient
from mlflow.models.signature import infer_signature

import pandas as pd


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Además, definimos las variables de configuración específicas de esta libreta: el nombre de la libreta activa, los hiperparámetros del *grid search* y los ajustes de invocación del trabajo de entrenamiento distribuido. Las variables de proyecto compartidas (`project`, `team`, `uc_model_name`, `mlflow_experiment_path`, etc.) ya están disponibles en el espacio de nombres tras la ejecución de `07_Utils.py`.

# COMMAND ----------

notebook_path_raw = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook = Path(notebook_path_raw).name

class_imbalance_strategy = "inverse_frequency_weights"
parent_run_name = "lr_hyperparameter_search"
search_strategy = "full_grid"
selection_metric = "val_auc_pr"
training_notebook_path = "./07_Training_Job"
evaluation_notebook_path = "./07_Evaluation_Job"
training_mode = "train"
training_timeout_seconds = 3600
evaluation_timeout_seconds = 3600
training_max_retries = 3

print(f"Project: {project}, team: {team}, environment: {environment}")
print(f"Task: {task}, algorithm: {algorithm_family}, framework: {framework}")
print(f"Notebook: {notebook}, user: {current_user}")
print(f"Experiment path: {mlflow_experiment_path}")
print(f"Unity Catalog model name: {uc_model_name}")
print(f"Training notebook: {training_notebook_path} (timeout: {training_timeout_seconds} seconds)")
print(f"Evaluation notebook: {evaluation_notebook_path} (timeout: {evaluation_timeout_seconds} seconds)")
print(f"Databricks file system temporary directory: {os.environ['MLFLOW_DFS_TMP']}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Por último, al trabajar con `MLflow` en entornos modernos como `Databricks Serverless` o utilizando `Spark Connect`, es habitual encontrarse con mensajes de advertencia (*warnings*) que, si bien no detienen la ejecución ni afectan al modelo, ensucian la salida de la libreta.
# MAGIC
# MAGIC Para mantener la limpieza visual de la libreta y centrarnos únicamente en las métricas y resultados reales del experimento, utilizamos el siguiente bloque de código para silenciar de forma selectiva los mensajes correspondientes:

# COMMAND ----------

warnings.filterwarnings(
    action = "ignore",
    category = UserWarning,
    module = "mlflow.types.utils"
)

logging.getLogger("mlflow.data.spark_dataset").setLevel(logging.ERROR)
logging.getLogger("mlflow.data.spark_delta_utils").setLevel(logging.ERROR)

logging.getLogger("mlflow.utils.environment").setLevel(logging.ERROR)
logging.getLogger("mlflow.utils.requirements_utils").setLevel(logging.ERROR)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2. Centralización de hiperparámetros
# MAGIC
# MAGIC **Todos** los hiperparámetros del experimento se definen exclusivamente aquí: tanto los fijos del preprocesado y del clasificador variables como los del *grid search*. Ninguna libreta de entrenamiento define ni asume ningún valor: los recibe todos mediante *widgets*.
# MAGIC
# MAGIC ### Hiperparámetros del preprocesado: fijos
# MAGIC
# MAGIC * `pp_imputer_strategy = "median"`: estrategia de imputación para columnas con nulos: `median` es más robusta ante valores atípicos que `mean`.
# MAGIC * `pp_var_selector_threshold = 0.01`: varianza mínima para conservar una característica.
# MAGIC * `pp_scaler_with_mean = False`: `False` preserva la dispersidad de los vectores *one-hot*; centrar en la media destruiría la dispersidad.
# MAGIC * `pp_scaler_with_std = True`: `True` normaliza a varianza unitaria.
# MAGIC * `pp_ohe_drop_last = True`: `True` elimina la última categoría para evitar multicolinealidad perfecta en modelos lineales.
# MAGIC * `pp_si_handle_invalid = "keep"`: `keep` asigna un índice especial a categorías no vistas en producción en lugar de lanzar un error.
# MAGIC * `pp_si_order_type = "frequencyDesc"`: `frequencyDesc` asigna índice 0 a la categoría más frecuente; con `dropLast = True` en *one-hot-encoding*, esa categoría queda como referencia.
# MAGIC * `pp_ohe_handle_invalid = "keep"`: política del codificador *one-hot* ante índices no vistos en entrenamiento.
# MAGIC * `pp_asm_handle_invalid = "error"`: `error` actúa como guarda de calidad: falla de forma explícita si algún nulo no fue imputado en etapas anteriores.
# MAGIC
# MAGIC ### Hiperparámetros del clasificador: fijos
# MAGIC
# MAGIC * `lr_family = "binomial"`: `binomial` especifica el modelo de regresión logística binaria.
# MAGIC * `lr_standardization = False`: `False` porque el `StandardScaler` del *pipeline* ya normaliza las características; activarlo sería redundante.
# MAGIC * `lr_threshold = 0.5`: umbral de decisión por defecto.
# MAGIC
# MAGIC ### Hiperparámetros del clasificador: *grid search*
# MAGIC
# MAGIC * `lr_reg_param_list = [0.001, 0.01, 0.1]`: intensidad de la regularización.
# MAGIC * `lr_elastic_net_param_list = [0.0, 0.5]`: mezcla `L2` (0.0) y `L1` (1.0).
# MAGIC * `lr_max_iter_list = [100]`: iteraciones máximas del optimizador `L-BFGS`.

# COMMAND ----------

pp_imputer_strategy = "median"
pp_var_selector_threshold = 0.01
pp_scaler_with_mean = False
pp_scaler_with_std = True
pp_ohe_drop_last = True
pp_si_handle_invalid = "keep"
pp_si_order_type = "frequencyDesc"
pp_ohe_handle_invalid = "keep"
pp_asm_handle_invalid = "error"

lr_family = "binomial"
lr_standardization = False
lr_threshold = 0.5

lr_reg_param_list = [0.001, 0.01, 0.1]
lr_elastic_net_param_list = [0.0, 0.5]
lr_max_iter_list = [500]
# lr_max_iter_list = [100] # Por defecto
n_grid = len(lr_reg_param_list) * len(lr_elastic_net_param_list) * len(lr_max_iter_list)

print(f"For the missing value imputation step, the strategy is set to use the '{pp_imputer_strategy}' method to ensure robustness against outliers.")
print(f"The variance selector is configured to filter out features with a variance lower than the threshold of {pp_var_selector_threshold}.")
print(f"Categorical features will be indexed using the '{pp_si_order_type}' frequency order, and any unseen labels will be handled using the '{pp_si_handle_invalid}' policy.")
print(f"The one-hot encoder will drop the last category ({pp_ohe_drop_last}) to prevent multicollinearity, managing unseen categories with the '{pp_ohe_handle_invalid}' strategy.")
print(f"Feature scaling is defined with mean centering set to {pp_scaler_with_mean} to preserve sparsity, while standardization to unit variance is set to {pp_scaler_with_std}.")
print(f"The final feature assembler is set to '{pp_asm_handle_invalid}' to explicitly fail and act as a quality guard if any null values remain.")
print(f"The classifier is fixed to the '{lr_family}' family with a default decision threshold of {lr_threshold}, and internal standardization is disabled ({lr_standardization}).")
print(f"The grid search will evaluate {n_grid} combinations in total, testing the specific regularization parameters {lr_reg_param_list}, the elastic net mixing values {lr_elastic_net_param_list}, and a maximum iteration limit of {lr_max_iter_list}.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 3. Configuración de `MLflow`
# MAGIC
# MAGIC Antes de comenzar a entrenar los modelos, es fundamental configurar cómo y dónde se guardará toda la información generada. En nuestro entorno, `MLflow` se articula en torno a los siguientes componentes y configuraciones clave:

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### `Tracking Server` (servidor de seguimiento)
# MAGIC
# MAGIC Actúa como la "bitácora" centralizada durante toda la fase de experimentación. Su función principal es registrar, por cada **ejecución** individual, todos los detalles críticos: los hiperparámetros utilizados, las métricas de rendimiento obtenidas y los artefactos generados (como el modelo en sí, gráficos de evaluación o informes tabulares). Al fijarlo a `"databricks"`, le indicamos que utilice el servidor alojado de forma nativa en el *workspace*, eliminando la necesidad de desplegar o mantener infraestructura adicional.

# COMMAND ----------

tracking_uri = "databricks"
mlflow.set_tracking_uri(tracking_uri)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### `Model Registry` (registro de modelos)
# MAGIC
# MAGIC Es el "escaparate" o catálogo exclusivo diseñado para almacenar y versionar únicamente aquellos modelos que están listos para pasar a producción. Al establecerlo a `"databricks-uc"`, esto asegura que los modelos se registren como activos de datos de primer nivel dentro de `Unity Catalog`, heredando de forma automática las mismas políticas de seguridad, control de acceso y gobernanza que ya aplicamos a nuestras tablas `Delta`. Además, este enfoque permite gestionar el ciclo de vida del modelo utilizando alias semánticos y flexibles (como `candidate`, `champion` o `challenger`) en lugar de depender de estados rígidos predefinidos.

# COMMAND ----------

registry_uri = "databricks-uc"
mlflow.set_registry_uri(registry_uri)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### *Autologging* (registro automático)
# MAGIC
# MAGIC La función `mlflow.autolog()` captura de forma automática y transparente el plan lógico completo del *pipeline* de preprocesamiento, los metadatos del clúster, la versión exacta de `Python` y las dependencias del entorno. Sin embargo, la activamos con el parámetro `log_models = False`. Esta decisión técnica es deliberada: nos permite mantener un **control total y manual** sobre el proceso final de empaquetado, la definición del entorno de inferencia y la firma de las variables (*signature*) del modelo definitivo que decidamos guardar en el registro.

# COMMAND ----------

autolog_log_models = False
autolog_log_datasets = True
autolog_disable = False
autolog_silent = True

mlflow.autolog(
    log_models = autolog_log_models,
    log_datasets = autolog_log_datasets,
    disable = autolog_disable,
    silent = autolog_silent
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Creación y activación del experimento
# MAGIC
# MAGIC En esta primera parte, utilizamos la ruta definida previamente para establecer el experimento activo. Si no existe, `MLflow` lo creará automáticamente. A continuación, instanciamos el cliente y recuperamos el objeto del experimento para tener acceso a su identificador y otros metadatos necesarios para los siguientes pasos.
# MAGIC

# COMMAND ----------

mlflow.set_experiment(mlflow_experiment_path)

client = MlflowClient()
experiment = mlflow.get_experiment_by_name(mlflow_experiment_path)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Etiquetas a nivel de experimento
# MAGIC
# MAGIC Las etiquetas (*tags*) asignadas a un experimento son metadatos clave que resultan visibles directamente en la lista general del *workspace* y se mantienen persistentes a lo largo de todas las **ejecuciones** vinculadas a dicho experimento. Su propósito es documentar el contexto de negocio y técnico del proyecto de un simple vistazo. Esto facilita enormemente la búsqueda, filtrado y organización de los proyectos sin necesidad de tener que abrir o investigar ninguna **ejecución** de forma individual.

# COMMAND ----------

experiment_tags = {
    "project": project,
    "team": team,
    "task": task,
    "label_column": label_column,
    "training_table": training_table,
    "algorithm": algorithm_family,
    "notebook": notebook
}

for tag_key, tag_value in experiment_tags.items():
    client.set_experiment_tag(experiment.experiment_id, tag_key, tag_value)

print(f"Tracking URI: {tracking_uri}")
print(f"Registry URI: {registry_uri}")
print(f"Experiment name: {experiment.name}")
print(f"Experiment identifier: {experiment.experiment_id}")
print(f"Artifact store: {experiment.artifact_location}")
print(f"Tags set: {list(experiment_tags.keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 4. Registro de conjuntos de datos como entidades `MLflow`
# MAGIC
# MAGIC Los conjuntos de datos se envuelven en objetos `mlflow.data.SparkDataset` mediante `mlflow.data.from_spark`. Este objeto almacena dos elementos clave:
# MAGIC
# MAGIC 1. **El nombre de la tabla `Delta` de origen** (`table_name`): `MLflow` crea un enlace directo en la pestaña `Inputs` de cada ejecución, completando el linaje de datos desde el origen hasta el modelo.
# MAGIC 2. **Un *digest*** criptográfico del esquema y estadísticas de muestra: si los datos cambian entre experimentos, el *digest* cambia y queda documentado, permitiendo detectar **deriva de datos** sin comparar los datos directamente.

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

print(f"Train rows: {train_df.count():,}")
print(f"Validation rows: {validation_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 5. Funciones auxiliares privadas de registro en `MLflow`
# MAGIC
# MAGIC Cada una de estas funciones encapsula un tipo concreto de registro. El uso de funciones privadas (prefijo `_`) mejora la legibilidad de `run_lr_experiment`, facilita las pruebas unitarias y evita duplicación de código.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### `_build_run_note` y `_build_run_tags`
# MAGIC
# MAGIC Las etiquetas de la ejecución describen su contexto de forma no numérica. La etiqueta `mlflow.parentRunId` es especial: `MLflow` la usa para agrupar ejecuciones hijo bajo el padre en la interfaz.

# COMMAND ----------

child_note = """### Logistic regression training run — auto insurance fraud detection

This execution trains a logistic regression model using a regularization parameter of **{reg_param}**, an elastic net mixture of **{elastic_net_param}**, and a limit of **{max_iter}** maximum iterations. The random seed is fixed at **{seed}** to guarantee full reproducibility.

For model evaluation and selection, we are monitoring the area under the precision-recall curve (`val_auc_pr`) as our primary metric on the validation dataset.
"""

# COMMAND ----------

def _build_run_note(reg_param, elastic_net_param, max_iter):
    """
    Build the free-text run description shown in the notes panel.
    """
    return child_note.format(
        reg_param = reg_param,
        elastic_net_param = elastic_net_param,
        max_iter = max_iter,
        seed = seed
    )


def _build_run_tags(reg_param, elastic_net_param, max_iter, parent_run_id):
    """
    Build the full tag dictionary for a child training run.
    """
    return {
        "mlflow.parentRunId": parent_run_id,
        "mlflow.note.content": _build_run_note(reg_param, elastic_net_param, max_iter),
        "project": project,
        "team": team,
        "task": task,
        "algorithm": algorithm_family,
        "run_type": "training",
        "notebook": notebook,
        "framework": framework,
        "environment": environment,
        "label_column": label_column,
        "train_start_date": train_start_date,
        "train_end_date": train_end_date,
        "validation_start_date": validation_start_date,
        "validation_end_date": validation_end_date,
        "test_start_date": test_start_date,
        "test_end_date": test_end_date,
        "spark_version": spark.version,
        "mlflow_version": mlflow.__version__,
        "class_imbalance_strategy": class_imbalance_strategy
    }

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### `_build_hyperparameter_dicts`
# MAGIC
# MAGIC Construye dos diccionarios: `pp` con los hiperparámetros del preprocesado y `hp` con los del clasificador.

# COMMAND ----------

def _build_hyperparameter_dicts(reg_param, elastic_net_param, max_iter):
    """
    Build the classifier hyperparameter dictionary and the pipeline configuration dictionary.
    """
    pp = {
        "pp_imputer_strategy": pp_imputer_strategy,
        "pp_var_selector_threshold": pp_var_selector_threshold,
        "pp_scaler_with_mean": pp_scaler_with_mean,
        "pp_scaler_with_std": pp_scaler_with_std,
        "pp_ohe_drop_last": pp_ohe_drop_last,
        "pp_si_handle_invalid": pp_si_handle_invalid,
        "pp_si_order_type": pp_si_order_type,
        "pp_ohe_handle_invalid": pp_ohe_handle_invalid,
        "pp_asm_handle_invalid": pp_asm_handle_invalid,
        "pp_n_assembler_inputs": len(assembler_input_columns)
    }
    hp = {
        "lr_max_iter": max_iter,
        "lr_reg_param": reg_param,
        "lr_elastic_net_param": elastic_net_param,
        "lr_family": lr_family,
        "lr_standardization": lr_standardization,
        "lr_threshold": lr_threshold,
        "lr_weight_col": class_weight_column,
        "seed": seed
    }

    return pp, hp

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### `_log_convergence_metrics`
# MAGIC
# MAGIC Registra el `objectiveHistory` del optimizador `L-BFGS` con `step = i`. `MLflow` acumula la serie temporal y la renderiza como un gráfico de línea en la pestaña `Metrics`, permitiendo inspeccionar si el modelo converge o se atasca.

# COMMAND ----------

def _log_convergence_metrics(convergence_metadata, max_iter):
    """
    Log the objective history as a step metric and scalar convergence information.
    """
    for i, value in enumerate(convergence_metadata.get("objective_history", [])):
        mlflow.log_metric("lr_objective_history", value, step = i)

    mlflow.log_metric("lr_total_iterations", convergence_metadata.get("total_iterations", max_iter))
    mlflow.log_metric("lr_converged", convergence_metadata.get("converged", 0.0))
    mlflow.log_metric("lr_intercept", convergence_metadata.get("lr_intercept", 0.0))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### `_log_pipeline_model`
# MAGIC
# MAGIC `mlflow.spark.log_model` registra el `PipelineModel` completo como un **`LoggedModel`**, con:
# MAGIC
# MAGIC * **Firma** (`signature`): contrato de esquema validado en inferencia.
# MAGIC * **Ejemplo de entrada** (`input_example`): permite a `MLflow` generar código de inferencia automático.
# MAGIC
# MAGIC > **Nota sobre `Online Feature Store`**: Si el modelo consumiera tablas de características (*feature tables*) de `Unity Catalog`, la mejor práctica sería registrarlo utilizando el **`FeatureEngineeringClient`** (`fe.log_model(...)`). Esto empaqueta el modelo junto con los metadatos de las características (*feature lookups*), lo que permite que, al desplegarlo en un *endpoint* de servicio (*model serving*), consulte **automáticamente** los valores en tiempo real en el `Online Feature Store`, requiriendo únicamente las claves primarias (en este caso, `customer_id`) en la petición de inferencia. Para más información, consultar este [enlace](https://docs.databricks.com/aws/en/machine-learning/feature-store/train-models-with-feature-store$0).

# COMMAND ----------

def _log_pipeline_model(model_save_path, input_example_path, output_example_path):
    """
    Load the scikit-learn pipeline from the joblib file on the Unity Catalog volume
    and log it to MLflow using mlflow.sklearn.

    The model is deleted from memory immediately after logging to avoid cache pressure.
    model_save_path is the local path to the .joblib file written by 07_Training_Job.
    """
    import joblib
    pipeline_model = joblib.load(model_save_path)
    input_example = pd.read_parquet(input_example_path)
    output_example = pd.read_parquet(output_example_path)
    signature = infer_signature(input_example, output_example)

    mlflow.sklearn.log_model(
        sk_model = pipeline_model,
        artifact_path = "pipeline_model",
        signature = signature,
        # evitar que mlflow.sklearn.log_model genere los ficheros de entorno problemáticos (restricciones de S3)
        pip_requirements=[],
        extra_pip_requirements=None,
        conda_env=None
        #input_example = input_example # sirve para que MLflow genere código de inferencia automático en la UI. Desactivado por restricciones
    )

    del pipeline_model

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### `_log_split_metrics`
# MAGIC
# MAGIC Registra las métricas de rendimiento calculadas para los conjuntos de entrenamiento y validación. Cada métrica se registra por duplicado de forma intencionada: primero, vinculándola explícitamente al identificador del modelo y al conjunto de datos correspondiente (`dataset`) para que aparezca correctamente contextualizada en la pestaña del modelo; y segundo, a nivel general de la ejecución para que esté disponible en las tablas comparativas y permita filtrar resultados usando `search_runs`. Además, la función calcula y registra la brecha (*gap*) de rendimiento entre entrenamiento y validación para las áreas bajo la curva (*AUC-PR* y *AUC-ROC*), lo que nos proporciona un indicador directo y cuantificable del sobreajuste (*overfitting*).

# COMMAND ----------

def _log_split_metrics(train_metrics, validation_metrics):
    """
    Log training and validation metrics linked to the model entity.
    """
    train_prefixed = {f"train_{key}": value for key, value in train_metrics.items()}
    val_prefixed = {f"val_{key}": value for key, value in validation_metrics.items()}

    # mlflow.log_metrics(train_prefixed, dataset = mlflow_train_ds)
    # mlflow.log_metrics(train_prefixed)
    # mlflow.log_metrics(val_prefixed, dataset = mlflow_validation_ds)
    # mlflow.log_metrics(val_prefixed)

    train_prefixed = {f"train_{key}": value for key, value in train_metrics.items()}
    val_prefixed = {f"val_{key}": value for key, value in validation_metrics.items()}
    mlflow.log_metrics(train_prefixed)
    mlflow.log_metrics(val_prefixed)

    gap_auc_pr = train_metrics["auc_pr"] - validation_metrics["auc_pr"]
    gap_auc_roc = train_metrics["auc_roc"] - validation_metrics["auc_roc"]
    mlflow.log_metrics({"gap_auc_pr": gap_auc_pr, "gap_auc_roc": gap_auc_roc})

    return gap_auc_pr, gap_auc_roc

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### `_log_figures_and_reports` y `_log_coefficients`
# MAGIC
# MAGIC Estas funciones persisten todos los artefactos físicos generados por las dos sesiones del `07_Evaluation_Job.ipynb` (una sobre el conjunto de entrenamiento y otra sobre el de validación). Leen las figuras en formato `.png` y los informes de clasificación en texto plano directamente desde el volumen de almacenamiento, transfiriéndolos al almacén de artefactos de `MLflow` sin que el nodo orquestador los cargue en memoria.
# MAGIC
# MAGIC La función `_log_coefficients` persiste tanto la figura `.png` de los coeficientes como el archivo `.csv` generado por el trabajo de entrenamiento.

# COMMAND ----------

def _log_figures_and_reports(
    train_figures_path,
    val_figures_path,
    train_report_path,
    val_report_path
):
    """
    Log diagnostic figures and classification reports from both evaluation sessions.
    """
    for filename in os.listdir(train_figures_path):
        if filename.endswith(".png"):
            mlflow.log_artifact(
                str(Path(train_figures_path) / filename),
                artifact_path = str(Path("figures") / "train")
            )
    for filename in os.listdir(val_figures_path):
        if filename.endswith(".png"):
            mlflow.log_artifact(
                str(Path(val_figures_path) / filename),
                artifact_path = str(Path("figures") / "val")
            )
    with open(train_report_path) as fh:
        mlflow.log_text(fh.read(), str(Path("reports") / "train_classification_report.txt"))
    with open(val_report_path) as fh:
        mlflow.log_text(fh.read(), str(Path("reports") / "validation_classification_report.txt"))


def _log_coefficients(figures_local_path, coefficients_csv_path):
    """
    Log the logistic regression coefficients figure and `.csv` as artefacts.
    """
    mlflow.log_artifact(
        str(Path(figures_local_path) / "lr_coefficients.png"),
        artifact_path = str(Path("figures") / "train")
    )
    mlflow.log_artifact(
        coefficients_csv_path,
        artifact_path = "reports"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 6. Función principal `run_lr_experiment`
# MAGIC
# MAGIC Esta función coordina las llamadas a `07_Training_Job.ipynb` y `07_Evaluation_Job.ipynb` y el registro completo en `MLflow`. Su flujo interno es:
# MAGIC
# MAGIC 1. Construye el `run_tag` y los diccionarios de hiperparámetros.
# MAGIC 2. Abre una ejecución hija anidada con `mlflow.start_run(nested = True)` y registra etiquetas, hiperparámetros y linaje de datos.
# MAGIC 3. Lanza `07_Training_Job.ipynb` (entrena el modelo y lo serializa en el volumen).
# MAGIC 4. Registra la convergencia y el modelo en `MLflow`. En este momento el `run_id` ya existe y el artefacto está disponible bajo `runs:/{run_id}/pipeline_model`.
# MAGIC 5. Lanza `07_Evaluation_Job.ipynb` sobre el conjunto de entrenamiento usando esa `URI`.
# MAGIC 6. Lanza `07_Evaluation_Job.ipynb` sobre el conjunto de validación usando la misma `URI`.
# MAGIC 7. Registra métricas, figuras, informes, umbral óptimo y resumen `.json`.
# MAGIC
# MAGIC ### Por qué `nested = True`
# MAGIC
# MAGIC El parámetro `nested = True` le indica a `MLflow` que esta ejecución es hija de la ejecución activa en el contexto actual. Sin él, se abriría una ejecución de nivel superior totalmente desconectada del padre, perdiendo así la organización jerárquica en la interfaz de seguimiento.

# COMMAND ----------

def run_lr_experiment(reg_param, elastic_net_param, max_iter, parent_run_id):
    """
    Launch one grid point in isolated sessions and log everything.

    Calls `07_Training_Job` for model fitting, then `07_Evaluation_Job` twice
    (once on train and once on validation) to compute metrics and generate
    diagnostic artefacts in independent `Spark Connect` sessions.

    Returns a summary dictionary consumed by the parent run to select the best
    candidate and build the grid search report.
    """
    run_tag = f"lr__rp{reg_param}__en{elastic_net_param}__seed{seed}"
    pp, hp = _build_hyperparameter_dicts(reg_param, elastic_net_param, max_iter)
    run_name = f"{run_tag}__train"

    with mlflow.start_run(run_name = run_name, nested = True) as run:
        run_id = run.info.run_id

        mlflow.set_tags(_build_run_tags(reg_param, elastic_net_param, max_iter, parent_run_id))
        mlflow.log_params(pp)
        mlflow.log_params(hp)
        mlflow.log_input(mlflow_train_ds, context = "training")
        mlflow.log_input(mlflow_validation_ds, context = "validation")

        training_result_json = dbutils.notebook.run(
            training_notebook_path,
            training_timeout_seconds,
            {
                "reg_param": str(reg_param),
                "elastic_net_param": str(elastic_net_param),
                "max_iter": str(max_iter),
                "family": str(lr_family),
                "standardization": str(lr_standardization),
                "threshold": str(lr_threshold),
                "imputer_strategy": str(pp_imputer_strategy),
                "var_selector_threshold": str(pp_var_selector_threshold),
                "scaler_with_mean": str(pp_scaler_with_mean),
                "scaler_with_std": str(pp_scaler_with_std),
                "ohe_drop_last": str(pp_ohe_drop_last),
                "si_handle_invalid": str(pp_si_handle_invalid),
                "si_order_type": str(pp_si_order_type),
                "ohe_handle_invalid": str(pp_ohe_handle_invalid),
                "asm_handle_invalid": str(pp_asm_handle_invalid),
                "training_mode": str(training_mode)
            }
        )
        training_result = json.loads(training_result_json)

        # Convergence curve: "step = " is a special key that populates the "Metrics" timeline chart
        _log_convergence_metrics(training_result["convergence_metadata"], max_iter)

        _log_pipeline_model(
            training_result["model_save_path"],
            training_result["input_example_path"],
            training_result["output_example_path"]
        )

        model_artifact_uri = training_result["model_save_path"]

        train_eval_json = dbutils.notebook.run(
            evaluation_notebook_path,
            evaluation_timeout_seconds,
            {
                "model_artifact_uri": model_artifact_uri,
                "evaluation_dataset": "train",
                "evaluation_tag": f"{run_tag}_train"
            }
        )
        train_eval = json.loads(train_eval_json)

        val_eval_json = dbutils.notebook.run(
            evaluation_notebook_path,
            evaluation_timeout_seconds,
            {
                "model_artifact_uri": model_artifact_uri,
                "evaluation_dataset": "validation",
                "evaluation_tag": f"{run_tag}_val"
            }
        )
        val_eval = json.loads(val_eval_json)

        train_metrics = train_eval["eval_metrics"]
        validation_metrics = val_eval["eval_metrics"]

        gap_auc_pr, gap_auc_roc = _log_split_metrics(train_metrics, validation_metrics)

        _log_figures_and_reports(
            train_eval["figures_local_path"],
            val_eval["figures_local_path"],
            train_eval["report_path"],
            val_eval["report_path"]
        )

        _log_coefficients(
            training_result["figures_local_path"],
            training_result["coefficients_csv_path"]
        )

        best_threshold = val_eval["best_threshold"]
        best_f1_at_threshold = val_eval["best_f1_at_threshold"]
        mlflow.log_metric("val_best_f1_threshold", best_threshold)
        mlflow.log_metric("val_best_f1_at_threshold", best_f1_at_threshold)

        run_summary = {
            "pipeline_config": pp,
            "hyperparameters": hp,
            "train_metrics": train_metrics,
            "validation_metrics": validation_metrics,
            "gap_auc_pr": gap_auc_pr,
            "gap_auc_roc": gap_auc_roc,
            "best_threshold": best_threshold,
            "best_f1_at_threshold": best_f1_at_threshold
        }
        mlflow.log_dict(run_summary, str(Path("reports") / "run_summary.json"))

    return {
        "run_id": run_id,
        "run_tag": run_tag,
        "reg_param": reg_param,
        "elastic_net_param": elastic_net_param,
        "max_iter": max_iter,
        "train_metrics": train_metrics,
        "validation_metrics": validation_metrics,
        "gap_auc_pr": gap_auc_pr,
        "gap_auc_roc": gap_auc_roc,
        "best_threshold": best_threshold,
        "best_f1_at_threshold": best_f1_at_threshold
    }

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 7. Ejecución de la búsqueda de hiperparámetros
# MAGIC
# MAGIC Se lanza el bucle principal envuelto dentro de una **ejecución padre** que proporciona cuatro beneficios:
# MAGIC
# MAGIC 1. **Organización visual**: La interfaz agrupa automáticamente las ejecuciones hijas.
# MAGIC 2. **Documentación autónoma**: El padre registra los rangos del *grid* y la métrica de selección como parámetros.
# MAGIC 3. **Recuperación programática**: Todos los hijos comparten el mismo `parentRunId`, filtrable con un solo predicado en `search_runs`.
# MAGIC 4. **Puente hacia producción**: Después se pueden recuperar los hiperparámetros del ganador con `client.get_run(parent_run_id).data.params`.
# MAGIC
# MAGIC > **Nota de diseño sobre la métrica de selección**: Durante esta ejecución, el bucle evaluará y comparará cada combinación de hiperparámetros. Es fundamental destacar que el código está diseñado para seleccionar como ganador al modelo que maximice ***AUC-PR*** en el conjunto de validación, en lugar de utilizar *AUC-ROC*.  En problemas con un desbalanceo de clases tan severo como la detección de fraude (donde los casos legítimos abruman a los fraudulentos), el *AUC-ROC* puede ofrecer una visión engañosa y excesivamente optimista del rendimiento. Priorizar el *AUC-PR* asegura que el modelo seleccionado sea verdaderamente el más competente a la hora de identificar la clase minoritaria (fraude) sin inundar el sistema de falsas alarmas.

# COMMAND ----------

parent_note = f"""### Logistic regression hyperparameter search

This parent run coordinates a `{search_strategy}` to explore the following hyperparameter combinations:

* **Regularization (`reg_param`)**: `{lr_reg_param_list}`
* **Elastic net mixture (`elastic_net_param`)**: `{lr_elastic_net_param_list}`
* **Maximum number of iterations (`max_iter`)**: `{lr_max_iter_list}`

The primary selection metric used to determine the best performing model is **`{selection_metric}`**.
"""

# COMMAND ----------

all_results = []

with mlflow.start_run(run_name = parent_run_name) as parent_run:
    parent_run_id = parent_run.info.run_id

    mlflow.set_tags({
        "mlflow.note.content": parent_note,
        "project": project,
        "team": team,
        "task": task,
        "algorithm": algorithm_family,
        "run_type": "hyperparameter_search",
        "notebook": notebook,
        "search_strategy": search_strategy,
        "selection_metric": selection_metric
    })

    grid_params = {
        "grid_reg_param_list": str(lr_reg_param_list),
        "grid_elastic_net_param_list": str(lr_elastic_net_param_list),
        "grid_max_iter_list": str(lr_max_iter_list),
        "grid_total_runs": n_grid,
        "grid_selection_metric": selection_metric
    }
    mlflow.log_params(grid_params)

    hp_grid = itertools.product(lr_reg_param_list, lr_elastic_net_param_list, lr_max_iter_list)

    for reg_param, elastic_net_param, max_iter in hp_grid:
        print(
            f"\nreg_param = {reg_param}, "
            f"elastic_net_param = {elastic_net_param}, "
            f"max_iter = {max_iter}"
        )

        success = False
        for attempt in range(1, training_max_retries + 1):
            try:
                result = run_lr_experiment(
                    reg_param = reg_param,
                    elastic_net_param = elastic_net_param,
                    max_iter = max_iter,
                    parent_run_id = parent_run_id
                )
                all_results.append(result)
                success = True
                break
            except Exception as e:
                # Extract only the first line of the error to keep the console clean
                error_msg = str(e).split("\n")[0]
                print(f"Attempt {attempt} over {training_max_retries} failed. Details: {error_msg}")
        if not success:
            print(f"Configuration aborted after {training_max_retries} consecutive failures.")
            continue

    # Safety check: ensure at least one run was successful before calculating the maximum
    if not all_results:
        error_msg = "All grid configurations failed after max retries. Aborting notebook execution."
        print()
        print(f"{error_msg}")
        raise RuntimeError(error_msg)
    else:
        key = lambda result: result["validation_metrics"]["auc_pr"]
        best = max(all_results, key = key)

        best_summary_metrics = {
            "best_val_auc_pr": best["validation_metrics"]["auc_pr"],
            "best_val_auc_roc": best["validation_metrics"]["auc_roc"],
            "best_val_f1": best["validation_metrics"]["f1"],
            "best_val_recall": best["validation_metrics"]["recall"],
            "best_val_precision": best["validation_metrics"]["precision"],
            "best_val_gap_auc_pr": best["gap_auc_pr"],
            "n_runs_completed": len(all_results)
        }
        mlflow.log_metrics(best_summary_metrics)

        best_params_on_parent = {
            "best_reg_param": str(best["reg_param"]),
            "best_elastic_net_param": str(best["elastic_net_param"]),
            "best_max_iter": str(best["max_iter"]),
            "best_run_id": str(best["run_id"]),
            "best_threshold": str(best["best_threshold"])
        }
        mlflow.log_params(best_params_on_parent)

        grid_summary = {"best": best, "all_results": all_results}
        mlflow.log_dict(grid_summary, str(Path("reports") / "grid_search_summary.json"))

        print()
        print("Grid search complete.")
        print(f"Best AUC-PR: {best['validation_metrics']['auc_pr']:.4f}")
        print(f"Best regularization parameter: {best['reg_param']}")
        print(f"Best elastic net mixture parameter: {best['elastic_net_param']}")
        print(f"Best maximum number of iterations: {best['max_iter']}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 8. Limpieza de artefactos temporales
# MAGIC
# MAGIC Una vez que todos los artefactos han sido transferidos a `MLflow`, se eliminan los directorios de ejecuciones temporales y evaluaciones del volumen de `Unity Catalog` para liberar espacio de almacenamiento.

# COMMAND ----------

grid_runs_tmp_path = str(Path(uc_volume_path) / "runs")
dbutils.fs.rm(grid_runs_tmp_path, recurse = True)
print(f"Temporary training artefacts removed from {grid_runs_tmp_path}")

grid_evaluations_tmp_path = str(Path(uc_volume_path) / "evaluations")
dbutils.fs.rm(grid_evaluations_tmp_path, recurse = True)
print(f"Temporary evaluation artefacts removed from {grid_evaluations_tmp_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 9. Comparación programática de resultados
# MAGIC
# MAGIC Una vez finalizado el proceso de búsqueda de hiperparámetros (*grid search*), `MLflow` proporciona varios enfoques complementarios para consultar, analizar y seleccionar el mejor modelo.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 9.1. Tabla comparativa local
# MAGIC
# MAGIC El primer enfoque consiste en construir una tabla comparativa en memoria (`DataFrame`) generada directamente a partir de la lista de resultados (`all_results`) recopilada durante el bucle de entrenamiento. Esta estrategia ofrece una visualización inmediata y sin latencia, ya que al operar con los datos de la sesión, no requiere realizar llamadas a la `API` de `MLflow` a través de la red.

# COMMAND ----------

comparison_df = (
    pd.DataFrame([
        {
            "run_tag": results["run_tag"],
            "reg_param": results["reg_param"],
            "elastic_net_param": results["elastic_net_param"],
            "train_auc_pr": results["train_metrics"]["auc_pr"],
            "train_auc_roc": results["train_metrics"]["auc_roc"],
            "val_auc_pr": results["validation_metrics"]["auc_pr"],
            "val_auc_roc": results["validation_metrics"]["auc_roc"],
            "val_f1": results["validation_metrics"]["f1"],
            "val_recall": results["validation_metrics"]["recall"],
            "val_precision": results["validation_metrics"]["precision"],
            "gap_auc_pr": results["gap_auc_pr"],
            "best_threshold": results["best_threshold"]
        }
        for results in all_results
    ])
    .sort_values("val_auc_pr", ascending = False)
    .reset_index(drop = True)
)
comparison_df.round(4)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 9.2. Consulta a la `API` de `MLflow` con `search_runs`
# MAGIC
# MAGIC El segundo enfoque, y el más estandarizado para automatizar flujos de trabajo en producción, es utilizar la `API` nativa `mlflow.search_runs`. A diferencia de una tabla en memoria local, esta función interroga directamente al servidor de seguimiento (*tracking server*) de `Databricks` utilizando una sintaxis de filtrado muy similar a `SQL`.
# MAGIC
# MAGIC A continuación, implementamos esta búsqueda programática. El código utiliza la función `mlflow.search_runs` pasándole los parámetros exactos para materializar nuestra estrategia:
# MAGIC
# MAGIC * **`filter_string`**: Aplica el concepto de linaje buscando únicamente el `parentRunId` de nuestra ejecución principal.
# MAGIC * **`order_by`**: Ordena los resultados de forma descendente (`DESC`) según nuestra métrica de evaluación principal (en este caso, `metrics.val_auc_pr`), garantizando que el modelo con mejor rendimiento quede posicionado en la primera fila.
# MAGIC * **`max_results`**: Lo limitamos a `1` para que el servidor de seguimiento nos devuelva exclusivamente el ganador absoluto, optimizando así la consulta y el consumo de memoria.
# MAGIC
# MAGIC Una vez recuperado el registro ganador, el código extrae su `run_id` y sus hiperparámetros. Este identificador es la "llave maestra" que nos permitirá, más adelante, registrar el artefacto del modelo para su paso a producción.
# MAGIC
# MAGIC > **Concepto de arquitectura**: La clave de esta búsqueda está en usar `tags.mlflow.parentRunId`. En un entorno real colaborativo, múltiples ingenieros o procesos automáticos registrarán ejecuciones en el mismo experimento de forma simultánea. Este filtro garantiza un **linaje estricto**: permite aislar y recuperar única y exclusivamente las ejecuciones hijo que nacieron de la ejecución padre actual, separando nuestra selección del ruido generado por el resto del equipo.

# COMMAND ----------

experiment_id = experiment.experiment_id

child_runs_filter = f"tags.mlflow.parentRunId = '{parent_run_id}' AND tags.run_type = 'training'"
child_runs_order_by = ["metrics.val_auc_pr DESC"]

child_runs_df = mlflow.search_runs(
    experiment_ids = [experiment_id],
    filter_string = child_runs_filter,
    order_by = child_runs_order_by
)

display_columns = [
    "run_id",
    "params.lr_reg_param",
    "params.lr_elastic_net_param",
    "metrics.train_auc_pr",
    "metrics.val_auc_pr",
    "metrics.val_f1",
    "metrics.val_recall",
    "metrics.val_precision",
    "metrics.gap_auc_pr",
    "metrics.val_best_f1_threshold"
]

available_columns = [column for column in display_columns if column in child_runs_df.columns]
child_runs_df[available_columns].round(4)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 9.3. Inspección y trazabilidad del candidato con `client.get_run`
# MAGIC
# MAGIC El tercer enfoque inspecciona directamente la ejecución candidata usando `client.get_run`. A través de esta instrucción, recuperamos el objeto completo con toda la información del modelo. 
# MAGIC
# MAGIC Con este identificador, construimos además la **`URI` del modelo** (`runs:/...`). Esta `URI` actúa como la referencia canónica que utiliza `MLflow` para localizar físicamente el modelo empaquetado dentro del almacén de artefactos, y será el parámetro de entrada obligatorio para realizar el registro formal en `Unity Catalog`.
# MAGIC
# MAGIC > **Concepto de linaje**: En un entorno colaborativo, el `run_id` funciona como la llave maestra para la trazabilidad completa. Es el nexo que vincula de forma inequívoca los hiperparámetros, las métricas obtenidas, los conjuntos de datos de entrada y el propio artefacto del modelo en una única entidad auditable.

# COMMAND ----------

candidate_run_id = child_runs_df.iloc[0]["run_id"]
candidate_run = client.get_run(candidate_run_id)

pipeline_artifact_path = "pipeline_model"
candidate_model_uri = f"runs:/{candidate_run_id}/{pipeline_artifact_path}"  # mlflow.sklearn logged artifact

print(f"Run identifier: {candidate_run_id}")
print(f"Run name: {candidate_run.data.tags.get('mlflow.runName', 'n/a')}")
print(f"Status: {candidate_run.info.status}")
print(f"Model URI: {candidate_model_uri}")
print()

print("Hyperparameters:")
for key, value in sorted(candidate_run.data.params.items()):
    print(f"\t{key} = {value}")
print()

print("Metrics:")
for key, value in sorted(candidate_run.data.metrics.items()):
    print(f"\t{key} = {value:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 10. Análisis detallado del modelo candidato

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 10.1. Auditoría de metadatos operativos con `get_run`
# MAGIC
# MAGIC Mientras que en la sección anterior extrajimos un resumen del rendimiento para la selección del modelo, el objeto devuelto por `client.get_run(run_id)` contiene además una cantidad masiva de metadatos operativos. 
# MAGIC
# MAGIC Esta vista profunda es especialmente útil para tareas de depuración (*debugging*) y auditoría de costes. Nos permite verificar el estado del ciclo de vida en el servidor, calcular exactamente cuánto tiempo tomó el entrenamiento distribuido en el clúster (usando las marcas de tiempo nativas) y auditar la volumetría total de hiperparámetros y métricas que `MLflow` ha capturado en segundo plano.

# COMMAND ----------

print(f"Run identifier: {candidate_run.info.run_id}")
print(f"Lifecycle stage: {candidate_run.info.lifecycle_stage}")
print(f"Artifact URI: {candidate_run.info.artifact_uri}")
print()

start_ts = datetime.fromtimestamp(candidate_run.info.start_time / 1000, timezone.utc)
end_ts = datetime.fromtimestamp(candidate_run.info.end_time / 1000, timezone.utc)
duration_sec = (end_ts - start_ts).total_seconds()

print("Chronometry")
print(f"Start: {start_ts.isoformat()}")
print(f"End: {end_ts.isoformat()}")
print(f"Total duration: {duration_sec} seconds\n")
print()

print("Telemetry volume:")
print(f"Tracked parameters: {candidate_run.data.params}")
print(f"Tracked metrics: {candidate_run.data.metrics}")
print(f"Tracked tags: {candidate_run.data.tags}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 10.2. Análisis de la curva de convergencia
# MAGIC
# MAGIC La función `client.get_metric_history(run_id, metric_key)` nos permite recuperar la serie temporal completa de la función de pérdida (`loss`) registrada en la libreta de entrenamiento.
# MAGIC
# MAGIC Para validar la calidad del ajuste, esta curva debe mostrar un descenso monótono. Si la pérdida final coincide con el límite de iteraciones configurado (`max_iter`), es un indicador claro de que el modelo no llegó a converger. En entornos de producción, esto nos obligaría a revisar la regularización o incrementar el presupuesto computacional del optimizador.

# COMMAND ----------

objective_history = client.get_metric_history(candidate_run_id, "lr_objective_history")

if objective_history:
    print(f"Convergence history: {len(objective_history)} steps")
    print(f"Initial loss: {objective_history[0].value:.6f}")
    print(f"Final loss: {objective_history[-1].value:.6f}")
    print(f"Total drop: {objective_history[0].value - objective_history[-1].value:.6f}")
    print()
    print("First 5 steps:")
    for entry in objective_history[:5]:
        print(f"Step {entry.step} with value {entry.value:.6f}")
else:
    print("Convergence history not available (sklearn solver does not expose per-iteration loss).")
    total_iter_history = client.get_metric_history(candidate_run_id, "lr_total_iterations")
    if total_iter_history:
        print(f"Total iterations: {total_iter_history[-1].value:.0f}")
    converged_history = client.get_metric_history(candidate_run_id, "lr_converged")
    if converged_history:
        print(f"Converged: {bool(converged_history[-1].value)}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 10.3. Auditoría del árbol de artefactos
# MAGIC
# MAGIC La función `client.list_artifacts(run_id)` permite inspeccionar de forma programática la estructura de archivos almacenada en el servidor de `MLflow`. Esta capacidad es vital para procesos de automatización donde necesitamos verificar la existencia de informes `.pdf`, archivos `.csv` de coeficientes o el propio modelo empaquetado antes de proceder a su registro en el catálogo.

# COMMAND ----------

for artifact in client.list_artifacts(candidate_run_id):
    marker = "[DIR]" if artifact.is_dir else "[FILE]"
    print(f"{marker} {artifact.path}")
    if artifact.is_dir:
        for sub_artifact in client.list_artifacts(candidate_run_id, artifact.path):
            sub_marker = "[DIR]" if sub_artifact.is_dir else "[FILE]"
            print(f"\t{sub_marker} {sub_artifact.path}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 11. Registro en `Unity Catalog Model Registry`
# MAGIC
# MAGIC Una vez seleccionado, el modelo candidato se registra bajo el alias **`candidate`** en lugar de `champion`. Esta distinción es fundamental para mantener un flujo de trabajo `MLOps` correcto y organizado:
# MAGIC
# MAGIC | Alias | Significado | ¿Quién lo asigna? |
# MAGIC |---|---|---|
# MAGIC | **`candidate`** | Es el mejor modelo en fase de validación, aún pendiente de la prueba final (conjunto de datos de prueba). | Libreta actual |
# MAGIC | **`champion`** | Es el modelo que ya ha sido evaluado en el conjunto de datos de prueba y aprobado para su paso a producción. | Libreta siguiente |
# MAGIC | **`challenger`** | Es un modelo que se encuentra en evaluación compitiendo contra el actual `champion`. | *Pipeline* de despliegue |
# MAGIC
# MAGIC ### Diferencia entre etiquetas de modelo y de versión
# MAGIC
# MAGIC Para asegurar un buen gobierno del dato, es importante distinguir cómo documentamos estos activos en el registro:
# MAGIC
# MAGIC * **Etiquetas de modelo** (`set_registered_model_tag`): Son persistentes a lo largo de todas las versiones del modelo. Se utilizan para guardar información estable y general, como el nombre del proyecto o el equipo responsable.
# MAGIC * **Etiquetas de versión** (`set_model_version_tag`): Son específicas de la versión particular que estamos registrando. Sirven para documentar datos puntuales como las métricas obtenidas, la ejecución exacta de origen y el criterio que se usó para su selección, lo cual resulta vital para futuras auditorías.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 11.1. Registro de la versión del modelo
# MAGIC
# MAGIC El primer paso es materializar el artefacto del experimento como una versión oficial dentro del catálogo. Esto crea una entrada en `Unity Catalog` vinculada a la `URI` del modelo guardado anteriormente.

# COMMAND ----------

# uc_model_version = mlflow.register_model(
#     model_uri = candidate_model_uri,
#     name = uc_model_name
# )

uc_model_version = client.create_model_version(
    name = uc_model_name,
    source = candidate_run.info.artifact_uri + "/pipeline_model",
    run_id = candidate_run_id
)

print(f"Registered model: {uc_model_name}")
print(f"Version: {uc_model_version.version}")
print(f"Status: {uc_model_version.status}")
print(f"Source URI: {uc_model_version.source}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 11.2. Asignación del alias y descripción del modelo
# MAGIC
# MAGIC Para facilitar la automatización, asignamos el alias `candidate`. Además, actualizamos la descripción general del modelo registrado para documentar qué tipo de algoritmo es y cuál es su arquitectura (*pipeline*).

# COMMAND ----------

candidate_alias = "candidate"
client.set_registered_model_alias(
    name = uc_model_name,
    alias = candidate_alias,
    version = uc_model_version.version
)
print(f"Alias '{candidate_alias}' → version {uc_model_version.version}")

model_description = f"""
### Logistic regression pipeline

Model designed for **auto insurance fraud detection** (siniestros de automoción).

* **Training data**: `{training_table}`.
* **Label**: `{label_column}` (`1` = fraud, `0` = legitimate claim).
* **Class imbalance**: handled via inverse-frequency sample weights (~7.9% fraud rate).

#### Pipeline architecture

1. `Imputer` (median for avg/ratio columns, constant 0 for count/sum columns)
2. Boolean casting (`COALESCE CAST` via `SQLTransformer`)
3. Feature engineering (`claimed_amount_log`, `late_report_flag`, `multi_party_flag`)
4. `StringIndexer`
5. `OneHotEncoder`
6. `VectorAssembler`
7. `VarianceThresholdSelector`
8. `StandardScaler`
9. `LogisticRegression`

> **Selection metric**: `val_auc_pr`.
> Temporal split dates and training mode are recorded in the version run tags.
"""

client.update_registered_model(name = uc_model_name, description = model_description)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 11.3. Configuración de etiquetas de modelo (*model tags*)
# MAGIC
# MAGIC Las etiquetas de modelo son **persistentes**. Se utilizan para metadatos que no suelen cambiar entre versiones, como el nombre del proyecto, el equipo responsable o el *framework* utilizado.

# COMMAND ----------

client.set_registered_model_tag(uc_model_name, "project", project)
client.set_registered_model_tag(uc_model_name, "team", team)
client.set_registered_model_tag(uc_model_name, "algorithm", algorithm_family)
client.set_registered_model_tag(uc_model_name, "framework", framework)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 11.4. Configuración de etiquetas de versión (*version tags*)
# MAGIC
# MAGIC A diferencia de las anteriores, estas etiquetas son **específicas de esta versión**. Documentan el linaje exacto (identificador de ejecución, fecha de división de datos) y las métricas obtenidas que justificaron su selección.

# COMMAND ----------

candidate_reg_param = candidate_run.data.params.get("lr_reg_param")
candidate_elastic_net_param = candidate_run.data.params.get("lr_elastic_net_param")
candidate_val_auc_pr = candidate_run.data.metrics.get("val_auc_pr")
candidate_val_auc_roc = candidate_run.data.metrics.get("val_auc_roc")
candidate_best_threshold = candidate_run.data.metrics.get("val_best_f1_threshold")
version_number = uc_model_version.version

client.set_model_version_tag(uc_model_name, version_number, "promoted_by", "auto__max_val_auc_pr")
client.set_model_version_tag(uc_model_name, version_number, "notebook_origin", notebook)
client.set_model_version_tag(uc_model_name, version_number, "parent_run_id", parent_run_id)
client.set_model_version_tag(uc_model_name, version_number, "candidate_run_id", candidate_run_id)
client.set_model_version_tag(uc_model_name, version_number, "train_end_date", train_end_date)
client.set_model_version_tag(uc_model_name, version_number, "validation_end_date", validation_end_date)
client.set_model_version_tag(uc_model_name, version_number, "reg_param", candidate_reg_param)
client.set_model_version_tag(uc_model_name, version_number, "elastic_net_param", candidate_elastic_net_param)
client.set_model_version_tag(uc_model_name, version_number, "val_auc_pr", f"{candidate_val_auc_pr:.4f}")
client.set_model_version_tag(uc_model_name, version_number, "val_auc_roc", f"{candidate_val_auc_roc:.4f}")
client.set_model_version_tag(uc_model_name, version_number, "best_threshold_val", f"{candidate_best_threshold:.2f}")
client.set_model_version_tag(uc_model_name, version_number, "lr_max_iter", candidate_run.data.params.get("lr_max_iter"))
client.set_model_version_tag(uc_model_name, version_number, "lr_family", candidate_run.data.params.get("lr_family"))
client.set_model_version_tag(uc_model_name, version_number, "lr_standardization", candidate_run.data.params.get("lr_standardization"))
client.set_model_version_tag(uc_model_name, version_number, "pp_imputer_strategy", candidate_run.data.params.get("pp_imputer_strategy"))
client.set_model_version_tag(uc_model_name, version_number, "pp_var_selector_threshold", candidate_run.data.params.get("pp_var_selector_threshold"))
client.set_model_version_tag(uc_model_name, version_number, "pp_scaler_with_mean", candidate_run.data.params.get("pp_scaler_with_mean"))
client.set_model_version_tag(uc_model_name, version_number, "pp_scaler_with_std", candidate_run.data.params.get("pp_scaler_with_std"))
client.set_model_version_tag(uc_model_name, version_number, "pp_ohe_drop_last", candidate_run.data.params.get("pp_ohe_drop_last"))
client.set_model_version_tag(uc_model_name, version_number, "pp_si_handle_invalid", candidate_run.data.params.get("pp_si_handle_invalid"))
client.set_model_version_tag(uc_model_name, version_number, "pp_si_order_type", candidate_run.data.params.get("pp_si_order_type"))
client.set_model_version_tag(uc_model_name, version_number, "pp_ohe_handle_invalid", candidate_run.data.params.get("pp_ohe_handle_invalid"))
client.set_model_version_tag(uc_model_name, version_number, "pp_asm_handle_invalid", candidate_run.data.params.get("pp_asm_handle_invalid"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 11.5. Verificación final del registro
# MAGIC
# MAGIC Como paso final, documentamos la versión con un resumen detallado y recuperamos el objeto desde `Unity Catalog` para confirmar que los alias y metadatos se han aplicado correctamente.

# COMMAND ----------

version_description = f"""
### Candidate version (alias: `{candidate_alias}`)

Selected for maximizing the **AUC-PR** metric on the validation dataset.

#### Regularization hyperparameters

* **`reg_param`**: `{candidate_reg_param}`
* **`elastic_net_param`**: `{candidate_elastic_net_param}`

#### Validation metrics

* **AUC-PR**: `{candidate_val_auc_pr:.4f}`
* **AUC-ROC**: `{candidate_val_auc_roc:.4f}`

**Decision threshold (from validation)**: `{candidate_best_threshold:.2f}`
**Parent run**: `{parent_run_id}`
"""
client.update_model_version(
    name = uc_model_name,
    version = version_number,
    description = version_description.strip()
)

model_version = client.get_model_version_by_alias(name = uc_model_name, alias = candidate_alias)
print(f"Candidate version : {model_version.version}")
indented_desc = model_version.description.replace(chr(10), chr(10) + "  ")
print(f"Description:\n {indented_desc}")
print(f"Tags: {model_version.tags}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 12. Búsquedas avanzadas de referencia
# MAGIC
# MAGIC A continuación, repasamos varios patrones de búsqueda que resultan muy útiles para tareas de monitorización, auditoría y depuración en el día a día. 
# MAGIC
# MAGIC Los cuatro ejemplos utilizan la `API` `search_runs` (que consulta los metadatos de las ejecuciones).

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 12.1. Ejecuciones con alto rendimiento
# MAGIC
# MAGIC Este primer patrón busca aislar únicamente aquellas ejecuciones de entrenamiento (`tags.run_type = 'training'`) que hayan superado un umbral de rendimiento específico en nuestra métrica principal (por ejemplo, `val_auc_pr > 0.1`). Además, ordena los resultados de mayor a menor para destacar a los mejores candidatos.

# COMMAND ----------

high_performance_filter = "metrics.val_auc_pr > 0.1 AND tags.run_type = 'training'"
high_performance_order_by = ["metrics.val_auc_pr DESC"]

high_performance_runs = mlflow.search_runs(
    experiment_ids = [experiment_id],
    filter_string = high_performance_filter,
    order_by = high_performance_order_by
)

high_performance_runs

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 12.2. Riesgo de sobreajuste por baja regularización
# MAGIC
# MAGIC Aquí buscamos ejecuciones de entrenamiento donde el parámetro de regularización (`lr_reg_param`) se haya configurado con un valor muy bajo (igual a 0.001). Esto es ideal para auditar rápidamente las ejecuciones que podrían tener un mayor riesgo de sufrir sobreajuste (*overfitting*).

# COMMAND ----------

low_regularization_filter = "params.lr_reg_param = '0.001' AND tags.run_type = 'training'"

low_regularization_runs = mlflow.search_runs(
    experiment_ids = [experiment_id],
    filter_string = low_regularization_filter
)

low_regularization_runs

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 12.3. Brecha de generalización u *overfitting*
# MAGIC
# MAGIC Este filtro detecta ejecuciones en las que la diferencia de rendimiento entre el conjunto de entrenamiento y el de validación (`gap_auc_pr`) supera un umbral crítico (en este caso, 0.05). Es una alerta directa para identificar modelos que están memorizando los datos en lugar de aprender a generalizar.

# COMMAND ----------

overfitting_filter = "metrics.gap_auc_pr > 0.05 AND tags.run_type = 'training'"

overfitting_runs = mlflow.search_runs(
    experiment_ids = [experiment_id],
    filter_string = overfitting_filter
)

overfitting_runs

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 12.4. Ejecuciones de un *grid search* específico
# MAGIC
# MAGIC Utilizando el concepto de linaje, este patrón recupera absolutamente todas las ejecuciones hijas que pertenecen a una ronda de hiperparámetros principal, filtrando sencillamente por su `parentRunId`.

# COMMAND ----------

all_grid_runs_filter = f"tags.mlflow.parentRunId = '{parent_run_id}'"

all_grid_runs = mlflow.search_runs(
    experiment_ids = [experiment_id],
    filter_string = all_grid_runs_filter
)

all_grid_runs

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 13. Conclusiones y siguientes pasos
# MAGIC
# MAGIC ### ¿Qué hemos visto?
# MAGIC
# MAGIC En esta libreta hemos automatizado la selección y el registro del mejor modelo de nuestro experimento utilizando la `API` de `MLflow` y `Unity Catalog`:
# MAGIC
# MAGIC 1. **Búsqueda programática**: Hemos superado la limitación de revisar modelos manualmente en la interfaz. Utilizando `mlflow.search_runs`, hemos consultado el servidor de seguimiento para filtrar y ordenar las ejecuciones basándonos estrictamente en su rendimiento (`val_auc_pr`), métricas de validación y linaje (`parentRunId`).
# MAGIC 2. **Auditoría profunda del candidato**: Antes de promover cualquier modelo, hemos validado su información. Usando `client.get_run` inspeccionamos a fondo la ejecución ganadora, recuperando sus hiperparámetros, métricas finales y la `URI` canónica de su artefacto físico, garantizando así la trazabilidad total desde el entrenamiento hasta el almacenamiento.
# MAGIC 3. **Registro orientado a `MLOps`**: Registramos el modelo ganador en `Unity Catalog` asignándole el alias de **`candidate`**. Entendimos que esta nomenclatura es crucial: un modelo recién salido del ajuste de hiperparámetros es solo un candidato hasta que demuestre su valor en un entorno de evaluación final.
# MAGIC 4. **Gobierno y trazabilidad**: Aplicamos la separación correcta de etiquetas en el `Model Registry`. Usamos etiquetas a nivel de modelo para metadatos organizativos estables (proyecto, equipo, *framework*), y etiquetas a nivel de versión para documentar el contexto específico (métricas, identificador de ejecución y criterios de selección) que justificó la elección de este modelo frente al resto.
# MAGIC
# MAGIC ### ¿Cuándo volver a ejecutar esta libreta?
# MAGIC
# MAGIC * **Primera vez**: Para evaluar el experimento inicial, seleccionar el primer modelo viable y registrarlo como el candidato base del proyecto.
# MAGIC * **Tras un ciclo de reentrenamiento**: Cuando el sistema de monitorización detecte *concept drift* y dispare el *pipeline* de reentrenamiento automático, se generarán cientos de ejecuciones nuevas. Esta libreta se ejecutará a continuación para cribar esos nuevos resultados y proponer un nuevo candidato actualizado.
# MAGIC * **Al cambiar los criterios de negocio**: Si el negocio decide priorizar la exhaustividad (*recall*) por encima de la precisión (*precision*), será necesario actualizar la métrica en la lógica de `search_runs` (`order_by`) y volver a ejecutar la libreta para seleccionar el modelo que mejor se adapte al nuevo objetivo.
# MAGIC * **Nunca de forma manual en producción**: Al igual que la ingesta de datos, esta libreta está diseñada para ser un nodo intermedio dentro de un flujo de trabajo orquestado. Su ejecución manual debe reservarse exclusivamente para entornos de desarrollo y depuración.
# MAGIC
# MAGIC ### ¿Qué sigue?
# MAGIC
# MAGIC Con nuestro mejor modelo asegurado y registrado bajo el alias `candidate` en `Unity Catalog`, el ciclo de vida del proyecto avanza hacia la fase de pruebas estandarizadas:
# MAGIC
# MAGIC 1. **Evaluación contra el conjunto de prueba**: La siguiente etapa del *pipeline* cargará este modelo `candidate` y lo enfrentará, por primera vez, a un conjunto de datos de prueba completamente aislado que no participó en la fase de validación. Esto nos dará la medida real de su capacidad de generalización.
# MAGIC 2. **Desafío al modelo en producción (`champion` contra `challenger`)**: Si ya existe un modelo operando en producción (el actual `champion`), el nuevo candidato asumirá el rol de `challenger`. La siguiente libreta comparará métricas estrictas entre ambos. Solo si el nuevo candidato demuestra una mejora estadísticamente significativa sin degradar la latencia ni la equidad del modelo, el sistema automatizado le transferirá el alias de `champion`, aprobando su despliegue definitivo para inferencia.