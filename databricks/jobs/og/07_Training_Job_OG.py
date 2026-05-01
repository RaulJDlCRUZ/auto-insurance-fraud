# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Entrenamiento de una combinación de hiperparámetros
# MAGIC
# MAGIC **Autor**: Juan Carlos Alfaro Jiménez
# MAGIC
# MAGIC Esta libreta **no contiene ninguna llamada a `MLflow`**. Su única responsabilidad es aislar el entrenamiento de un único `PipelineModel` de `MLlib` utilizando los hiperparámetros recibidos. Durante su ejecución independiente, ajustará el modelo a los datos y guardará el artefacto físico resultante directamente en un volumen de `Unity Catalog`.
# MAGIC
# MAGIC ### ¿Por qué desacoplar el entrenamiento de la orquestadora?
# MAGIC
# MAGIC `Spark Connect` gestiona los modelos entrenados por `MLlib` almacenándolos en una caché del lado del servidor, la cual tiene un límite estricto de **1`GB` por sesión**. Si intentásemos entrenar iterativamente múltiples combinaciones de hiperparámetros (un *grid search* tradicional) dentro de una misma sesión, estos modelos se acumularían rápidamente en memoria, provocando el fallo `ML_CACHE_SIZE_OVERFLOW_EXCEPTION`.
# MAGIC
# MAGIC La solución arquitectónica óptima consiste en delegar el entrenamiento a esta libreta secundaria, invocándola de forma aislada mediante la instrucción `dbutils.notebook.run()` desde cualquier libreta orquestadora del proyecto (ya sea en la fase de experimentación o durante el paso a producción). 
# MAGIC
# MAGIC De este modo, cada invocación levanta una **sesión de `Spark Connect` completamente limpia y nueva**, garantizando que la caché de memoria se libere correctamente al terminar cada ciclo de entrenamiento.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. Importaciones y carga de utilidades compartidas
# MAGIC
# MAGIC El *script* `07_Utils.py` actúa como nuestra caja de herramientas central. Expone la configuración estructural del proyecto y las funciones maestras para la carga controlada del conjunto de datos, la generación de los *splits* temporales y el cálculo dinámico de los pesos de clase. Además, deja preparados en el entorno global todas las listas de columnas y expresiones `SQL` necesarias para construir el *pipeline* de preprocesamiento.
# MAGIC
# MAGIC Aunque este *script* también contiene funciones de evaluación de modelos, en esta sesión aislada nos apoyaremos en él **exclusivamente** para la creación del conjunto de datos, el entrenamiento del *pipeline* y, finalmente, la generación gráfica de los coeficientes del algoritmo.
# MAGIC
# MAGIC A continuación, importamos las librerías necesarias **exclusivamente** para el ensamblado y entrenamiento del *pipeline* en esta sesión aislada.

# COMMAND ----------

exec(open("07_Utils.py").read(), globals())

# COMMAND ----------

import gc
import json
from pathlib import Path

import pandas as pd

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import (
    Imputer,
    OneHotEncoder,
    SQLTransformer,
    StandardScaler,
    StringIndexer,
    VarianceThresholdSelector,
    VectorAssembler
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2. Recepción de parámetros
# MAGIC
# MAGIC Los *widgets* son el mecanismo estándar en `Databricks` para parametrizar y pasar información dinámicamente entre libretas. En nuestro flujo, la libreta orquestadora inyecta todos estos valores simultáneamente utilizando el parámetro `arguments` de la instrucción `dbutils.notebook.run()`.
# MAGIC
# MAGIC > **Importante sobre el tipado**: Todos los valores recibidos a través de los *widgets* llegan **siempre como cadenas de texto** (`String`). Es imprescindible convertirlos explícitamente a su tipo de dato correspondiente (flotantes, enteros o booleanos) antes de pasarlos al modelo o al *pipeline*.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2.1. Hiperparámetros del preprocesado
# MAGIC
# MAGIC Estos hiperparámetros controlan el comportamiento de las etapas de transformación de datos previas al clasificador. No se definen aquí, esta libreta solo los recibe y los aplica:
# MAGIC
# MAGIC * **`imputer_strategy`**: Estrategia de imputación para columnas numéricas con nulos (`median` es más robusta ante valores atípicos que `mean`).
# MAGIC * **`var_selector_threshold`**: Varianza mínima que debe tener una característica para no ser descartada.
# MAGIC * **`scaler_with_mean`**: Centrar los datos en la media antes de escalar (mantenido en `False` para no destruir la dispersidad del *one-hot encoding*).
# MAGIC * **`scaler_with_std`**: Normalizar la distribución a varianza unitaria.
# MAGIC * **`ohe_drop_last`**: Eliminar la última categoría en la codificación *one-hot* para evitar multicolinealidad perfecta.
# MAGIC * **`si_handle_invalid`**: Política del `StringIndexer` ante categorías nuevas (`keep` asigna un índice especial).
# MAGIC * **`si_order_type`**: Criterio de ordenación de categorías (`frequencyDesc` asigna el índice 0 a la más frecuente).
# MAGIC * **`ohe_handle_invalid`**: Política del codificador *one-hot* ante índices desconocidos en inferencia.
# MAGIC * **`asm_handle_invalid`**: Política del `VectorAssembler` ante nulos residuales (`error` actúa como barrera de calidad estricta).

# COMMAND ----------

# Default values are used only in interactive runs; notebook.run() overrides them
dbutils.widgets.text("imputer_strategy", "median")
dbutils.widgets.text("var_selector_threshold", "0.01")
dbutils.widgets.text("scaler_with_mean", "False")
dbutils.widgets.text("scaler_with_std", "True")
dbutils.widgets.text("ohe_drop_last", "True")
dbutils.widgets.text("si_handle_invalid", "keep")
dbutils.widgets.text("si_order_type", "frequencyDesc")
dbutils.widgets.text("ohe_handle_invalid", "keep")
dbutils.widgets.text("asm_handle_invalid", "error")

imputer_strategy = dbutils.widgets.get("imputer_strategy")
var_selector_threshold = float(dbutils.widgets.get("var_selector_threshold"))
scaler_with_mean = dbutils.widgets.get("scaler_with_mean").lower() == "true"
scaler_with_std = dbutils.widgets.get("scaler_with_std").lower() == "true"
ohe_drop_last = dbutils.widgets.get("ohe_drop_last").lower() == "true"
si_handle_invalid = dbutils.widgets.get("si_handle_invalid")
si_order_type = dbutils.widgets.get("si_order_type")
ohe_handle_invalid = dbutils.widgets.get("ohe_handle_invalid")
asm_handle_invalid = dbutils.widgets.get("asm_handle_invalid")

print(f"Imputer strategy: {imputer_strategy}")
print(f"Variance selector threshold: {var_selector_threshold}")
print(f"Scaler with mean: {scaler_with_mean}")
print(f"Scaler with standard deviation: {scaler_with_std}")
print(f"One-hot encoding drop last category: {ohe_drop_last}")
print(f"String indexer handle invalid: {si_handle_invalid}")
print(f"String indexer order type: {si_order_type}")
print(f"One-hot encoding handle invalid: {ohe_handle_invalid}")
print(f"Assembler handle invalid: {asm_handle_invalid}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2.2. Hiperparámetros del clasificador
# MAGIC
# MAGIC Estos hiperparámetros controlan directamente el comportamiento del algoritmo final (`LogisticRegression`):
# MAGIC
# MAGIC * **`reg_param`**: Intensidad global de la penalización de regularización (`L2` puro o mezcla de `L1` y `L2`).
# MAGIC * **`elastic_net_param`**: Balance de la penalización `Elastic Net` (mezcla entre `Ridge` `0.0` y `Lasso` `1.0`).
# MAGIC * **`max_iter`**: Número máximo de iteraciones permitidas para el optimizador `L-BFGS`.
# MAGIC * **`family`**: Tipo de modelo (`binomial` especifica regresión logística binaria).
# MAGIC * **`standardization`**: Estandarización interna del clasificador (fijado a `False` porque el *pipeline* ya incluye un `StandardScaler`).
# MAGIC * **`threshold`**: Umbral de decisión por defecto.

# COMMAND ----------

dbutils.widgets.text("reg_param", "0.01")
dbutils.widgets.text("elastic_net_param", "0.0")
dbutils.widgets.text("max_iter", "100")
dbutils.widgets.text("family", "binomial")
dbutils.widgets.text("standardization", "False")
dbutils.widgets.text("threshold", "0.5")

reg_param = float(dbutils.widgets.get("reg_param"))
elastic_net_param = float(dbutils.widgets.get("elastic_net_param"))
max_iter = int(dbutils.widgets.get("max_iter"))
family = dbutils.widgets.get("family")
standardization = dbutils.widgets.get("standardization").lower() == "true"
threshold = float(dbutils.widgets.get("threshold"))

# Run tag: Unique visual identifier for this grid point
run_tag = f"lr__rp{reg_param}__en{elastic_net_param}__seed{seed}"

print(f"Regularization hyperparameter: {reg_param}")
print(f"Elastic net hyperparameter: {elastic_net_param}")
print(f"Maximum iterations: {max_iter}")
print(f"Family: {family}")
print(f"Standardization: {standardization}")
print(f"Threshold: {threshold}")
print(f"Run tag: {run_tag}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2.3. Parámetros de orquestación y modo de entrenamiento
# MAGIC
# MAGIC Para soportar el ciclo de vida completo de `MLOps`, la libreta acepta un parámetro `training_mode` que determina qué particiones temporales de datos se utilizarán para ajustar el modelo. Dependiendo de la fase del proyecto orquestada, inyectaremos un modo distinto:
# MAGIC
# MAGIC * **`train` (fase de experimentación)**: El modelo se entrena exclusivamente con el conjunto de entrenamiento. Es el modo por defecto y se utiliza para iterar sobre múltiples hiperparámetros y validarlos de forma justa contra el conjunto de validación.
# MAGIC * **`train_val` (fase de evaluación del `challenger`)**: Fusiona los conjuntos de entrenamiento y validación. Se invoca una vez que los mejores hiperparámetros ya han sido fijados. Permite que el modelo consolide su aprendizaje con más información histórica antes de someterlo al examen final e imparcial contra el conjunto de prueba.
# MAGIC * **`train_val_test` (fase de despliegue del `champion`)**: Utiliza el histórico completo de datos disponible. Se ejecuta únicamente cuando el `challenger` ha ganado la evaluación y va a ser desplegado a producción. Este paso de *refit* final garantiza que el sistema en producción haya aprendido de las tácticas de fraude más recientes registradas, mitigando el *concept drift*.

# COMMAND ----------

dbutils.widgets.text("training_mode", "train")

training_mode = dbutils.widgets.get("training_mode")

print(f"Training mode: {training_mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 3. Obtención del conjunto de datos de entrenamiento
# MAGIC
# MAGIC En el *script* `07_Utils.py` los `DataFrame` correspondientes a las particiones temporales (`train_df`, `validation_df`, `test_df`) y al histórico completo (`df_raw`) ya se encuentran instanciados en memoria. 
# MAGIC
# MAGIC El propósito de esta sección es juntar el conjunto de datos definitivo (`training_data`) que consumirá el algoritmo, dictado por el parámetro `training_mode` inyectado desde la libreta orquestadora:
# MAGIC
# MAGIC * **Fusión dinámica**: Dependiendo de la fase de ciclo de vida del modelo (búsqueda de hiperparámetros, evaluación de candidato o paso a producción), uniremos las particiones correspondientes para aprovechar la cantidad óptima de información histórica.
# MAGIC * **Pesaje de clases adaptativo**: Dado que la proporción de transacciones fraudulentas frente a las legítimas puede variar al añadir meses enteros de validación o prueba al conjunto de entrenamiento, recalculamos dinámicamente los pesos inversos de frecuencia utilizando la función utilitaria `apply_class_weights()`. Esto garantiza que la función de pérdida del algoritmo mantenga un balance matemático perfecto independientemente de la partición ensamblada.

# COMMAND ----------

if training_mode == "train":
    mode_description = "Using the strict temporal training partition."
    training_data = train_df
elif training_mode == "train_val":
    mode_description = "Merging validation and training datasets."
    training_data = train_df.unionByName(validation_df)
else:
    mode_description = "Using the complete historical dataset."
    training_data = train_df.unionByName(validation_df).unionByName(test_df)

train_weighted = apply_class_weights(training_data)

print(f"Mode {training_mode}: {mode_description}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 4. Construcción del *pipeline* de preprocesado
# MAGIC
# MAGIC La función `build_preprocessing_stages` recibe todos los hiperparámetros de preprocesado como **parámetros explícitos** de la función, en lugar de leerlos pasivamente desde variables globales del entorno.
# MAGIC
# MAGIC Este enfoque arquitectónico ofrece dos grandes ventajas:
# MAGIC
# MAGIC 1. **Trazabilidad estricta**: Queda documentado y explícito en la firma de la función qué valores controlan el comportamiento de cada etapa, sin necesidad de buscar dónde se definieron las variables en otras libretas.
# MAGIC 2. **Seguridad frente a mutaciones**: Si `07_Utils.py` modificara accidentalmente alguna variable entre llamadas durante un flujo complejo, los hiperparámetros de nuestro entrenamiento seguirían estando protegidos al ser inyectados por valor.
# MAGIC
# MAGIC > **Nota sobre variables estructurales**: Las variables que definen la estructura inmutable del *pipeline* (como `imputer_input_columns`, las sentencias `SQL` estáticas, etc.) sí continúan leyéndose del espacio de nombres heredado de `07_Utils.py`, ya que son una consecuencia directa y estática del esquema del conjunto de datos, no hiperparámetros sujetos a ajuste u optimización.

# COMMAND ----------

def build_preprocessing_stages(
    imputer_strategy,
    var_selector_threshold,
    scaler_with_mean,
    scaler_with_std,
    ohe_drop_last,
    si_handle_invalid,
    si_order_type,
    ohe_handle_invalid,
    asm_handle_invalid
):
    """
    Return a fresh list of preprocessing stages for one pipeline run.

    Creates new `Estimator` instances on every call so that the fitted state
    (vocabularies, medians, scales, etc.) from a previous `Pipeline.fit()`
    never leaks into the next run.

    Structural column lists and `SQL` statements are read from the `07_Utils.py`
    namespace. Hyperparameter values come exclusively from the function parameters.
    """
    # 1. Imputation for nullable numeric columns
    imputer = Imputer(
        inputCols = imputer_input_columns,
        outputCols = imputer_output_columns,
        strategy = imputer_strategy
    )

    # 2. Boolean flags cast to DOUBLE with inline null imputation via COALESCE
    boolean_transformer = SQLTransformer(statement = boolean_statement)

    # 3. Feature engineering from current-transaction fields only
    feature_engineer = SQLTransformer(statement = feature_engineering_statement)

    # 4. Learn category vocabularies
    string_indexer = StringIndexer(
        inputCols = string_indexer_input_columns,
        outputCols = string_indexer_output_columns,
        handleInvalid = si_handle_invalid,
        stringOrderType = si_order_type
    )

    # 5. One-hot encoding
    ohe = OneHotEncoder(
        inputCols = ohe_input_columns,
        outputCols = ohe_output_columns,
        handleInvalid = ohe_handle_invalid,
        dropLast = ohe_drop_last
    )

    # 6. Assemble all feature columns into a single dense or sparse vector
    assembler = VectorAssembler(
        inputCols = assembler_input_columns,
        outputCol = assembler_output_column,
        handleInvalid = asm_handle_invalid
    )

    # 7. Remove quasi-constant features before scaling
    var_selector = VarianceThresholdSelector(
        featuresCol = var_selector_input_column,
        outputCol = var_selector_output_column,
        varianceThreshold = var_selector_threshold
    )

    # 8. Normalize to unit variance, preserving sparsity
    standard_scaler = StandardScaler(
        inputCol = scaler_input_column,
        outputCol = scaler_output_column,
        withMean = scaler_with_mean,
        withStd = scaler_with_std
    )

    preprocessing_stages = [
        imputer,
        boolean_transformer,
        feature_engineer,
        string_indexer,
        ohe,
        assembler,
        var_selector,
        standard_scaler
    ]

    return preprocessing_stages

# COMMAND ----------

# Sanity check: list stage types to confirm the pipeline is wired correctly
_stages = build_preprocessing_stages(
    imputer_strategy = imputer_strategy,
    var_selector_threshold = var_selector_threshold,
    scaler_with_mean = scaler_with_mean,
    scaler_with_std = scaler_with_std,
    ohe_drop_last = ohe_drop_last,
    si_handle_invalid = si_handle_invalid,
    si_order_type = si_order_type,
    ohe_handle_invalid = ohe_handle_invalid,
    asm_handle_invalid = asm_handle_invalid
)

for i, stage in enumerate(_stages, 1):
    print(f"{i}. {type(stage).__name__}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 5. Construcción y entrenamiento del *pipeline* completo
# MAGIC
# MAGIC En esta fase, juntamos las transformaciones y el algoritmo final en un único flujo de trabajo y procedemos a su entrenamiento. Creamos instancias **completamente nuevas** de todas las etapas de preprocesado llamando a nuestra función `build_preprocessing_stages` e inyectándole los hiperparámetros recibidos por los *widgets*. Esto garantiza matemáticamente que el estado ajustado de una ejecución anterior (vocabularios, varianzas, medias, etc.) no contamine la iteración actual.

# COMMAND ----------

preprocessing_stages = build_preprocessing_stages(
    imputer_strategy = imputer_strategy,
    var_selector_threshold = var_selector_threshold,
    scaler_with_mean = scaler_with_mean,
    scaler_with_std = scaler_with_std,
    ohe_drop_last = ohe_drop_last,
    si_handle_invalid = si_handle_invalid,
    si_order_type = si_order_type,
    ohe_handle_invalid = ohe_handle_invalid,
    asm_handle_invalid = asm_handle_invalid
)

lr_clf = LogisticRegression(
    featuresCol = features_column,
    labelCol = label_column,
    weightCol = class_weight_column,
    maxIter = max_iter,
    regParam = reg_param,
    elasticNetParam = elastic_net_param,
    family = family,
    standardization = standardization,
    threshold = threshold
)

pipeline_stages = preprocessing_stages + [lr_clf]
full_pipeline = Pipeline(stages = pipeline_stages)

pipeline_model = full_pipeline.fit(train_weighted)

lr_fitted = pipeline_model.stages[-1]

print("Pipeline fitted successfully.")
print(f"Total iterations: {lr_fitted.summary.totalIterations}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 6. Serialización del *pipeline* entrenado
# MAGIC
# MAGIC El `PipelineModel` completo se escribe físicamente en el volumen de `Unity Catalog` en formato nativo de `Spark ML`.

# COMMAND ----------

run_tmp_path = str(Path(uc_volume_path) / "runs" / run_tag)
model_save_path = str(Path(run_tmp_path) / "pipeline_model")
dbutils.fs.mkdirs(model_save_path)

pipeline_model.write().overwrite().save(model_save_path)

print(f"Pipeline model successfully saved to {model_save_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 7. Interpretabilidad: Extracción y exportación de coeficientes
# MAGIC
# MAGIC Para comprender cómo el modelo toma sus decisiones, extraemos los pesos (coeficientes) que la regresión logística ha asignado a cada característica. 
# MAGIC
# MAGIC Dado que nuestro *pipeline* incluye etapas que modifican dinámicamente el esquema de los datos (como la expansión de vectores de `OneHotEncoder` y el filtrado de `VarianceThresholdSelector`), utilizamos la función `extract_feature_names` para garantizar que obtenemos los nombres correctos y definitivos de las variables.
# MAGIC

# COMMAND ----------

expanded_feature_names, selected_feature_names = extract_feature_names(pipeline_model, train_weighted)

lr_coefficients = lr_fitted.coefficients.toArray().tolist()

print(f"Assembler input columns ({len(expanded_feature_names)}): {expanded_feature_names}")
print(f"Selected features ({len(selected_feature_names)}): {selected_feature_names}")
print(f"Coefficients ({len(lr_coefficients)}): {lr_coefficients}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 7.1. Gráfica de impacto de características
# MAGIC
# MAGIC Generamos y guardamos una figura que visualiza los coeficientes más importantes. Esta representación permite identificar de un vistazo, mediante un código de colores, qué variables empujan la decisión del algoritmo hacia el fraude y cuáles hacia una transacción legítima.

# COMMAND ----------

figures_local_path = str(Path(run_tmp_path) / "figures")
dbutils.fs.mkdirs(figures_local_path)

save_diagnostic_figure(
    fig_lr_coefficients(lr_coefficients, selected_feature_names, f"Coefficients — {run_tag}"),
    figures_local_path,
    "lr_coefficients.png"
)

print(f"Diagnostic figure successfully saved to {str(Path(figures_local_path) / 'lr_coefficients.png')}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 7.2. Coeficientes como `.csv`
# MAGIC
# MAGIC Se exportan los coeficientes ordenados por valor absoluto. Este artefacto complementa la figura de barras con un registro tabular que facilita la interpretabilidad del modelo.

# COMMAND ----------

coef_df = pd.DataFrame({
    "feature": selected_feature_names,
    "coefficient": lr_coefficients
})

coef_df_sorted = (
    coef_df
    .assign(abs_coef = lambda df: df["coefficient"].abs())
    .sort_values(by = "abs_coef", ascending = False)
    .drop(columns = ["abs_coef"])
)

coefficients_csv_path = str(Path(run_tmp_path) / "lr_coefficients.csv")
coef_df_sorted.to_csv(coefficients_csv_path, index = False)

print(f"Logistic regression coefficients successfully saved to {coefficients_csv_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 8. Ejemplos de entrada y salida para la firma del modelo
# MAGIC
# MAGIC Se guardan como archivos `parquet` en el volumen de `Unity Catalog` para inferir correctamente el esquema de entrada y salida (la "firma") del modelo antes de registrarlo.

# COMMAND ----------

signature_sample_size = 5
transform_buffer_size = 300

input_example_pandas_df = train_weighted.limit(signature_sample_size).toPandas()

sample_predictions = pipeline_model.transform(train_weighted.limit(transform_buffer_size))
output_example_pandas_df = (
    to_pandas_predictions(sample_predictions)[[prob_fraud_column, prediction_column]].head(signature_sample_size)
)

# Clean up residual Spark Connect metadata to avoid serialization issues
clean_input_df = pd.DataFrame(input_example_pandas_df.to_dict("list"))
clean_output_df = pd.DataFrame(output_example_pandas_df.to_dict("list"))

input_example_path = str(Path(run_tmp_path) / "input_example.parquet")
output_example_path = str(Path(run_tmp_path) / "output_example.parquet")

clean_input_df.to_parquet(input_example_path, index = False)
clean_output_df.to_parquet(output_example_path, index = False)

print(f"Input examples successfully saved to {input_example_path}")
print(f"Output examples successfully saved to {output_example_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 9. Metadatos de convergencia del optimizador
# MAGIC
# MAGIC El atributo `objectiveHistory` contiene el valor de la función de pérdida (*loss function*) calculado al final de cada iteración por el optimizador matemático (habitualmente `L-BFGS`).
# MAGIC
# MAGIC Extraemos este historial como una lista nativa de `Python` para auditar si el modelo convergió de forma natural o si se detuvo prematuramente por alcanzar el límite máximo de iteraciones (`max_iter`).

# COMMAND ----------

has_history = (
    hasattr(lr_fitted, "summary")
    and hasattr(lr_fitted.summary, "objectiveHistory")
)

objective_history = list(lr_fitted.summary.objectiveHistory) if has_history else []
total_iterations = int(lr_fitted.summary.totalIterations) if has_history else max_iter

convergence_metadata = {
    "objective_history": objective_history,
    "total_iterations": total_iterations,
    "converged": float(len(objective_history) < max_iter) if has_history else 0.0,
    "lr_intercept": float(lr_fitted.intercept)
}

if has_history:
    print(f"Initial loss: {objective_history[0]:.6f}")
    print(f"Final loss: {objective_history[-1]:.6f}")
    print(f"Converged: {bool(convergence_metadata['converged'])}")
else:
    print("Convergence history not available.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 10. Liberación de memoria y retorno del resultado
# MAGIC
# MAGIC Como paso final, eliminamos explícitamente los objetos de `Spark` de mayor tamaño en memoria (conjunto de datos de entrenamiento, *pipeline* y el modelo) y forzamos la recolección de basura nativa de `Python` (`gc.collect()`). Esta práctica defensiva reduce drásticamente el riesgo de que la caché del clúster se sature antes de que termine la sesión.
# MAGIC
# MAGIC Finalmente, utilizamos `dbutils.notebook.exit` para devolver el control a la libreta orquestadora. Puesto que esta función solo admite cadenas de texto, empaquetamos todos los hiperparámetros y rutas de artefactos en un diccionario y lo serializamos a formato `.json`.

# COMMAND ----------

del training_data, pipeline_model, full_pipeline, lr_clf, preprocessing_stages, lr_fitted
gc.collect()

result = {
    "run_tag": run_tag,
    "reg_param": reg_param,
    "elastic_net_param": elastic_net_param,
    "max_iter": max_iter,
    "family": family,
    "standardization": standardization,
    "threshold": threshold,
    "imputer_strategy": imputer_strategy,
    "var_selector_threshold": var_selector_threshold,
    "scaler_with_mean": scaler_with_mean,
    "scaler_with_std": scaler_with_std,
    "ohe_drop_last": ohe_drop_last,
    "si_handle_invalid": si_handle_invalid,
    "si_order_type": si_order_type,
    "ohe_handle_invalid": ohe_handle_invalid,
    "asm_handle_invalid": asm_handle_invalid,
    "training_mode": training_mode,
    "model_save_path": model_save_path,
    "figures_local_path": figures_local_path,
    "coefficients_csv_path": coefficients_csv_path,
    "input_example_path": input_example_path,
    "output_example_path": output_example_path,
    "convergence_metadata": convergence_metadata
}

print(f"Exiting notebook and returning results for run: {run_tag}")
dbutils.notebook.exit(json.dumps(result))