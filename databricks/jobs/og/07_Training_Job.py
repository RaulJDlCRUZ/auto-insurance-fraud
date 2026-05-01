# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Entrenamiento de una combinación de hiperparámetros
# MAGIC
# MAGIC **Autor**: Juan Carlos Alfaro Jiménez
# MAGIC
# MAGIC Esta libreta **no contiene ninguna llamada a `MLflow`**. Su única responsabilidad es aislar el entrenamiento de un único pipeline de `scikit-learn` utilizando los hiperparámetros recibidos. Durante su ejecución independiente, ajustará el modelo a los datos y guardará el artefacto físico resultante directamente en un volumen de `Unity Catalog`.
# MAGIC
# MAGIC ### ¿Por qué usar `scikit-learn` en lugar de `Spark MLlib`?
# MAGIC
# MAGIC Databricks Serverless ejecuta las sesiones de notebook sobre `Spark Connect`, que impone una lista blanca estricta (`WhitelistingPy4JSecurityManager`) sobre los constructores `Java` accesibles desde el driver de `Python`. Todos los constructores de `pyspark.ml` (incluyendo `Imputer`, `StringIndexer`, `VectorAssembler`, `LogisticRegression`, etc.) están bloqueados en este contexto, haciendo imposible construir un `Pipeline` de `MLlib` de forma nativa.
# MAGIC
# MAGIC `scikit-learn` corre íntegramente en el driver de `Python`, sin depender de la `JVM`, por lo que no está sujeto a estas restricciones. El conjunto de entrenamiento se materializa desde `Spark` a `pandas` antes del ajuste, operación que es segura dado el límite de 200.000 filas aplicado en `07_Utils.py`.
# MAGIC
# MAGIC ### ¿Por qué desacoplar el entrenamiento de la orquestadora?
# MAGIC
# MAGIC El aislamiento en sesiones independientes vía `dbutils.notebook.run()` sigue siendo necesario para garantizar que el estado del pipeline ajustado (vocabularios, estadísticas de escalado, etc.) no se acumule en memoria entre iteraciones del grid search.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. Importaciones y carga de utilidades compartidas

# COMMAND ----------

exec(open("07_Utils.py").read(), globals())

# COMMAND ----------

import gc
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from sklearn.pipeline import Pipeline as SklearnPipeline
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.feature_selection import VarianceThreshold

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2. Recepción de parámetros
# MAGIC
# MAGIC Los *widgets* son el mecanismo estándar en `Databricks` para parametrizar y pasar información dinámicamente entre libretas. La libreta orquestadora inyecta todos estos valores simultáneamente utilizando el parámetro `arguments` de `dbutils.notebook.run()`.
# MAGIC
# MAGIC > **Importante sobre el tipado**: Todos los valores recibidos a través de los *widgets* llegan **siempre como cadenas de texto** (`String`). Es imprescindible convertirlos explícitamente a su tipo de dato correspondiente antes de pasarlos al modelo o al *pipeline*.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2.1. Hiperparámetros del preprocesado

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
# MAGIC * **`reg_param`**: Intensidad de la regularización. En `scikit-learn`, `LogisticRegression` usa el parámetro `C = 1 / reg_param`.
# MAGIC * **`elastic_net_param`**: Balance `Elastic Net` (0.0 = Ridge, 1.0 = Lasso). Requiere `solver = 'saga'`.
# MAGIC * **`max_iter`**: Número máximo de iteraciones del optimizador.
# MAGIC * **`family`**: Ignorado en sklearn (siempre binomial para clasificación binaria).
# MAGIC * **`standardization`**: Ignorado (el pipeline ya incluye `StandardScaler`).
# MAGIC * **`threshold`**: Umbral de decisión; se aplica en postproceso sobre `predict_proba`.

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

# C is the inverse of regularization strength in scikit-learn
lr_C = 1.0 / reg_param if reg_param > 0 else 1e6

# l1_ratio maps elastic_net_param directly (0.0 = L2 / Ridge, 1.0 = L1 / Lasso)
# penalty = 'elasticnet' requires solver = 'saga'
lr_penalty = "elasticnet"
lr_solver = "saga"

run_tag = f"lr__rp{reg_param}__en{elastic_net_param}__seed{seed}"

print(f"Regularization parameter (reg_param): {reg_param} → C = {lr_C}")
print(f"Elastic net parameter: {elastic_net_param}")
print(f"Maximum iterations: {max_iter}")
print(f"Penalty: {lr_penalty}, Solver: {lr_solver}")
print(f"Threshold: {threshold}")
print(f"Run tag: {run_tag}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2.3. Parámetros de orquestación y modo de entrenamiento
# MAGIC
# MAGIC * **`train`**: Entrena con el conjunto de entrenamiento. Modo por defecto para el grid search.
# MAGIC * **`train_val`**: Fusiona entrenamiento y validación. Se usa al reentrenar con los mejores hiperparámetros.
# MAGIC * **`train_val_test`**: Usa el histórico completo. Solo para el modelo final a producción.

# COMMAND ----------

dbutils.widgets.text("training_mode", "train")
training_mode = dbutils.widgets.get("training_mode")
print(f"Training mode: {training_mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 3. Obtención y materialización del conjunto de entrenamiento
# MAGIC
# MAGIC Las particiones `train_df`, `validation_df` y `test_df` ya están instanciadas desde `07_Utils.py`.
# MAGIC Se selecciona la partición correspondiente al modo de entrenamiento, se aplican los pesos de clase y
# MAGIC se materializa a `pandas` para el ajuste con `scikit-learn`.
# MAGIC
# MAGIC El límite de `to_pandas_max_rows` (200.000 filas) definido en `07_Utils.py` actúa como barrera de
# MAGIC seguridad para evitar desbordamientos de memoria en el driver. Para `train_val` y `train_val_test`
# MAGIC se aplica el mismo límite proporcional al tamaño de la partición combinada.

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

# Materialize to pandas for scikit-learn.
# apply_class_weights already computed n_fraud and n_legit; weights are in class_weight_column.
# We sample up to to_pandas_max_rows rows maintaining class proportions via stratified sampling.
train_pandas = (
    train_weighted
    .limit(to_pandas_max_rows)
    .toPandas()
)

print(f"Training rows materialized to pandas: {len(train_pandas):,}")
print(f"Columns: {len(train_pandas.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 4. Construcción del pipeline de preprocesado y clasificador
# MAGIC
# MAGIC El pipeline de `scikit-learn` replica la misma arquitectura lógica del pipeline original de `Spark MLlib`:
# MAGIC
# MAGIC 1. **`ColumnTransformer`**: Aplica en paralelo tres ramas de transformación:
# MAGIC    - *Rama numérica*: `SimpleImputer` (mediana) + `StandardScaler` sobre las columnas numéricas con nulos.
# MAGIC    - *Rama de conteos*: `SimpleImputer` (constante 0) sobre columnas de agregación cuyo vacío semántico es ausencia de actividad.
# MAGIC    - *Rama categórica*: `SimpleImputer` (constante `"__missing__"`) + `OneHotEncoder` sobre columnas de cadena.
# MAGIC 2. **`VarianceThreshold`**: Elimina características con varianza inferior al umbral configurado.
# MAGIC 3. **`LogisticRegression`**: Clasificador binario con regularización `Elastic Net` y `solver = saga`.
# MAGIC
# MAGIC Las columnas binarias (`boolean_columns`) se tratan como numéricas sin imputación adicional, ya que `apply_class_weights` garantiza que `COALESCE` las ha llenado a 0.0 en la capa `Gold`.

# COMMAND ----------

def build_sklearn_pipeline(
    imputer_strategy,
    var_selector_threshold,
    scaler_with_mean,
    scaler_with_std,
    ohe_drop_last,
    ohe_handle_invalid,
    lr_C,
    lr_penalty,
    lr_solver,
    elastic_net_param,
    max_iter
):
    """
    Build a fresh scikit-learn Pipeline equivalent to the original Spark MLlib pipeline.

    The ColumnTransformer handles three column groups in parallel:
    - profile_numeric_columns + agg_null_columns: median imputation + standard scaling
    - agg_zero_columns + boolean_columns: constant-0 imputation (no scaling needed,
      already in [0,1] or small integer range and handled by the scaler downstream)
    - categorical_columns: missing-value imputation + one-hot encoding

    The engineered features (claimed_amount_log, late_report_flag, multi_party_flag)
    are computed inline via pandas before fitting, as sklearn has no SQLTransformer.

    Returns the unfitted pipeline.
    """
    # All numeric columns that need median imputation
    numeric_impute_cols = profile_numeric_columns + agg_null_columns

    # Numeric columns that need zero imputation (counts/sums on empty windows)
    numeric_zero_cols = agg_zero_columns

    # Boolean flag columns: already 0/1 integers from Gold layer, no imputation needed
    boolean_cols = boolean_columns

    # Categorical columns for OHE
    cat_cols = categorical_columns

    # Numeric pipeline: median imputation → standard scaling
    numeric_transformer = SklearnPipeline(steps=[
        ("imputer", SimpleImputer(strategy=imputer_strategy)),
        ("scaler", StandardScaler(with_mean=scaler_with_mean, with_std=scaler_with_std)),
    ])

    # Zero-fill pipeline: constant 0 imputation → standard scaling
    zero_transformer = SklearnPipeline(steps=[
        ("imputer", SimpleImputer(strategy="constant", fill_value=0.0)),
        ("scaler", StandardScaler(with_mean=scaler_with_mean, with_std=scaler_with_std)),
    ])

    # Boolean pipeline: pass-through with zero fill for any residual nulls → scaling
    boolean_transformer = SklearnPipeline(steps=[
        ("imputer", SimpleImputer(strategy="constant", fill_value=0.0)),
        ("scaler", StandardScaler(with_mean=scaler_with_mean, with_std=scaler_with_std)),
    ])

    # Categorical pipeline: fill missing → OHE
    # handle_unknown maps to ohe_handle_invalid: "keep" → "ignore" (sklearn equivalent)
    ohe_handle = "ignore" if ohe_handle_invalid == "keep" else "error"
    categorical_transformer = SklearnPipeline(steps=[
        ("imputer", SimpleImputer(strategy="constant", fill_value="__missing__")),
        ("ohe", OneHotEncoder(
            # drop="last" if ohe_drop_last else None,
            drop="first" if ohe_drop_last else None,
            handle_unknown=ohe_handle,
            sparse_output=True
        )),
    ])

    # Engineered feature columns (computed before fit via _add_engineered_features)
    engineered_cols = engineered_columns

    preprocessor = ColumnTransformer(
        transformers=[
            ("num_impute", numeric_transformer, numeric_impute_cols),
            ("num_zero", zero_transformer, numeric_zero_cols),
            ("bool", boolean_transformer, boolean_cols),
            ("cat", categorical_transformer, cat_cols),
            ("eng", StandardScaler(with_mean=scaler_with_mean, with_std=scaler_with_std), engineered_cols),
        ],
        remainder="drop",
        verbose_feature_names_out=True
    )

    pipeline = SklearnPipeline(steps=[
        ("preprocessor", preprocessor),
        ("var_selector", VarianceThreshold(threshold=var_selector_threshold)),
        ("classifier", LogisticRegression(
            C=lr_C,
            penalty=lr_penalty,
            l1_ratio=elastic_net_param,
            solver=lr_solver,
            max_iter=max_iter,
            random_state=seed,
            class_weight=None,  # Class weights handled via sample_weight parameter in fit()
            n_jobs=-1
        )),
    ])

    return pipeline


def _add_engineered_features(df):
    """
    Compute the three engineered features that in the original Spark pipeline
    were produced by a SQLTransformer. Operates on a pandas DataFrame in-place.

    - claimed_amount_log: log(claimed_amount_eur + 1), robust to zero amounts.
    - late_report_flag: 1 if days_to_report > 7, else 0.
    - multi_party_flag: 1 if n_parties_involved > 2, else 0.
    """
    df = df.copy()
    df["claimed_amount_log"] = np.log1p(df["claimed_amount_eur"].fillna(0.0))
    df["late_report_flag"] = (df["days_to_report"].fillna(0) > 7).astype(int)
    df["multi_party_flag"] = (df["n_parties_involved"].fillna(0) > 2).astype(int)
    return df

# COMMAND ----------

# Sanity check: verify all expected column groups are populated from Utils namespace
print(f"Profile numeric columns ({len(profile_numeric_columns)}): {profile_numeric_columns}")
print(f"Aggregation null columns ({len(agg_null_columns)}): {agg_null_columns}")
print(f"Aggregation zero columns ({len(agg_zero_columns)}): {agg_zero_columns}")
print(f"Boolean columns ({len(boolean_columns)}): {boolean_columns}")
print(f"Categorical columns ({len(categorical_columns)}): {categorical_columns}")
print(f"Engineered columns ({len(engineered_columns)}): {engineered_columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 5. Entrenamiento del pipeline

# COMMAND ----------

# Add engineered features to the pandas training set
train_pandas_eng = _add_engineered_features(train_pandas)

# Extract feature matrix, labels and sample weights
X_train = train_pandas_eng.drop(columns=[label_column, claim_id_column, policy_id_column, date_column, class_weight_column], errors="ignore")
y_train = train_pandas_eng[label_column].astype(int)
sample_weights = train_pandas_eng[class_weight_column].values

# Build and fit the pipeline
full_pipeline = build_sklearn_pipeline(
    imputer_strategy=imputer_strategy,
    var_selector_threshold=var_selector_threshold,
    scaler_with_mean=scaler_with_mean,
    scaler_with_std=scaler_with_std,
    ohe_drop_last=ohe_drop_last,
    ohe_handle_invalid=ohe_handle_invalid,
    lr_C=lr_C,
    lr_penalty=lr_penalty,
    lr_solver=lr_solver,
    elastic_net_param=elastic_net_param,
    max_iter=max_iter
)

pipeline_model = full_pipeline.fit(X_train, y_train, classifier__sample_weight=sample_weights)

lr_fitted = pipeline_model.named_steps["classifier"]

print("Pipeline fitted successfully.")
print(f"Total iterations: {lr_fitted.n_iter_[0]}")
print(f"Converged: {lr_fitted.n_iter_[0] < max_iter}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 6. Serialización del pipeline entrenado
# MAGIC
# MAGIC El pipeline se serializa con `joblib` al volumen de `Unity Catalog`. `joblib` es el formato
# MAGIC estándar para persistir modelos de `scikit-learn` y es el que `mlflow.sklearn` usa internamente.

# COMMAND ----------

run_tmp_path = str(Path(uc_volume_path) / "runs" / run_tag)
model_save_path = str(Path(run_tmp_path) / "pipeline_model.joblib")
dbutils.fs.mkdirs(run_tmp_path)

joblib.dump(pipeline_model, model_save_path)

print(f"Pipeline model successfully saved to {model_save_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 7. Interpretabilidad: extracción y exportación de coeficientes

# COMMAND ----------

def extract_feature_names_sklearn(pipeline_model):
    """
    Extract the final feature names after ColumnTransformer and VarianceThreshold.

    Returns:
        expanded_feature_names: all feature names after preprocessing (before variance filter)
        selected_feature_names: feature names that survived the VarianceThreshold step
    """
    preprocessor = pipeline_model.named_steps["preprocessor"]
    var_selector = pipeline_model.named_steps["var_selector"]

    expanded_feature_names = list(preprocessor.get_feature_names_out())

    selected_mask = var_selector.get_support()
    selected_feature_names = [
        name for name, kept in zip(expanded_feature_names, selected_mask) if kept
    ]

    return expanded_feature_names, selected_feature_names


expanded_feature_names, selected_feature_names = extract_feature_names_sklearn(pipeline_model)
lr_coefficients = lr_fitted.coef_[0].tolist()

print(f"Assembler input features ({len(expanded_feature_names)}): {expanded_feature_names[:10]} ...")
print(f"Selected features ({len(selected_feature_names)}): {selected_feature_names[:10]} ...")
print(f"Coefficients ({len(lr_coefficients)})")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 7.1. Gráfica de impacto de características

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

# COMMAND ----------

coef_df = pd.DataFrame({
    "feature": selected_feature_names,
    "coefficient": lr_coefficients
})

coef_df_sorted = (
    coef_df
    .assign(abs_coef=lambda df: df["coefficient"].abs())
    .sort_values(by="abs_coef", ascending=False)
    .drop(columns=["abs_coef"])
)

coefficients_csv_path = str(Path(run_tmp_path) / "lr_coefficients.csv")
coef_df_sorted.to_csv(coefficients_csv_path, index=False)

print(f"Logistic regression coefficients successfully saved to {coefficients_csv_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 8. Ejemplos de entrada y salida para la firma del modelo

# COMMAND ----------

signature_sample_size = 5

# Input example: raw feature rows before any transformation
input_example_pandas_df = train_pandas_eng.head(signature_sample_size).drop(
    columns=[label_column, class_weight_column], errors="ignore"
)

# Output example: prob_fraud and prediction columns
X_sample = input_example_pandas_df.drop(
    columns=[claim_id_column, policy_id_column, date_column], errors="ignore"
)
prob_sample = pipeline_model.predict_proba(X_sample)[:, 1]
pred_sample = (prob_sample >= threshold).astype(int)

output_example_pandas_df = pd.DataFrame({
    prob_fraud_column: prob_sample,
    prediction_column: pred_sample.astype(float)
})

# Clean up any residual metadata before saving as parquet
clean_input_df = pd.DataFrame(input_example_pandas_df.to_dict("list"))
clean_output_df = pd.DataFrame(output_example_pandas_df.to_dict("list"))

input_example_path = str(Path(run_tmp_path) / "input_example.parquet")
output_example_path = str(Path(run_tmp_path) / "output_example.parquet")

clean_input_df.to_parquet(input_example_path, index=False)
clean_output_df.to_parquet(output_example_path, index=False)

print(f"Input examples successfully saved to {input_example_path}")
print(f"Output examples successfully saved to {output_example_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 9. Metadatos de convergencia del optimizador
# MAGIC
# MAGIC `scikit-learn` expone `n_iter_` en lugar de `objectiveHistory`. No hay una curva de pérdida
# MAGIC por iteración disponible sin instrumentación adicional del solver, así que se reporta el
# MAGIC número total de iteraciones y si el modelo convergió antes del límite.

# COMMAND ----------

total_iterations = int(lr_fitted.n_iter_[0])
converged = total_iterations < max_iter

convergence_metadata = {
    "objective_history": [],   # Not available in sklearn without callback instrumentation
    "total_iterations": total_iterations,
    "converged": float(converged),
    "lr_intercept": float(lr_fitted.intercept_[0])
}

print(f"Total iterations: {total_iterations}")
print(f"Converged: {converged}")
print(f"Intercept: {lr_fitted.intercept_[0]:.6f}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 10. Liberación de memoria y retorno del resultado

# COMMAND ----------

del train_weighted, pipeline_model, full_pipeline, lr_fitted
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