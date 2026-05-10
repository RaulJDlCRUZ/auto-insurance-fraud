# Databricks notebook source
# MAGIC %md
# MAGIC # Restauración de aliases previa al relanzamiento de `08_Production`
# MAGIC
# MAGIC Este notebook auxiliar restaura el estado de aliases en Unity Catalog
# MAGIC al punto exacto anterior a la primera ejecución fallida de `08_Production`,
# MAGIC dejando el modelo listo para un relanzamiento limpio.
# MAGIC
# MAGIC Esto es debido a que en el bloque 11 de `08_Production`, no se está haciendo el paso de imputar a 0.0 las columnas de `VectorAssembler`, lo cual arroja en un error.
# MAGIC
# MAGIC **Ejecutar una sola vez, justo antes de relanzar `08_Production`.**

# COMMAND ----------

from mlflow import MlflowClient

client = MlflowClient(registry_uri="databricks-uc")

UC_MODEL_NAME    = "workspace.auto_insurance_fraud.fraud_lr_pipeline"
CANDIDATE_VERSION = 6
CHAMPION_VERSION  = 8   # versión registrada en el refit fallido; se limpia su alias

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Restaurar alias `candidate` en la versión 6

# COMMAND ----------

# Quitar 'challenger' de la versión 6 si existe
try:
    v = client.get_model_version_by_alias(UC_MODEL_NAME, "challenger")
    if int(v.version) == CANDIDATE_VERSION:
        client.delete_registered_model_alias(UC_MODEL_NAME, "challenger")
        print(f"Alias 'challenger' eliminado de la versión {CANDIDATE_VERSION}.")
    else:
        print(f"Alias 'challenger' apunta a la versión {v.version}, no a {CANDIDATE_VERSION}. Revisa manualmente.")
except Exception:
    print("Alias 'challenger' no encontrado. No hay nada que eliminar.")

# Asignar 'candidate' a la versión 6
client.set_registered_model_alias(UC_MODEL_NAME, "candidate", CANDIDATE_VERSION)
print(f"Alias 'candidate' asignado a la versión {CANDIDATE_VERSION}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Limpiar alias `champion` de la versión 7

# COMMAND ----------

# Quitar 'champion' de la versión 7 si existe
try:
    v = client.get_model_version_by_alias(UC_MODEL_NAME, "champion")
    if int(v.version) == CHAMPION_VERSION:
        client.delete_registered_model_alias(UC_MODEL_NAME, "champion")
        print(f"Alias 'champion' eliminado de la versión {CHAMPION_VERSION}.")
    else:
        print(f"Alias 'champion' apunta a la versión {v.version}, no a {CHAMPION_VERSION}. Revisa manualmente.")
except Exception:
    print("Alias 'champion' no encontrado. No hay nada que eliminar.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificación del estado final

# COMMAND ----------

for alias in ["candidate", "challenger", "champion", "retired", "rejected"]:
    try:
        v = client.get_model_version_by_alias(UC_MODEL_NAME, alias)
        print(f"  {alias:12s} → versión {v.version}")
    except Exception:
        print(f"  {alias:12s} → (no asignado)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estado esperado tras la ejecución
# MAGIC
# MAGIC ```
# MAGIC   candidate    → versión 6
# MAGIC   challenger   → (no asignado)
# MAGIC   champion     → (no asignado)
# MAGIC   retired      → (no asignado)
# MAGIC   rejected     → (no asignado)
# MAGIC ```
# MAGIC
# MAGIC Si la salida coincide, puedes relanzar `08_Production` con seguridad.