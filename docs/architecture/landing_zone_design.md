# **Diseño de la Landing Zone**

> Este fichero simula o retrata el diseño de la `landing_zone` como si se hiciese _en papel_

El script de `/src/data_generation/generate.py` generó una estructura tal que así:

```
data/
├── context/
├── events/YYYY/MM/
└── source_buffer/YYYY/MM/
```

Es **exacta y perfectamente compatible** con un esquema estilo _Hive partitioning_ porque:

- Se generaron carpetas con formato **year=YYYY / month=MM** $\rightarrow$ Spark las lee como particiones:
    - cada carpeta hoja contiene solo ese periodo
    - separación física entre `claims` y `labels` (relevante para un posible _delayed feedback_)
- Cada fichero es **JSON line‑delimited**, perfecto para ingestion incremental
- El año 2025 está separado en `source_buffer/`, que puede simular _"landing zone late arriving data"_

## Propuesta de mapeo conceptual

| Zona Lakehouse | Significado | Equivalente en el repo |
|----------------|-------------|------------------|
| **Landing / Raw** | Datos tal cual llegan | `data/events/*`, `data/source_buffer/*` |
| **Context / Master** | Datos de referencia estáticos | `data/context/policies.csv` |
| **Bronze (Databricks)** | Tablas externas en DBFS | `:dbfs/FileStore/raw/...` |
| **Silver** | Datos limpios y conformados | Tablas Delta Hive |
| **Gold** | Feature store / agregados / ML inputs | Tablas Delta optimizadas |

## Propuesta en Databricks Free Edition

Como no se dispone de _Unity Catalog_, vamos a **replicar la Landing Zone usando DBFS**, creando un volumen llamado `insurance_lake`, construyendo así la ruta:

> `/Volumes/workspace/default/insurance_lake/`

Con la siguiente estructura:

```
insurance_lake/
├── lz/
├── raw/
│   ├──context/policies.csv
│   └──events/
│       ├──claims/
│       └──labels/
├── source_buffer/
├── base/
└── gold/
```

> `lz/` es la zona de aterrizaje (**_landing zone_**): el lugar donde van a ir los archivos tal cual llegan desde fuera antes de cualquier validación, limpieza o ingesta en `raw/`

## Configuración en local (WSL2 | Ubuntu 22)

> Workspace ofuscado

```bash
sudo apt install zip unzip
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/v0.295.0/install.sh | sudo sh
export BROWSER=wslview
sudo apt install wslu -y
export DATABRICKS_CLI_BROWSER=system
databricks auth login --host https://<mi-workspace>.cloud.databricks.com
databricks fs ls dbfs:/Volumes/workspace/default/insurance_lake --profile <mi-workspace> # Probamos que funcione
```

Y luego...

```bash
databricks fs cp ~/auto-insurance-fraud/data/context/policies.csv dbfs:/Volumes/workspace/default/insurance_lake/raw/context/policies.csv --overwrite --profile <mi-workspace>
databricks fs cp -r ~/auto-insurance-fraud/data/events/claims/ dbfs:/Volumes/workspace/default/insurance_lake/raw/events/claims --overwrite --profile <mi-workspace>
databricks fs cp -r ~/auto-insurance-fraud/data/events/labels/ dbfs:/Volumes/workspace/default/insurance_lake/raw/events/labels --overwrite --profile <mi-workspace>
```

Y de esta manera **tenemos copiado en Databricks todos los datos sintéticos históricos**. Ahora podemos copiar los futuros (simulados):

```bash
databricks fs cp -r ~/auto-insurance-fraud/data/source_buffer/ dbfs:/Volumes/workspace/default/insurance_lake/source_buffer --overwrite --profile <mi-workspace>
```

De esta manera evitamos mezclar con datos históricos, pues no transformamos todavía (a diferencia de los anteriores), simplemente lo queremos para **simular _streaming_**.

---

### Resumen de datos subidos a Databricks

- context $\rightarrow$ maestro estático
- claims $\rightarrow$ llegan en tiempo real / continuo
- labels $\rightarrow$ llegan más tarde (_delayed feedback_)
- source_buffer $\rightarrow$ simula datos del futuro