from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, expr, struct, to_timestamp, to_date, year, month, datediff, current_timestamp, lit, to_json
)
import yaml
from delta import configure_spark_with_delta_pip
from pathlib import Path

# ========================
# PATH RESOLUTION
# ========================

PROJECT_ROOT = Path(__file__).resolve().parents[4]

BRONZE_BASE_PATH = PROJECT_ROOT / "pipelines" / "bronze"
SILVER_BASE_PATH = PROJECT_ROOT / "pipelines" / "silver"

# Ensure Silver directories exist
SILVER_BASE_PATH.mkdir(parents=True, exist_ok=True)

RULES_PATH = PROJECT_ROOT / "src" / "dlt_pipeline" / "rules"

BRONZE_PATHS = {
    "claims": str(BRONZE_BASE_PATH / "claims"),
    "labels": str(BRONZE_BASE_PATH / "labels"),
    "policies": str(BRONZE_BASE_PATH / "policies")
}

SILVER_PATHS = {
    "claims": str(SILVER_BASE_PATH / "claims"),
    "claims_enriched": str(SILVER_BASE_PATH / "claims_enriched"),
    "labels": str(SILVER_BASE_PATH / "labels"),
    "policies": str(SILVER_BASE_PATH / "policies"),
    "quarantine": str(SILVER_BASE_PATH / "quarantine")
}

# ========================
# SPARK SESSION
# ========================

builder = (
    SparkSession.builder.appName("silver_transformation")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# ========================
# LOAD RULES YAML
# ========================
 
def load_rules(filename):
    with open(RULES_PATH / filename) as f:
        return yaml.safe_load(f)["rules"]

claims_rules = load_rules("claims_rules.yml")
labels_rules = load_rules("labels_rules.yml")
policies_rules = load_rules("policies_rules.yml")

# ========================
# DLQ METADATA FUNCTION
# ========================
def add_dlq_metadata(df, rule_name, severity, constraint, source):
    return (
        df.withColumn("error_rule", lit(rule_name))
          .withColumn("error_severity", lit(severity))
          .withColumn("error_constraint", lit(constraint))
          .withColumn("error_source", lit(source))
    )

def dedupe_columns(df):
    seen = set()
    new_cols = []
    for c in df.columns:
        if c not in seen:
            new_cols.append(c)
            seen.add(c)
        else:
            # Duplicate column → rename it
            new_cols.append(f"{c}_dup")
    return df.toDF(*new_cols)


def normalize_dlq(df, rule):
    """
    Converts any invalid-row DF into a consistent DLQ schema.
    Works in Spark local and Databricks.
    The JOIN produces duplicated column names! We need to dedupe them before converting to JSON.
    """

    df = dedupe_columns(df)

    return (
        df
        .withColumn("record_json", to_json(struct([col(c) for c in df.columns])))
        .select("record_json")
        .withColumn("error_rule", lit(rule.get("name")))
        .withColumn("error_severity", lit(rule.get("severity")))
        .withColumn("error_constraint", lit(rule.get("constraint")))
        .withColumn("error_source_table", lit(rule.get("tag")))
        .withColumn("error_timestamp", current_timestamp())
    )


# ========================
# GENERIC RULE APPLIER
# ========================

def apply_rules(df, rules):
    """
    Applies validation rules to a DataFrame.
    Returns a cleaned DataFrame and a quarantine DataFrame.
    """

    quarantine_df = None

    for rule in rules:
        condition = rule["constraint"]
        severity = rule["severity"]

        invalid_rows = df.filter(f"NOT ({condition})")

        # Convert invalid rows into DLQ-standard schema
        invalid_rows = normalize_dlq(invalid_rows, rule)

        # invalid_rows = df.filter(f"NOT ({condition})")

        # # Add metadata BEFORE sending to quarantine
        
        # invalid_rows = add_dlq_metadata(
        #     invalid_rows,
        #     rule_name=rule["name"],
        #     severity=rule["severity"],
        #     constraint=rule["constraint"],
        #     source=rule.get("tag", "unknown")
        # )

        if severity == "error":
            # Collect invalid rows
            if quarantine_df is None:
                quarantine_df = invalid_rows
            else:
                quarantine_df = quarantine_df.union(invalid_rows)

            # Keep valid rows
            df = df.filter(condition)

        else:
            # Add warning flag as a boolean column
            df = df.withColumn(f"warn_{rule['name']}", expr(f"NOT ({condition})"))

    return df, quarantine_df

# ========================
# LOAD BRONZE TABLES
# ========================

claims = spark.read.format("delta").load(BRONZE_PATHS["claims"])
labels = spark.read.format("delta").load(BRONZE_PATHS["labels"])
policies = spark.read.format("delta").load(BRONZE_PATHS["policies"])

# ========================
# TYPE CASTING
# ========================

claims = claims.withColumn("timestamp", to_timestamp("timestamp"))
labels = labels.withColumn("label_available_date", to_timestamp("label_available_date"))
policies = policies.withColumn("policy_start_date", to_date("policy_start_date"))

# ========================
# APPLY RULES
# ========================

claims_clean, claims_quarantine = apply_rules(claims, claims_rules)
labels_clean, labels_quarantine = apply_rules(labels, labels_rules)
policies_clean, policies_quarantine = apply_rules(policies, policies_rules)

# ========================
# REFERENTIAL INTEGRITY
# ========================

claims_enriched = claims_clean.join(policies_clean, "policy_id", "left")


claims_fk_invalid = claims_enriched.filter(
    col("policy_start_date").isNull()
)

# Normalize FK invalid rows into DLQ schema
claims_fk_invalid = normalize_dlq(
    claims_fk_invalid,
    {
        "name": "policy_fk_missing",
        "severity": "error",
        "constraint": "policy_start_date IS NOT NULL",
        "tag": "claims_enriched"
    }
)

claims_enriched = claims_enriched.filter(col("policy_start_date").isNotNull())

# ========================
# DERIVED FEATURES
# ========================

claims_enriched = (
    claims_enriched.withColumn("event_date", to_date("timestamp"))
                   .withColumn("event_year", year("timestamp"))
                   .withColumn("event_month", month("timestamp"))
)

claims_with_labels = claims_enriched.join(labels_clean, "claim_id", "left")

claims_with_labels = claims_with_labels.withColumn(
    "label_delay_days",
    datediff(col("label_available_date"), col("timestamp"))
)

# ========================
# QUARANTINE UNION
# ========================

# quarantine_all = None

# for qdf in [claims_quarantine, labels_quarantine, policies_quarantine, claims_fk_invalid]:
#     if qdf is not None:
#         if quarantine_all is None:
#             quarantine_all = qdf
#         else:
#             quarantine_all = quarantine_all.unionByName(qdf, allowMissingColumns=True)

# Collect all DLQ DFs
quarantine_dfs = [
    q
    for q in [
        claims_quarantine,
        labels_quarantine,
        policies_quarantine,
        claims_fk_invalid
    ]
    if q is not None
]

# If any exist → union safely
if quarantine_dfs:
    quarantine_all = quarantine_dfs[0]
    for qdf in quarantine_dfs[1:]:
        quarantine_all = quarantine_all.unionByName(qdf)
else:
    quarantine_all = None

# ========================
# WRITE SILVER TABLES
# ========================

claims_clean.write.format("delta").mode("overwrite").save(SILVER_PATHS["claims"])
labels_clean.write.format("delta").mode("overwrite").save(SILVER_PATHS["labels"])
policies_clean.write.format("delta").mode("overwrite").save(SILVER_PATHS["policies"])

claims_with_labels.write.format("delta").mode("overwrite").save(SILVER_PATHS["claims_enriched"])

if quarantine_all is not None:
    quarantine_all.write.format("delta").mode("overwrite").save(SILVER_PATHS["quarantine"])

print("Silver transformation completed successfully.")