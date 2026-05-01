"""
Shared utilities for the experimentation phase of the auto insurance fraud
detection project.
"""


###############################################################################
# Imports
###############################################################################

import os
from datetime import datetime, timedelta
from pathlib import Path

from dateutil.relativedelta import relativedelta

import numpy as np

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
matplotlib.use("Agg")  # Non-interactive backend: safe on cluster drivers with no display

# pyspark.ml.evaluation removed: metrics now computed via scikit-learn in compute_metrics()
# VarianceThresholdSelectorModel removed: sklearn pipeline uses VarianceThreshold instead
from pyspark.sql import functions as F

from sklearn.calibration import calibration_curve
from sklearn.metrics import (
    confusion_matrix,
    precision_recall_curve,
    roc_curve
)


###############################################################################
# Data and infrastructure
###############################################################################

CATALOG = "workspace"
DATABASE = "auto_insurance_fraud"
TRAINING_TABLE = f"{CATALOG}.{DATABASE}.fraud_training_dataset"

catalog = CATALOG
database = DATABASE
training_table = TRAINING_TABLE

uc_volume_path = Path("/") / "Volumes" / CATALOG / DATABASE / "ml_artifacts"

# Required for logging models on serverless clusters:
# without a volume path, serialization has nowhere to write
os.environ["MLFLOW_DFS_TMP"] = str(uc_volume_path)


###############################################################################
# Column configuration
###############################################################################

LABEL_COLUMN = "is_fraud"
CLASS_WEIGHT_COLUMN = "class_weight"
FEATURES_COLUMN = "features_scaled"
DATE_COLUMN = "claim_timestamp"
LABEL_AVAILABLE_DATE_COLUMN = "label_available_date"
CLAIM_ID_COLUMN = "claim_id"
POLICY_ID_COLUMN = "policy_id"

label_column = LABEL_COLUMN
class_weight_column = CLASS_WEIGHT_COLUMN
features_column = FEATURES_COLUMN
date_column = DATE_COLUMN
label_available_date_column = LABEL_AVAILABLE_DATE_COLUMN
claim_id_column = CLAIM_ID_COLUMN
policy_id_column = POLICY_ID_COLUMN


###############################################################################
# Temporal split configuration
###############################################################################

# Test window: everything added after ml.data_previous_max_date of the previous
# cycle; duration varies depending on when retraining is triggered.
# Validation window: VALIDATION_WINDOW_MONTHS months immediately before the test window.
# Training window: TRAINING_WINDOW_MONTHS immediately before the validation window;
# capped to avoid stale fraud patterns degrading performance.
# On the first version of the table, the split falls back to the defaults.
TRAINING_WINDOW_MONTHS = 36
VALIDATION_WINDOW_MONTHS = 12

# Random seed: shared so run tags built in both notebooks are identical
seed = 45127


###############################################################################
# Project metadata
###############################################################################

# Shared across all notebooks and used as tags throughout
current_user = spark.sql("SELECT current_user()").collect()[0][0]
project = "auto_insurance_fraud_detection"
team = "ml_engineering"
task = "binary_classification"
environment = "development"
algorithm_family = "logistic_regression"
framework = "pyspark.ml"

uc_model_name = f"{catalog}.{database}.fraud_lr_pipeline"

# The experiment is always co-located with the assets of this project
mlflow_experiment_name = "fraud_detection_training"
mlflow_experiment_path = str(
    Path("/") / "Workspace" / "Users" / current_user / ".experiments" / database / mlflow_experiment_name
)


###############################################################################
# Raw data load
###############################################################################

df_raw = spark.table(training_table)

print(f"Total rows: {df_raw.count():,}")
print(f"Total columns: {len(df_raw.columns)}")
print()


###############################################################################
# Temporal split and inverse-frequency class weights
###############################################################################

# Read the table properties of the latest version to determine the split strategy.
# ml.delta_semantic_version drives the branching logic: version 0 uses the fixed
# initial splits, while any subsequent version uses the rolling window anchored
# to ml.data_previous_max_date persisted by the data generation notebook.
properties_df = spark.sql(f"SHOW TBLPROPERTIES {training_table}")
semantic_version_row = properties_df.filter("key = 'ml.delta_semantic_version'").first()
delta_semantic_version = int(semantic_version_row["value"]) if semantic_version_row else 0

if delta_semantic_version == 0:
    # First version of the table: training 2015–2021, validation 2022–2023, test 2024 (reserved).
    # The dataset covers 10 years of claims (2015–2024); we reserve the last 12 months as
    # an out-of-time test set and use the preceding 24 months for validation.
    train_end = datetime(2021, 12, 31)
    validation_end = datetime(2023, 12, 31)
else:
    # Subsequent versions: rolling window anchored to the previous cycle maximum date.
    # ml.data_previous_max_date is the maximum claim_timestamp before the last overwrite,
    # persisted in the table properties by the data generation notebook.
    # Everything after that date in the current table becomes the test window.
    # Validation is the VALIDATION_WINDOW_MONTHS immediately before that cutoff.
    # Training is the TRAINING_WINDOW_MONTHS immediately before validation.
    previous_max_date_row = properties_df.filter("key = 'ml.data_previous_max_date'").first()
    validation_end = datetime.strptime(previous_max_date_row["value"], "%Y-%m-%d")
    train_end = validation_end - relativedelta(months=VALIDATION_WINDOW_MONTHS)

train_start = train_end - relativedelta(months=TRAINING_WINDOW_MONTHS)

train_start_date = train_start.strftime("%Y-%m-%d")
train_end_date = train_end.strftime("%Y-%m-%d")
validation_start_date = (train_end + timedelta(days=1)).strftime("%Y-%m-%d")
validation_end_date = validation_end.strftime("%Y-%m-%d")
test_start_date = (validation_end + timedelta(days=1)).strftime("%Y-%m-%d")
test_end_date = df_raw.agg(
    F.max(F.col(date_column)).alias("max_date")
).collect()[0]["max_date"].strftime("%Y-%m-%d")

train_df = df_raw.filter(
    (F.col(date_column) >= train_start_date) & (F.col(date_column) <= train_end_date)
)
validation_df = df_raw.filter(
    (F.col(date_column) >= validation_start_date) & (F.col(date_column) <= validation_end_date)
)
test_df = df_raw.filter(
    (F.col(date_column) >= test_start_date) & (F.col(date_column) <= test_end_date)
)

print(f"Semantic version: {delta_semantic_version}")
print()
print(f"Train period: {train_start_date} → {train_end_date}")
print(f"Validation period: {validation_start_date} → {validation_end_date}")
print(f"Test period: {test_start_date} → {test_end_date}")
print()
print(f"Train rows: {train_df.count():,}")
print(f"Validation rows: {validation_df.count():,}")
print(f"Test rows: {test_df.count():,}")
print()


def apply_class_weights(df):
    """
    Calculates and applies inverse-frequency class weights dynamically
    based on the exact distribution of the provided DataFrame.

    The dataset has ~7.9% fraud rate (474,911 / 6,000,000), producing weights
    of approximately 5.8x for fraud and 0.54x for legitimate claims.
    """
    n_total = df.count()
    n_fraud = df.filter(F.col(label_column) == 1).count()
    n_legit = n_total - n_fraud

    pct_fraud = 100 * n_fraud / n_total if n_total > 0 else 0.0
    pct_legit = 100 * n_legit / n_total if n_total > 0 else 0.0

    weight_fraud = n_total / (2.0 * n_fraud) if n_fraud > 0 else 1.0
    weight_legit = n_total / (2.0 * n_legit) if n_legit > 0 else 1.0

    print(f"Total rows for training: {n_total:,}")
    print(f"Fraud: {n_fraud:,} ({pct_fraud:.2f}%), weight = {weight_fraud:.2f}")
    print(f"Legit: {n_legit:,} ({pct_legit:.2f}%), weight = {weight_legit:.2f}")
    print()

    weighted_df = df.withColumn(
        class_weight_column,
        F.when(F.col(label_column) == 1.0, weight_fraud).otherwise(weight_legit)
    )

    return weighted_df


###############################################################################
# Column classification
###############################################################################

# Type sets used to route each field to the correct pipeline stage
numeric_types = {"IntegerType", "LongType", "FloatType", "DoubleType", "DecimalType"}
boolean_types = {"BooleanType"}
categorical_types = {"StringType"}

# Integer columns that encode binary yes/no flags and should be treated as
# boolean features rather than continuous numeric ones. This prevents the
# standard scaler from assigning disproportionate magnitude to them.
binary_flag_columns = [
    "police_report_filed",
    "outside_business_hours",
    "has_third_party_injury",
    "third_party_same_insurer",
    "telematics_anomaly",
    "has_telematics",
    "multi_policy",
    "high_mileage_flag",
    "is_electric",
]

# The label, identifiers, temporal columns, and audit metadata are never
# included in the feature vector. They are excluded here rather than dropped
# in the pipeline so they remain available after transform for joins and
# traceability downstream.
#
# gold_agg_timestamp and gold_profile_timestamp are pipeline audit columns
# added by the medallion layer and have no predictive value.
# label_available_date is excluded to prevent target leakage: the date the
# fraud label was confirmed is not available at prediction time.
exclude_columns = [
    label_column,
    claim_id_column,
    policy_id_column,
    label_available_date_column,
    date_column,
    "year",
    "month",
    "gold_agg_timestamp",
    "gold_profile_timestamp",
    "gold_spine_timestamp",
    "__START_AT",
    "__END_AT",
    "is_current",
]

numeric_columns = []
boolean_columns = []
categorical_columns = []

for field in df_raw.schema.fields:
    column_name = field.name
    type_name = type(field.dataType).__name__
    if column_name in exclude_columns:
        continue
    if column_name in binary_flag_columns:
        boolean_columns.append(column_name)
    elif type_name in numeric_types:
        numeric_columns.append(column_name)
    elif type_name in boolean_types:
        boolean_columns.append(column_name)
    elif type_name in categorical_types:
        categorical_columns.append(column_name)

print(f"Numeric ({len(numeric_columns)}): {numeric_columns}")
print(f"Boolean ({len(boolean_columns)}): {boolean_columns}")
print(f"Categorical ({len(categorical_columns)}): {categorical_columns}")
print()


###############################################################################
# Preprocessing configuration
###############################################################################

# Stage 1: imputation
#
# Imputation strategy by column group:
#
# - Aggregation columns (num_*, total_amount_*, num_telematics_*):
#   Spark window functions return NULL — not 0 — when a window is empty.
#   76.6% of policyholders have no prior claims, so these columns are NULL
#   for the majority of rows. We impute with 0 (constant), which is
#   semantically correct: NULL means "no activity", which equals 0 events.
#
# - Ratio/average aggregation columns (avg_amount_*, amount_ratio_*, fraud_rate_30d):
#   Also NULL on empty windows but imputing with 0 could be misleading
#   (e.g. a fraud_rate_30d of 0 implies a clean history). We impute with
#   the median so the model treats them as "typical" rather than "clean".
#
# - Profile numeric columns (age, premium, vehicle attributes, etc.):
#   These come from customer_profile and should always be present after the
#   PiT join. We impute with the median as a safe fallback for any gaps.

# Aggregation count/sum columns: NULL on empty window → impute with 0
agg_zero_prefixes = (
    "num_claims_",
    "total_amount_",
    "num_telematics_",
    "num_fraud_confirmed_",
    "num_unique_shops_",
)
agg_zero_columns = [c for c in numeric_columns if c.startswith(agg_zero_prefixes)]

# Aggregation average/ratio columns: NULL on empty window → impute with median
agg_null_prefixes = (
    "avg_amount_",
    "amount_ratio_",
    "fraud_rate_",
)
agg_null_columns = [c for c in numeric_columns if c.startswith(agg_null_prefixes)]

# Profile numeric columns: always present after PiT join → impute with median as fallback
profile_numeric_columns = [
    c for c in numeric_columns
    if not c.startswith(agg_zero_prefixes) and not c.startswith(agg_null_prefixes)
]

imputer_input_columns = profile_numeric_columns + agg_null_columns
imputer_output_columns = [f"{c}_imp" for c in imputer_input_columns]

# Stage 2: binary flag cast
#
# Binary integer flags are cast to Double for VectorAssembler compatibility.
# COALESCE treats NULL as 0.0 (flag absent = not triggered).
boolean_cast_expressions = ", ".join([
    f"COALESCE(CAST({c} AS DOUBLE), 0.0) AS {c}_dbl"
    for c in boolean_columns
])
boolean_output_columns = [f"{c}_dbl" for c in boolean_columns]
boolean_statement = f"SELECT *, {boolean_cast_expressions} FROM __THIS__"

# Stage 3: claim-level feature engineering
#
# Only features derivable from the claim event itself at inference time.
# Policyholder history features already come from customer_aggregations via
# the PiT join in notebook 05 and do not need to be re-derived here.
feature_engineering_statement = (
    "SELECT *, "
    "LOG(claimed_amount_eur + 1) AS claimed_amount_log, "
    "CAST((days_to_report > 7) AS INT) AS late_report_flag, "
    "CAST((n_parties_involved > 2) AS INT) AS multi_party_flag "
    "FROM __THIS__"
)
engineered_columns = [
    "claimed_amount_log",
    "late_report_flag",
    "multi_party_flag",
]

# Stages 4 and 5: categorical encoding
string_indexer_input_columns = categorical_columns
string_indexer_output_columns = [f"{c}_idx" for c in categorical_columns]

ohe_input_columns = string_indexer_output_columns
ohe_output_columns = [f"{c}_ohe" for c in categorical_columns]

# Stage 6: vector assembly
assembler_input_columns = (
    imputer_output_columns
    + agg_zero_columns
    + boolean_output_columns
    + ohe_output_columns
    + engineered_columns
)
assembler_output_column = "features"

# Stages 7 and 8: variance threshold selection and standard scaling
var_selector_input_column = assembler_output_column
var_selector_output_column = "features_var_filtered"
scaler_input_column = var_selector_output_column
scaler_output_column = features_column

print(f"Assembler inputs ({len(assembler_input_columns)}): {assembler_input_columns}")
print()


###############################################################################
# Visualization
###############################################################################

# Threshold sweep grid: shared between the threshold visualization and the
# optimal-threshold search loop so the logged value matches the figure
threshold_sweep_start = 0.01
threshold_sweep_stop = 0.99
threshold_sweep_steps = 99

fig_size_standard = (6, 5)
fig_size_wide = (8, 5)
fig_size_confusion = (11, 4)
fig_size_coef_width = 9
fig_size_coef_row_h = 0.30
fig_size_coef_min_h = 4

color_roc = "#1f77b4"
color_pr = "#ff7f0e"
color_calibration = "#2ca02c"
color_positive_coef = "#d62728"
color_negative_coef = "#1f77b4"
color_random_baseline = "gray"

calibration_n_bins = 10
coefficients_top_n = 30


def save_diagnostic_figure(fig, directory_path, filename):
    """
    Saves a matplotlib figure to the specified directory and closes it to free memory.
    """
    file_path = Path(directory_path) / filename
    fig.savefig(file_path, dpi=150, bbox_inches="tight")
    plt.close("all")


def fig_pr_curve(y_true, y_prob, auc_pr, title):
    """
    Plot the PR curve with AUC annotation and a shaded area.
    Preferred over ROC for imbalanced datasets: the random baseline equals
    the positive class rate, not a fixed diagonal.
    """
    precision_values, recall_values, _ = precision_recall_curve(y_true, y_prob)
    baseline = y_true.mean()
    fig, ax = plt.subplots(figsize=fig_size_standard)
    ax.plot(recall_values, precision_values, lw=2, color=color_pr, label=f"AUC-PR = {auc_pr:.4f}")
    ax.axhline(baseline, color=color_random_baseline, linestyle="--", lw=1, label=f"Random baseline = {baseline:.3f}")
    ax.fill_between(recall_values, precision_values, alpha=0.08, color=color_pr)
    ax.set_xlabel("Recall")
    ax.set_ylabel("Precision")
    ax.set_title(title)
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()
    return fig


def fig_roc_curve(y_true, y_prob, auc_roc, title):
    """
    Plot the ROC curve with AUC annotation and a shaded area under the curve.
    The random classifier diagonal (AUC = 0.5) is shown as a dashed baseline.
    """
    fpr, tpr, _ = roc_curve(y_true, y_prob)
    fig, ax = plt.subplots(figsize=fig_size_standard)
    ax.plot(fpr, tpr, lw=2, color=color_roc, label=f"AUC-ROC = {auc_roc:.4f}")
    ax.plot([0, 1], [0, 1], "k--", lw=1, alpha=0.5, label="Random classifier")
    ax.fill_between(fpr, tpr, alpha=0.08, color=color_roc)
    ax.set_xlabel("False positive rate")
    ax.set_ylabel("True positive rate")
    ax.set_title(title)
    ax.legend(loc="lower right")
    ax.grid(alpha=0.3)
    plt.tight_layout()
    return fig


def fig_confusion_matrix(y_true, y_pred, title):
    """
    Plot the confusion matrix as two side-by-side panels: raw counts and
    row-normalized percentages. Row normalization reveals the per-class
    detection rate independently of class frequency.
    """
    cm = confusion_matrix(y_true, y_pred)
    cm_pct = cm.astype(float) / cm.sum(axis=1, keepdims=True) * 100
    fig, axes = plt.subplots(1, 2, figsize=fig_size_confusion)
    for ax, data, fmt, subtitle in [
        (axes[0], cm, "d", "Counts"),
        (axes[1], cm_pct, ".1f", "Row %"),
    ]:
        im = ax.imshow(data, cmap="Blues")
        plt.colorbar(im, ax=ax)
        for i in range(2):
            for j in range(2):
                ax.text(
                    j, i,
                    format(data[i, j], fmt),
                    ha="center", va="center",
                    color="white" if data[i, j] > data.max() / 2 else "black",
                    fontsize=12, fontweight="bold",
                )
        ax.set_xticks([0, 1])
        ax.set_yticks([0, 1])
        ax.set_xticklabels(["Legit", "Fraud"])
        ax.set_yticklabels(["Legit", "Fraud"])
        ax.set_xlabel("Predicted")
        ax.set_ylabel("True")
        ax.set_title(f"{title} — {subtitle}")
    plt.tight_layout()
    return fig


def fig_lr_coefficients(coef_array, feature_names, title):
    """
    Plot the top logistic regression coefficients sorted by absolute value.
    Red means positive coefficient (push toward fraud) and
    blue means negative (push toward legit).
    """
    coef = np.array(coef_array)
    n = min(coefficients_top_n, len(coef))
    idx = np.argsort(np.abs(coef))[-n:][::-1]
    fig_height = max(fig_size_coef_min_h, n * fig_size_coef_row_h)
    fig, ax = plt.subplots(figsize=(fig_size_coef_width, fig_height))
    colors = [color_positive_coef if c > 0 else color_negative_coef for c in coef[idx]]
    ax.barh(range(n), coef[idx], color=colors, edgecolor="white", linewidth=0.5)
    ax.set_yticks(range(n))
    ax.set_yticklabels([feature_names[i] for i in idx], fontsize=8)
    ax.axvline(0, color="black", lw=0.8)
    ax.set_xlabel("Coefficient value")
    ax.set_title(title)
    ax.legend(
        handles=[
            Patch(color=color_positive_coef, label="Indicative of fraud"),
            Patch(color=color_negative_coef, label="Indicative of legit"),
        ],
        loc="lower right",
        fontsize=8,
    )
    ax.invert_yaxis()
    plt.tight_layout()
    return fig


def fig_calibration_curve(y_true, y_prob, title):
    """
    Plot the calibration curve (reliability diagram). A perfectly calibrated
    model follows the diagonal. Deviations above it indicate under-confidence;
    below it, over-confidence.
    """
    frac_pos, mean_pred = calibration_curve(y_true, y_prob, n_bins=calibration_n_bins)
    fig, ax = plt.subplots(figsize=fig_size_standard)
    ax.plot(mean_pred, frac_pos, "s-", lw=2, color=color_calibration, label="Model")
    ax.plot([0, 1], [0, 1], "k--", lw=1, label="Perfect calibration")
    ax.fill_between(mean_pred, frac_pos, mean_pred, alpha=0.1, color=color_calibration)
    ax.set_xlabel("Mean predicted probability")
    ax.set_ylabel("Fraction of positives")
    ax.set_title(title)
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()
    return fig


def _compute_threshold_metrics(y_true, y_prob):
    """
    Internal helper to calculate precision, recall, and F1 across all thresholds.
    Ensures mathematical consistency between threshold searching and plotting.
    """
    thresholds = np.linspace(threshold_sweep_start, threshold_sweep_stop, threshold_sweep_steps)
    precisions, recalls, f1s = [], [], []

    for t in thresholds:
        y_pred = (y_prob >= t).astype(int)
        tp = ((y_pred == 1) & (y_true == 1)).sum()
        fp = ((y_pred == 1) & (y_true == 0)).sum()
        fn = ((y_pred == 0) & (y_true == 1)).sum()

        p = tp / (tp + fp) if (tp + fp) > 0 else 0
        r = tp / (tp + fn) if (tp + fn) > 0 else 0
        f = 2 * p * r / (p + r) if (p + r) > 0 else 0

        precisions.append(p)
        recalls.append(r)
        f1s.append(f)

    return thresholds, precisions, recalls, f1s


def find_best_threshold(y_true, y_prob):
    """
    Sweeps through decision thresholds to find the one that maximizes F1-score.
    """
    thresholds, _, _, f1s = _compute_threshold_metrics(y_true, y_prob)
    best_idx = np.argmax(f1s)

    return thresholds[best_idx], f1s[best_idx]


def fig_threshold_sweep(y_true, y_prob, title):
    """
    Plot precision, recall, and F1-score across the full range of decision thresholds.
    The vertical dashed line marks the threshold that maximizes F1-score.
    """
    thresholds, precisions, recalls, f1s = _compute_threshold_metrics(y_true, y_prob)
    best_t = thresholds[np.argmax(f1s)]
    fig, ax = plt.subplots(figsize=fig_size_wide)
    ax.plot(thresholds, precisions, label="Precision", color=color_pr)
    ax.plot(thresholds, recalls, label="Recall", color=color_roc)
    ax.plot(thresholds, f1s, label="F1", color=color_calibration, lw=2)
    ax.axvline(best_t, color=color_random_baseline, linestyle="--", lw=1, label=f"Best F1 threshold = {best_t:.2f}")
    ax.set_xlabel("Decision threshold")
    ax.set_ylabel("Score")
    ax.set_title(title)
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()
    return fig


###############################################################################
# Evaluation
###############################################################################

raw_prediction_column = "rawPrediction"
prediction_column = "prediction"
probability_column = "probability"
prob_fraud_column = "prob_fraud"

# Hard limit for ".toPandas()" conversions. Bringing massive distributed datasets into
# the cluster driver's local memory for processing can cause severe out-of-memory crashes.
# This acts as a safety threshold.
to_pandas_max_rows = 200_000

def compute_metrics(y_true, y_prob, y_pred):
    """
    Compute all six evaluation metrics from numpy arrays.

    Parameters
    ----------
    y_true : array-like of int
        Ground-truth binary labels (0 = legit, 1 = fraud).
    y_prob : array-like of float
        Predicted probability of the positive class (fraud).
    y_pred : array-like of int
        Hard binary predictions at the chosen decision threshold.

    Returns a plain dictionary so values can be returned seamlessly
    to the orchestrator notebook without any complex transformations.
    """
    from sklearn.metrics import (
        roc_auc_score,
        average_precision_score,
        f1_score,
        precision_score,
        recall_score,
        accuracy_score,
    )
    return {
        "auc_roc": float(roc_auc_score(y_true, y_prob)),
        "auc_pr": float(average_precision_score(y_true, y_prob)),
        "f1": float(f1_score(y_true, y_pred, zero_division=0)),
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "accuracy": float(accuracy_score(y_true, y_pred)),
    }


# to_pandas_predictions removed: evaluation job materializes Spark partitions
# directly to pandas and calls pipeline_model.predict_proba() on the feature matrix.


# extract_feature_names removed: replaced by extract_feature_names_sklearn()
# defined inline in 07_Training_Job.py, which uses
# pipeline_model.named_steps["preprocessor"].get_feature_names_out()
# and pipeline_model.named_steps["var_selector"].get_support().


print("07_Utils.py script loaded successfully.")
