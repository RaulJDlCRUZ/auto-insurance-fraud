"""
Shared `MLflow` utilities for the production evaluation and promotion pipeline.
"""


###############################################################################
# Imports
###############################################################################

import json
import logging
import os
import warnings
from pathlib import Path

import mlflow
import mlflow.spark
from mlflow.models.signature import infer_signature

import pandas as pd

from pyspark.ml import PipelineModel


###############################################################################
# Production run configuration
###############################################################################

production_run_name = "production_evaluation"
selection_metric = "auc_pr"
training_notebook_path = "./07_Training_Job"
evaluation_notebook_path = "./07_Evaluation_Job"
training_timeout_seconds = 3600
evaluation_timeout_seconds = 3600
training_max_retries = 3
evaluation_max_retries = 3


###############################################################################
# Table configuration
###############################################################################

baseline_table_name = f"{catalog}.{database}.gold_fraud_test_baseline"


###############################################################################
# Column configuration
###############################################################################

inference_timestamp_col = "inference_timestamp"
model_version_col = "model_version"


###############################################################################
# MLflow setup
###############################################################################

def setup_mlflow_warnings():
    """
    Suppress noisy but harmless `MLflow` warnings that clutter notebook output
    on `Databricks Serverless` and `Spark Connect` environments.
    """
    warnings.filterwarnings(
        action = "ignore",
        category = UserWarning,
        module = "mlflow.types.utils"
    )
    logging.getLogger("mlflow.data.spark_dataset").setLevel(logging.ERROR)
    logging.getLogger("mlflow.data.spark_delta_utils").setLevel(logging.ERROR)
    logging.getLogger("mlflow.utils.environment").setLevel(logging.ERROR)
    logging.getLogger("mlflow.utils.requirements_utils").setLevel(logging.ERROR)


###############################################################################
# Candidate metadata
###############################################################################

# Mapping from Unity Catalog tag name to local key used throughout the pipeline.
# Keeping this in one place ensures that tag reads and writes are always in sync.
_CANDIDATE_TAG_KEYS = {
    "candidate_run_id": "candidate_run_id",
    "best_threshold_val": "best_threshold_val",
    "reg_param": "reg_param",
    "elastic_net_param": "elastic_net_param",
    "lr_max_iter": "max_iter",
    "lr_family": "family",
    "lr_standardization": "standardization",
    "pp_imputer_strategy": "imputer_strategy",
    "pp_var_selector_threshold": "var_selector_threshold",
    "pp_scaler_with_mean": "scaler_with_mean",
    "pp_scaler_with_std": "scaler_with_std",
    "pp_ohe_drop_last": "ohe_drop_last",
    "pp_si_handle_invalid": "si_handle_invalid",
    "pp_si_order_type": "si_order_type",
    "pp_ohe_handle_invalid": "ohe_handle_invalid",
    "pp_asm_handle_invalid": "asm_handle_invalid"
}

_CANDIDATE_DEFAULTS = {
    "best_threshold_val": "0.5",
    "max_iter": "100",
    "family": "binomial",
    "standardization": "False",
    "imputer_strategy": "median",
    "var_selector_threshold": "0.01",
    "scaler_with_mean": "False",
    "scaler_with_std": "True",
    "ohe_drop_last": "True",
    "si_handle_invalid": "keep",
    "si_order_type": "frequencyDesc",
    "ohe_handle_invalid": "keep",
    "asm_handle_invalid": "error"
}


def extract_candidate_metadata(candidate_version):
    """
    Extract all relevant metadata from a `Unity Catalog` model version that
    carries the `candidate` alias.

    Returns a dictionary with normalized local keys (see `_CANDIDATE_TAG_KEYS`)
    plus `version_number` and `evaluation_tag`. Default values from
    `_CANDIDATE_DEFAULTS` are applied when a tag is absent.
    """
    tags = candidate_version.tags
    version = candidate_version.version

    metadata = {"version_number": version, "evaluation_tag": f"challenger_v{version}"}

    for tag_key, local_key in _CANDIDATE_TAG_KEYS.items():
        default = _CANDIDATE_DEFAULTS.get(local_key)
        metadata[local_key] = tags.get(tag_key, default)

    # Coerce the threshold to float immediately so callers never have to
    metadata["best_threshold_val"] = float(metadata["best_threshold_val"])

    return metadata


def build_challenger_hyperparams(candidate_metadata):
    """
    Build the hyperparameter dictionary expected by `run_training_job` from the
    normalized candidate metadata returned by `extract_candidate_metadata`.

    All values are kept as strings because `dbutils.notebook.run()` only
    accepts string widget values.
    """
    return {
        "reg_param": candidate_metadata["reg_param"],
        "elastic_net_param": candidate_metadata["elastic_net_param"],
        "max_iter": candidate_metadata["max_iter"],
        "family": candidate_metadata["family"],
        "standardization": candidate_metadata["standardization"],
        "threshold": str(candidate_metadata["best_threshold_val"]),
        "imputer_strategy": candidate_metadata["imputer_strategy"],
        "var_selector_threshold": candidate_metadata["var_selector_threshold"],
        "scaler_with_mean": candidate_metadata["scaler_with_mean"],
        "scaler_with_std": candidate_metadata["scaler_with_std"],
        "ohe_drop_last": candidate_metadata["ohe_drop_last"],
        "si_handle_invalid": candidate_metadata["si_handle_invalid"],
        "si_order_type": candidate_metadata["si_order_type"],
        "ohe_handle_invalid": candidate_metadata["ohe_handle_invalid"],
        "asm_handle_invalid": candidate_metadata["asm_handle_invalid"]
    }


###############################################################################
# Registry read
###############################################################################

def get_champion_metadata(client, uc_model_name):
    """
    Attempt to retrieve the current `champion` model version and its metadata.

    Returns `None` on a cold start (no `champion` alias yet), allowing the
    caller to skip the champion-versus-challenger comparison and promote
    the challenger directly.

    The model artifact URI points to the `challenger_model` artefact logged
    during the champion's own production evaluation run. This is the model
    that was trained on `train + validation` and evaluated on `test`, ensuring
    a symmetric comparison with the new challenger.
    """
    try:
        champion_version = client.get_model_version_by_alias(
            name  = uc_model_name,
            alias = "champion"
        )
        version_number = champion_version.version
        production_run_id = champion_version.tags.get("production_run_id")
        return {
            "version_number": version_number,
            "production_run_id": production_run_id,
            "best_threshold_val": float(champion_version.tags.get("best_threshold_val", "0.5")),
            "evaluation_tag": f"champion_v{version_number}",
            "model_artifact_uri": f"runs:/{production_run_id}/challenger_model"
        }
    except Exception:
        return None


###############################################################################
# Notebook orchestration
###############################################################################

def run_training_job(
    notebook_path,
    timeout,
    hyperparams,
    training_mode,
    max_retries
):
    """
    Launch `07_Training_Job.ipynb` in an isolated `Spark Connect` session and return the
    parsed result dictionary.

    All hyperparameters are passed as `String` via widgets, as required by
    `dbutils.notebook.run()`. The `training_mode` controls which data partitions
    are used: `"train"`, `"train_val"`, or `"train_val_test"`.
    """
    for attempt in range(1, max_retries + 1):
        try:
            result_json = dbutils.notebook.run(
                notebook_path,
                timeout,
                {**{k: str(v) for k, v in hyperparams.items()}, "training_mode": training_mode}
            )
            return json.loads(result_json)
        except Exception as e:
            error_msg = str(e).split("\n")[0]
            print(f"Training attempt {attempt} of {max_retries} failed. Details: {error_msg}")

    raise RuntimeError(
        f"Training job '{notebook_path}' aborted after {max_retries} consecutive failures."
    )


def run_evaluation_job(
    notebook_path,
    timeout,
    model_artifact_uri,
    evaluation_dataset,
    evaluation_tag,
    max_retries
):
    """
    Launch `07_Evaluation_Job.ipynb` in an isolated `Spark Connect` session and return the
    parsed result dictionary.

    The model must already be logged to an active `MLflow` run before calling this
    function, so that `model_artifact_uri` (`runs:/<run_id>/<artifact_path>`) resolves
    correctly in the child session.
    """
    for attempt in range(1, max_retries + 1):
        try:
            result_json = dbutils.notebook.run(
                notebook_path,
                timeout,
                {
                    "model_artifact_uri": model_artifact_uri,
                    "evaluation_dataset": evaluation_dataset,
                    "evaluation_tag": evaluation_tag
                }
            )
            return json.loads(result_json)
        except Exception as e:
            error_msg = str(e).split("\n")[0]
            print(f"Evaluation attempt {attempt} of {max_retries} failed. Details: {error_msg}")

    raise RuntimeError(
        f"Evaluation job '{notebook_path}' aborted after {max_retries} consecutive failures."
    )


###############################################################################
# Model logging
###############################################################################

def log_pipeline_model(
    model_save_path,
    input_example_path,
    output_example_path,
    artifact_path
):
    """
    Load a `PipelineModel` from a `Unity Catalog` volume path and log it to the active
    `MLflow` run. The signature is inferred from the `parquet` example files. The model
    is deleted from memory immediately after logging to avoid cache pressure.

    Requires `MLFLOW_DFS_TMP` to be set to a `Unity Catalog` volume path, as `Serverless`
    clusters cannot serialize `Spark ML` models without a volume-backed temporary directory.
    """
    pipeline_model = PipelineModel.load(model_save_path)
    input_example = pd.read_parquet(input_example_path)
    output_example = pd.read_parquet(output_example_path)
    signature = infer_signature(input_example, output_example)

    mlflow.spark.log_model(
        spark_model = pipeline_model,
        artifact_path = artifact_path,
        signature = signature
    )
    del pipeline_model


###############################################################################
# Promotion decision
###############################################################################

def make_promotion_decision(challenger_metrics, champion_metrics, metric_key):
    """
    Decide whether the challenger should replace the champion.

    The challenger must **strictly** exceed the champion on `metric_key`. In
    the event of a tie the champion is kept, following the principle that a new
    model must demonstrate a real improvement over the one already in production.

    When there is no champion (`champion_metrics = None`) the challenger is
    promoted unconditionally.
    """
    if champion_metrics is None:
        return True, "No champion exists. Challenger promoted directly."

    challenger_score = challenger_metrics[metric_key]
    champion_score = champion_metrics[metric_key]
    delta = challenger_score - champion_score
    challenger_wins = challenger_score > champion_score

    if challenger_wins:
        reason = (
            f"Challenger wins by {delta:+.4f} {metric_key} on test "
            f"({challenger_score:.4f} versus {champion_score:.4f})."
        )
    else:
        reason = (
            f"Champion holds by {-delta:+.4f} {metric_key} on test "
            f"({champion_score:.4f} versus {challenger_score:.4f})."
        )

    return challenger_wins, reason


###############################################################################
# Registry write
###############################################################################

def promote_candidate_to_challenger(client, uc_model_name, version_number):
    """
    Atomically swap the `candidate` alias to `challenger` on the given model version.

    Assigns `challenger` first, then removes `candidate` so that the model is
    never left without at least one alias during the transition.
    """
    client.set_registered_model_alias(
        name = uc_model_name,
        alias = "challenger",
        version = version_number
    )
    client.delete_registered_model_alias(
        name = uc_model_name,
        alias = "candidate"
    )


def apply_promotion_aliases(
    client,
    uc_model_name,
    challenger_wins,
    challenger_version_number,
    final_version_number,
    champion_version_number,
    challenger_test_metrics,
    challenger_validation_threshold,
    challenger_hyperparams,
    champion_exists,
    production_run_id,
    candidate_run_id,
    test_end_date
):
    """
    Apply all `Unity Catalog` alias and tag changes that reflect the promotion decision.

    When the challenger wins, the current champion is moved to `retired` with a retirement
    reason tag, the full-refit version is crowned `champion` with its promotion metadata,
    and the `challenger` alias is removed from the evaluated version.

    When the champion holds, the challenger version is moved to `rejected` with a rejection
    reason tag and its test score, and the `challenger` alias is removed.
    """
    if challenger_wins:
        if champion_exists:
            client.set_registered_model_alias(
                name = uc_model_name,
                alias = "retired",
                version = champion_version_number
            )
            client.delete_registered_model_alias(
                name = uc_model_name,
                alias = "champion"
            )
            client.set_model_version_tag(
                uc_model_name, champion_version_number,
                "retirement_reason", "outperformed_by_challenger"
            )
            print(f"Version {champion_version_number} → 'retired'")

        client.set_registered_model_alias(
            name = uc_model_name,
            alias = "champion",
            version = final_version_number
        )
        client.delete_registered_model_alias(
            name = uc_model_name,
            alias = "challenger"
        )
        client.set_model_version_tag(uc_model_name, final_version_number, "promoted_to_champion_by", "auto__max_test_auc_pr")
        client.set_model_version_tag(uc_model_name, final_version_number, "test_auc_pr", f"{challenger_test_metrics['auc_pr']:.4f}")
        client.set_model_version_tag(uc_model_name, final_version_number, "best_threshold_val", str(challenger_validation_threshold))
        client.set_model_version_tag(uc_model_name, final_version_number, "reg_param", challenger_hyperparams["reg_param"])
        client.set_model_version_tag(uc_model_name, final_version_number, "elastic_net_param", challenger_hyperparams["elastic_net_param"])
        client.set_model_version_tag(uc_model_name, final_version_number, "production_run_id", production_run_id)
        client.set_model_version_tag(uc_model_name, final_version_number, "candidate_run_id", candidate_run_id)
        client.set_model_version_tag(uc_model_name, final_version_number, "test_end_date", test_end_date)
        print(f"Version {final_version_number} (full refit on training, validation, and test) → 'champion'")

    else:
        client.set_registered_model_alias(
            name = uc_model_name,
            alias = "rejected",
            version = challenger_version_number
        )
        client.delete_registered_model_alias(
            name = uc_model_name,
            alias = "challenger"
        )
        client.set_model_version_tag(uc_model_name, challenger_version_number, "rejection_reason", "did_not_outperform_champion")
        client.set_model_version_tag(uc_model_name, challenger_version_number, "test_auc_pr", f"{challenger_test_metrics['auc_pr']:.4f}")
        print(f"Version {challenger_version_number} → 'rejected'")


###############################################################################
# MLflow run logging
###############################################################################

def log_production_metrics(
    challenger_metrics,
    champion_metrics,
    challenger_run_id,
    champion_run_id,
    challenger_wins,
    decision_reason
):
    """
    Log all metrics, parameters, and decision tags to the active `MLflow` run
    at the end of the production evaluation pipeline.

    Challenger metrics are always logged. Champion metrics, the delta, and
    the champion run id are only logged when a champion exists.
    """
    mlflow.set_tags({
        "decision": "champion_promoted" if challenger_wins else "champion_held",
        "decision_reason": decision_reason
    })

    mlflow.log_metrics({f"challenger_test_{k}": v for k, v in challenger_metrics.items()})

    if champion_metrics is not None:
        mlflow.log_metrics({f"champion_test_{k}": v for k, v in champion_metrics.items()})
        mlflow.log_metric(
            "test_auc_pr_delta",
            challenger_metrics["auc_pr"] - champion_metrics["auc_pr"]
        )

    mlflow.log_param("challenger_run_id", challenger_run_id)
    mlflow.log_param("challenger_wins", str(challenger_wins))
    if champion_run_id is not None:
        mlflow.log_param("champion_run_id", champion_run_id)


def log_evaluation_artifacts(eval_result, role):
    """
    Log all diagnostic figures and the classification report produced by
    `07_Evaluation_Job.ipynb` into the active `MLflow` run.

    Figures (`.png`) are logged under `figures/<role>/`.
    The classification report is logged under `reports/<role>_classification_report.txt`.
    """
    figures_path = Path(eval_result["figures_local_path"])
    for filename in os.listdir(figures_path):
        if filename.endswith(".png"):
            mlflow.log_artifact(
                str(figures_path / filename),
                artifact_path = str(Path("figures") / role)
            )

    with open(eval_result["report_path"]) as fh:
        mlflow.log_text(
            fh.read(),
            str(Path("reports") / f"{role}_classification_report.txt")
        )


###############################################################################
# Cleanup
###############################################################################

def cleanup_temporary_artifacts(uc_volume_path):
    """
    Remove the temporary evaluation and training artefacts written to the
    `Unity Catalog` volume during the production run.

    Both the `evaluations/` and `runs/` subdirectories are deleted recursively.
    """
    base = Path(uc_volume_path)
    for subdirectory in ("evaluations", "runs"):
        path = str(base / subdirectory)
        dbutils.fs.rm(path, recurse = True)
        print(f"Temporary artefacts removed from {path}")

print("08_Utils.py script loaded successfully.")
