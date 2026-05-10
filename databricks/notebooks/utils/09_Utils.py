"""
Shared utilities for the production inference and label enrichment pipeline.
Adapted from the credit card fraud reference project to the auto insurance
fraud domain.

Key changes vs. original:
  - entity_key      : 'customer_id' -> 'policy_id'
  - timestamp_key   : 'timestamp'   -> 'claim_timestamp'
  - profile_feature_names   : credit card profile -> policyholder/vehicle profile
  - aggregation_feature_names: card velocity features -> claims rolling windows
  - customer_agg_table: gold_customer_aggregations_inference (new table)
"""


###############################################################################
# Imports
###############################################################################

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup


###############################################################################
# Table configuration
###############################################################################

spine_table             = f"{catalog}.{database}.fraud_inference_spine"
customer_profile_table  = f"{catalog}.{database}.customer_profile"
customer_agg_table      = f"{catalog}.{database}.customer_aggregations_inference"
inference_enriched_table = f"{catalog}.{database}.gold_fraud_inference_enriched"
# fraud_labels_table      = f"{catalog}.{database}.bronze_labels"
fraud_labels_table      = f"{catalog}.{database}.fraud_spine"


###############################################################################
# Feature store configuration
###############################################################################

# Lookup key: policy_id is the customer identifier in the auto insurance domain.
# In the reference project this was customer_id (credit cards); here each
# policyholder is identified by their policy_id.
entity_key    = "policy_id"
timestamp_key = "claim_timestamp"

# ---------------------------------------------------------------------------
# Profile features
# Subset of customer_profile columns used by the champion model.
# Excludes SCD2 control columns (__START_AT, __END_AT, is_current) and
# audit columns (gold_profile_timestamp) which are not model features.
# Must match exactly the columns present in fraud_training_dataset.
# ---------------------------------------------------------------------------
profile_feature_names = [
    # Policyholder demographics
    "policyholder_age",
    "age_group",
    "gender",
    "region",
    "region_type",
    "occupation",
    # Policy and financial profile
    "coverage_type",
    "payment_frequency",
    "annual_premium_eur",
    "bonus_malus_years",
    "policy_tenure_days",
    "premium_per_day",
    "has_telematics",
    "multi_policy",
    # Vehicle characteristics
    "vehicle_make",
    "vehicle_type",
    "vehicle_year",
    "is_electric",
    "annual_mileage_km",
    "vehicle_age",
    "vehicle_age_group",
    "vehicle_value_eur",
    "high_mileage_flag",
    # Risk score
    "risk_score_static",
]

# ---------------------------------------------------------------------------
# Aggregation features
# Rolling-window behavioural metrics per policyholder (policy_id).
# Computed in customer_aggregations_inference with the same rangeBetween(-X,-1)
# logic as the training table, guaranteeing no training-serving skew.
# Columns that may be null for first-claim policyholders are covered by
# fillna(0) in the inference pipeline before VectorAssembler.
# ---------------------------------------------------------------------------
aggregation_feature_names = [
    # 1-hour window
    "num_claims_1h",
    "total_amount_1h",
    "avg_amount_1h",
    "num_telematics_1h",
    # 24-hour window
    "num_claims_24h",
    "total_amount_24h",
    "avg_amount_24h",
    "num_telematics_24h",
    # 7-day window
    "num_claims_7d",
    "total_amount_7d",
    "avg_amount_7d",
    "num_telematics_7d",
    "num_fraud_confirmed_7d",
    # 30-day window
    "num_claims_30d",
    "total_amount_30d",
    "avg_amount_30d",
    "num_telematics_30d",
    "num_fraud_confirmed_30d",
    "num_unique_shops_30d",
    # Derived metrics (may be null → fillna(0) downstream)
    "amount_ratio_24h_vs_30d",
    "fraud_rate_30d",
]

# ---------------------------------------------------------------------------
# FeatureLookup definitions
#
# customer_profile uses SCD Type 2 (__START_AT / __END_AT), so the
# timestamp_lookup_key ensures point-in-time correctness: the profile version
# active at claim_timestamp is retrieved, not the current one.
#
# customer_aggregations_inference uses claim_timestamp as its own timestamp
# key (registered in 03b), so the point-in-time join is handled natively
# by the Feature Store.
# ---------------------------------------------------------------------------
profile_lookup = FeatureLookup(
    table_name         = customer_profile_table,
    feature_names      = profile_feature_names,
    lookup_key         = entity_key,
    timestamp_lookup_key = timestamp_key,
)

aggregations_lookup = FeatureLookup(
    table_name         = customer_agg_table,
    feature_names      = aggregation_feature_names,
    lookup_key         = entity_key,
    timestamp_lookup_key = timestamp_key,
)

feature_lookups = [profile_lookup, aggregations_lookup]

print(f"Profile features      ({len(profile_feature_names):>2}): {profile_feature_names}")
print(f"Aggregation features  ({len(aggregation_feature_names):>2}): {aggregation_feature_names}")
print(f"Total feature columns    : {len(profile_feature_names) + len(aggregation_feature_names)}")
print()
print("09_Utils.py script loaded successfully.")
