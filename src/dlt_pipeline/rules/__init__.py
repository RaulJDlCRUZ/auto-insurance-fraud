# src/dlt_pipeline/rules/__init__.py

from .claims import get_rules as _claims_rules
from .policies import get_rules as _policies_rules
from .labels import get_rules as _labels_rules
from .integrity import get_rules as _integrity_rules

# Acción por defecto si una regla no especifica la suya.
_DEFAULT_ACTION = "quarantine"

# Acción asignada a cada regla por nombre.
# - "drop":        el registro se descarta silenciosamente (dato irrecuperable)
# - "quarantine":  el registro se desvía a tabla _quarantine para revisión
# - "warn":        el registro pasa, pero se loguea la anomalía
_RULE_ACTIONS: dict[str, str] = {
    # claims — nulidad en campos clave: sin ellos el registro es inútil
    "claim_id_not_null":                    "drop",
    "policy_id_not_null":                   "drop",
    "timestamp_not_null":                   "drop",
    "claimed_amount_not_null":              "drop",

    # claims — dominio
    "claimed_amount_positive":              "quarantine",
    "days_to_report_non_negative":          "quarantine",
    "n_parties_involved_positive":          "quarantine",
    "injury_level_valid":                   "quarantine",
    "witnesses_valid":                      "quarantine",
    "claim_channel_valid":                  "warn",      # canal nuevo = posible drift, no error

    # claims — lógica
    "telematics_anomaly_requires_device":   "quarantine",
    "third_party_injury_requires_parties":  "quarantine",

    # policies — nulidad
    # "policy_id_not_null":                   "drop",    # repeated key literal
    "policy_start_date_not_null":           "drop",
    "vehicle_value_not_null":               "quarantine",

    # policies — dominio
    "policyholder_age_valid":               "quarantine",
    "vehicle_year_valid":                   "quarantine",
    "vehicle_value_positive":               "quarantine",
    "annual_premium_positive":              "quarantine",
    "region_type_valid":                    "quarantine",
    "coverage_type_valid":                  "quarantine",

    # policies — lógica
    "updated_at_after_start_date":          "warn",

    # labels
    "label_claim_id_not_null":              "drop",
    "is_fraud_not_null":                    "drop",
    "label_available_date_not_null":        "drop",
    "is_fraud_binary":                      "quarantine",

    # integridad referencial
    "claim_has_valid_policy":               "quarantine",
    "label_has_valid_claim":                "quarantine",
}


def get_rules_for(tag: str) -> list[dict]:
    """
    Devuelve todas las reglas de un tag dado, con su acción inyectada.

    Uso:
        from src.dlt_pipeline.rules import get_rules_for
        claims_rules = get_rules_for("claims")
    """
    all_rules = (
        _claims_rules()
        + _policies_rules()
        + _labels_rules()
        + _integrity_rules()
    )
    return [
        {**rule, "action": _RULE_ACTIONS.get(rule["name"], _DEFAULT_ACTION)}
        for rule in all_rules
        if rule["tag"] == tag
    ]


def get_all_rules() -> list[dict]:
    """Devuelve todas las reglas de todas las entidades con su acción."""
    all_rules = (
        _claims_rules()
        + _policies_rules()
        + _labels_rules()
        + _integrity_rules()
    )
    return [
        {**rule, "action": _RULE_ACTIONS.get(rule["name"], _DEFAULT_ACTION)}
        for rule in all_rules
    ]