def get_bodyshop_rules():
    return [

        {
            "name": "bodyshop_flag_binary",
            "constraint": "is_high_risk_body_shop IN (0, 1)",
            "severity": "error",
            "tag": "claims_enriched"
        }

    ]