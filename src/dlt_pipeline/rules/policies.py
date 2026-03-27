def get_policies_rules():
    return [

        {
            "name": "policy_id_not_null",
            "constraint": "policy_id IS NOT NULL",
            "severity": "error",
            "tag": "policies"
        },

        {
            "name": "policyholder_age_valid",
            "constraint": "policyholder_age BETWEEN 18 AND 100",
            "severity": "error",
            "tag": "policies"
        },

        {
            "name": "annual_premium_positive",
            "constraint": "annual_premium_eur > 0",
            "severity": "error",
            "tag": "policies"
        },

        {
            "name": "vehicle_year_valid",
            "constraint": "vehicle_year >= 1980",
            "severity": "error",
            "tag": "policies"
        },

        {
            "name": "vehicle_value_non_negative",
            "constraint": "vehicle_value_eur >= 0",
            "severity": "error",
            "tag": "policies"
        },

        {
            "name": "annual_mileage_non_negative",
            "constraint": "annual_mileage_km >= 0",
            "severity": "error",
            "tag": "policies"
        },

        {
            "name": "bonus_malus_non_negative",
            "constraint": "bonus_malus_years >= 0",
            "severity": "error",
            "tag": "policies"
        },

        {
            "name": "region_type_valid",
            "constraint": """
                region_type IN (
                    'urban',
                    'suburban',
                    'rural'
                )
            """,
            "severity": "warn",
            "tag": "policies"
        }

    ]