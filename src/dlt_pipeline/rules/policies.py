def get_rules() -> list[dict]:
    """
    Reglas de calidad declarativas para bronze_policies -> silver_policies.
    Nota: bronze_policies llegó con todos los campos numéricos como string.
    Estas reglas se aplican DESPUÉS del casting en la transformación.
    """
    return [
        # ------------------------------------------------------------------ #
        # 1. NULIDAD                                                          #
        # ------------------------------------------------------------------ #
        {
            "name": "policy_id_not_null",
            "constraint": "policy_id IS NOT NULL",
            "tag": "policies",
        },
        {
            "name": "policy_start_date_not_null",
            "constraint": "policy_start_date IS NOT NULL",
            "tag": "policies",
        },
        {
            "name": "vehicle_value_not_null",
            "constraint": "vehicle_value_eur IS NOT NULL",
            "tag": "policies",
        },
        # ------------------------------------------------------------------ #
        # 2. DOMINIO — rangos demográficos y categóricos                      #
        # ------------------------------------------------------------------ #
        {
            "name": "policyholder_age_valid",
            "constraint": "policyholder_age BETWEEN 18 AND 100",
            "tag": "policies",
        },
        {
            "name": "vehicle_year_valid",
            "constraint": "vehicle_year BETWEEN 1980 AND 2026",
            "tag": "policies",
        },
        {
            "name": "vehicle_value_positive",
            "constraint": "vehicle_value_eur > 0",
            "tag": "policies",
        },
        {
            "name": "annual_premium_positive",
            "constraint": "annual_premium_eur > 0",
            "tag": "policies",
        },
        {
            "name": "region_type_valid",
            "constraint": "region_type IN ('urban', 'suburban', 'rural')",
            "tag": "policies",
        },
        {
            "name": "coverage_type_valid",
            "constraint": "coverage_type IN ('third_party', 'third_party_plus', 'comprehensive')",
            "tag": "policies",
        },
        # ------------------------------------------------------------------ #
        # 3. LÓGICA                                                           #
        # ------------------------------------------------------------------ #
        {
            # La póliza no puede haberse actualizado antes de su inicio.
            "name": "updated_at_after_start_date",
            "constraint": "policy_updated_at >= policy_start_date",
            "tag": "policies",
        },
    ]