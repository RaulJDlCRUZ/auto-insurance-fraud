def get_rules() -> list[dict]:
    """
    Reglas de calidad declarativas para bronze_claims -> silver_claims.
    Cada regla tiene:
      - name:       identificador único, snake_case
      - constraint: expresión SQL válida (devuelve boolean)
      - tag:        tabla a la que pertenece (para filtrado en el pipeline)
    """
    return [
        # ------------------------------------------------------------------ #
        # 1. NULIDAD — campos obligatorios para cualquier join o cálculo      #
        # ------------------------------------------------------------------ #
        {
            "name": "claim_id_not_null",
            "constraint": "claim_id IS NOT NULL",
            "tag": "claims",
        },
        {
            "name": "policy_id_not_null",
            "constraint": "policy_id IS NOT NULL",
            "tag": "claims",
        },
        {
            "name": "timestamp_not_null",
            "constraint": "timestamp IS NOT NULL",
            "tag": "claims",
        },
        {
            "name": "claimed_amount_not_null",
            "constraint": "claimed_amount_eur IS NOT NULL",
            "tag": "claims",
        },
        # ------------------------------------------------------------------ #
        # 2. DOMINIO — valores dentro de rangos o conjuntos válidos           #
        # ------------------------------------------------------------------ #
        {
            "name": "claimed_amount_positive",
            "constraint": "claimed_amount_eur > 0",
            "tag": "claims",
        },
        {
            "name": "days_to_report_non_negative",
            "constraint": "days_to_report >= 0",
            "tag": "claims",
        },
        {
            "name": "n_parties_involved_positive",
            "constraint": "n_parties_involved >= 1",
            "tag": "claims",
        },
        {
            "name": "injury_level_valid",
            "constraint": "injury_level IN ('none', 'minor', 'moderate', 'severe')",
            "tag": "claims",
        },
        {
            "name": "witnesses_valid",
            "constraint": "witnesses IN ('none', 'one', 'multiple')",
            "tag": "claims",
        },
        {
            "name": "claim_channel_valid",
            "constraint": "claim_channel IN ('phone', 'web', 'app_selfservice', 'agent', 'online')",
            "tag": "claims",
        },
        # ------------------------------------------------------------------ #
        # 3. LÓGICA — consistencia interna del registro                       #
        # ------------------------------------------------------------------ #
        {
            # Si hay anomalía telemática, el flag de telematics en la póliza
            # debe ser 1. Esta regla se evalúa tras el join con policies.
            "name": "telematics_anomaly_requires_device",
            "constraint": "NOT (telematics_anomaly = 1 AND has_telematics = 0)",
            "tag": "claims",
        },
        {
            # Un siniestro con lesiones a terceros requiere al menos 2 partes.
            "name": "third_party_injury_requires_parties",
            "constraint": "NOT (has_third_party_injury = 1 AND n_parties_involved < 2)",
            "tag": "claims",
        },
    ]