def get_rules() -> list[dict]:
    """
    Reglas de calidad para bronze_labels -> silver_labels.
    Tabla simple pero crítica: una etiqueta huérfana o con fecha inválida
    contaminaría el conjunto de entrenamiento del modelo.
    """
    return [
        # ------------------------------------------------------------------ #
        # 1. NULIDAD                                                          #
        # ------------------------------------------------------------------ #
        {
            "name": "label_claim_id_not_null",
            "constraint": "claim_id IS NOT NULL",
            "tag": "labels",
        },
        {
            "name": "is_fraud_not_null",
            "constraint": "is_fraud IS NOT NULL",
            "tag": "labels",
        },
        {
            "name": "label_available_date_not_null",
            "constraint": "label_available_date IS NOT NULL",
            "tag": "labels",
        },
        # ------------------------------------------------------------------ #
        # 2. DOMINIO                                                          #
        # ------------------------------------------------------------------ #
        {
            # El único valor válido es 0 o 1 — no admitimos nada más.
            "name": "is_fraud_binary",
            "constraint": "is_fraud IN (0, 1)",
            "tag": "labels",
        },
    ]