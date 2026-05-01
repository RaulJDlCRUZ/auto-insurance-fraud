def get_rules() -> list[dict]:
    """
    Reglas de integridad referencial entre tablas.
    A diferencia del resto, estas reglas NO se evalúan sobre una tabla aislada
    sino sobre el resultado de un join. El pipeline las aplica en una fase
    posterior, tras tener claims y policies ya limpias en silver.
    """
    return [
        {
            # Todo siniestro debe referenciar una póliza que exista en silver.
            # Se evalúa sobre el left join claims <-> policies;
            # si policy_id no matchea, el campo 'policy_found' será NULL.
            "name": "claim_has_valid_policy",
            "constraint": "policy_found IS NOT NULL",
            "tag": "integrity_claims",
        },
        {
            # Toda etiqueta debe estar vinculada a un siniestro cargado.
            # Se evalúa sobre el left join labels <-> claims.
            "name": "label_has_valid_claim",
            "constraint": "claim_found IS NOT NULL",
            "tag": "integrity_labels",
        },
    ]