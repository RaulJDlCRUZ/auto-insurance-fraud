def get_labels_rules():
    return [

        {
            "name": "label_claim_id_not_null",
            "constraint": "claim_id IS NOT NULL",
            "severity": "error",
            "tag": "labels"
        },

        {
            "name": "label_is_fraud_binary",
            "constraint": "is_fraud IN (0, 1)",
            "severity": "error",
            "tag": "labels"
        },

        {
            "name": "label_available_date_not_null",
            "constraint": "label_available_date IS NOT NULL",
            "severity": "error",
            "tag": "labels"
        },

        # {
        #     "name": "label_after_claim_timestamp",
        #     "constraint": "label_available_date >= timestamp",
        #     "severity": "warn",
        #     "tag": "labels"
        # }

    ]