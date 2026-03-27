def get_temporal_rules():
    return [

        {
            "name": "event_year_exists",
            "constraint": "event_year IS NOT NULL",
            "severity": "warn",
            "tag": "claims"
        },

        {
            "name": "event_month_exists",
            "constraint": "event_month IS NOT NULL",
            "severity": "warn",
            "tag": "claims"
        },

        {
            "name": "event_date_exists",
            "constraint": "event_date IS NOT NULL",
            "severity": "warn",
            "tag": "claims"
        },

        {
            "name": "label_available_date_valid",
            "constraint": "label_available_date >= event_date",
            "severity": "warn",
            "tag": "claims"
        }

    ]