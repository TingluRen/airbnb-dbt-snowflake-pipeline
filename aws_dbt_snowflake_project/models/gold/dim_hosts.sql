{{ config(materialized='table') }}

{% set configs = [
    {
        "table" : ref('silver_hosts'),
        "columns" : [
            "SILVER_hosts.HOST_ID",
            "SILVER_hosts.HOST_NAME",
            "SILVER_hosts.HOST_SINCE",
            "SILVER_hosts.IS_SUPERHOST",
            "SILVER_hosts.RESPONSE_RATE",
            "SILVER_hosts.RESPONSE_RATE_QUALITY",
            "SILVER_hosts.CREATED_AT AS HOST_CREATED_AT"
        ],
        "alias" : "SILVER_hosts"
    }
] %}

SELECT
    {% for cfg in configs %}
        {{ cfg['columns'] | join(',\n        ') }}
    {% endfor %}
FROM {{ configs[0]['table'] }} AS {{ configs[0]['alias'] }}
