{{ config(materialized='table') }}

{% set configs = [
    {
        "table" : ref('silver_bookings'),
        "columns" : [
            "SILVER_bookings.BOOKING_ID",
            "SILVER_bookings.LISTING_ID",
            "SILVER_listings.HOST_ID",
            "SILVER_bookings.BOOKING_DATE",
            "SILVER_bookings.TOTAL_AMOUNT",
            "SILVER_bookings.SERVICE_FEE",
            "SILVER_bookings.CLEANING_FEE",
            "SILVER_bookings.BOOKING_STATUS",
            "SILVER_bookings.CREATED_AT AS BOOKING_CREATED_AT"
        ],
        "alias" : "SILVER_bookings"
    },
    {
        "table" : ref('silver_listings'),
        "columns" : [],
        "alias" : "SILVER_listings",
        "join_condition" : "SILVER_bookings.LISTING_ID = SILVER_listings.LISTING_ID"
    }
] %}

SELECT
    {%- for cfg in configs %}
        {%- if cfg['columns'] | length > 0 %}
            {{ cfg['columns'] | join(',\n        ') }}
            {%- if not loop.last %},{% endif %}
        {%- endif %}
    {%- endfor %}
FROM
    {% for cfg in configs %}
        {% if loop.first %}
            {{ cfg['table'] }} AS {{ cfg['alias'] }}
        {% else %}
            LEFT JOIN {{ cfg['table'] }} AS {{ cfg['alias'] }}
                ON {{ cfg['join_condition'] }}
        {% endif %}
    {% endfor %}
