{{ config(materialized='table') }}

{% set configs = [
    {
        "table" : ref('silver_listings'),
        "columns" : [
            "SILVER_listings.LISTING_ID",
            "SILVER_listings.HOST_ID",
            "SILVER_listings.PROPERTY_TYPE",
            "SILVER_listings.ROOM_TYPE",
            "SILVER_listings.CITY",
            "SILVER_listings.COUNTRY",
            "SILVER_listings.ACCOMMODATES",
            "SILVER_listings.BEDROOMS",
            "SILVER_listings.BATHROOMS",
            "SILVER_listings.PRICE_PER_NIGHT",
            "SILVER_listings.PRICE_PER_NIGHT_TAG",
            "SILVER_listings.CREATED_AT AS LISTING_CREATED_AT"
        ],
        "alias" : "SILVER_listings"
    }
] %}

SELECT
    {% for cfg in configs %}
        {{ cfg['columns'] | join(',\n        ') }}
    {% endfor %}
FROM {{ configs[0]['table'] }} AS {{ configs[0]['alias'] }}
