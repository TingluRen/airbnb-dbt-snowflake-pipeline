{{ config(materialized='view') }}

SELECT 
    BOOKING_ID,
    BOOKING_DATE,
    BOOKING_STATUS,
    CREATED_AT
FROM {{ ref('obt') }}