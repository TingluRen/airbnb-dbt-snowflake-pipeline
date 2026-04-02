SELECT *
FROM {{ ref('fact_bookings') }}
WHERE total_amount < 0