SELECT *
FROM {{ ref('fact_bookings') }}
WHERE booking_date > current_date