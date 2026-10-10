{{ config(
    schema = 'GOLD',
    materialized = 'table'
) }}


SELECT DISTINCT

    venue_id,
    venue_name,
    venue_city,
    venue_country

FROM {{ ref("TRANSFORM") }}



