{{ config(
    schema = 'GOLD',
    materialized = 'table'
) }}

SELECT DISTINCT

match_id,
match_date,
match_format,
tournament_name,
season,
match_stage,
toss_decision,

FROM {{ ref("TRANSFORM") }}
