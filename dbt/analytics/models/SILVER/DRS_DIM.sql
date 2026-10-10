{{ config(
    schema = 'GOLD',
    materialized = 'table'
) }}

SELECT DISTINCT
    drs_review_id,
    drs_reviewed_by,
    drs_original_decision,
    drs_outcome

FROM {{ ref("TRANSFORM") }}




