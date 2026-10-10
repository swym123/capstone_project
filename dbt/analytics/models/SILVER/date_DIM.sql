{{ config(
    schema = 'CORE',
    materialized = 'table'
) }}

WITH date_spine AS (
    SELECT DATEADD(day, SEQ4(), '2025-01-01'::DATE) AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 10000))
)

SELECT
    TO_NUMBER(TO_CHAR(date_day, 'YYYYMMDD')) AS DATE_KEY,
    date_day AS DATE_DAY,
    YEAR(date_day) AS YEAR,
    QUARTER(date_day) AS QUARTER,
    MONTH(date_day) AS MONTH,
    MONTHNAME(date_day) AS MONTH_NAME,
    WEEK(date_day) AS WEEK_OF_YEAR,
    DAY(date_day) AS DAY_OF_MONTH,
    DAYOFWEEK(date_day) AS DAY_OF_WEEK,
    CASE WHEN DAYOFWEEKISO(date_day) IN (6, 7)  THEN TRUE ELSE FALSE END AS IS_WEEKEND

FROM date_spine