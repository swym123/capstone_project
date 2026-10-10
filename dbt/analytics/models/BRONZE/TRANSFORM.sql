{{ config(
    schema = 'SILVER',
    materialized = 'table'
) }}


WITH source_data AS (
    SELECT
        "delivery_id" AS delivery_id,
        "delivery_timestamp" AS delivery_timestamp,
        "innings_no" AS innings_no,
        "over_no" AS over_no,
        "ball_in_over" AS ball_in_over,
        "runs_off_bat" AS runs_off_bat,
        "extra_type" AS extra_type,
        "extra_runs" AS extra_runs,
        "total_runs" AS total_runs,
        "is_wicket" AS is_wicket,
        "dismissal_type" AS dismissal_type,
        "ball_speed_kmph" AS ball_speed_kmph,
        "non_striker_id" AS non_striker_id,
        "shot_type" AS shot_type,
        "shot_region" AS shot_region,
        "match_id" AS match_id,
        "match_date" AS match_date,
        "match_format" AS match_format,
        "tournament_name" AS tournament_name,
        "season" AS season,
        "match_stage" AS match_stage,
        "toss_decision" AS toss_decision,
        "venue_id" AS venue_id,
        "venue_name" AS venue_name,
        "venue_city" AS venue_city,
        "venue_country" AS venue_country,
        "batting_team_id" AS batting_team_id,
        "batting_team_name" AS batting_team_name,
        "bowling_team_id" AS bowling_team_id,
        "bowling_team_name" AS bowling_team_name,
        "batsman_id" AS batsman_id,
        "batsman_name" AS batsman_name,
        "batsman_dob" AS batsman_dob,
        "batsman_nationality" AS batsman_nationality,
        "batting_hand" AS batting_hand,
        "batsman_role" AS batsman_role,
        "batsman_email" AS batsman_email,
        "batsman_phone" AS batsman_phone,
        "batsman_icc_rating" AS batsman_icc_rating,
        "bowler_id" AS bowler_id,
        "bowler_name" AS bowler_name,
        "bowler_nationality" AS bowler_nationality,
        "bowling_style" AS bowling_style,
        "bowler_icc_rating" AS bowler_icc_rating,
        "contract_id" AS contract_id,
        "contract_grade" AS contract_grade,
        "annual_retainer_usd" AS annual_retainer_usd,
        "contract_status" AS contract_status,
        "drs_review_id" AS drs_review_id,
        "drs_reviewed_by" AS drs_reviewed_by,
        "drs_original_decision" AS drs_original_decision,
        "drs_outcome" AS drs_outcome,
        "_LOADED_AT" AS _LOADED_AT
    FROM {{ source('raw', 'RAW_TABLE') }}
)

SELECT
    TRIM(delivery_id) AS delivery_id,

    COALESCE(
        TRY_TO_TIMESTAMP_NTZ(
            TRIM(delivery_timestamp),
            'YYYY-MM-DD HH24:MI:SS'
        ),
        TRY_TO_TIMESTAMP_NTZ(
            TRIM(delivery_timestamp),
            'DD-MM-YYYY HH24:MI'
        ),
        TRY_TO_TIMESTAMP_NTZ(
            TRIM(delivery_timestamp),
            'MM/DD/YYYY HH24:MI'
        ),
        TRY_TO_TIMESTAMP_NTZ(
            TRIM(delivery_timestamp)
        )
    ) AS delivery_timestamp,

    TRY_CAST(TRIM(innings_no) AS INTEGER) AS innings_no,
    TRY_CAST(TRIM(over_no) AS INTEGER) AS over_no,
    TRY_CAST(TRIM(ball_in_over) AS INTEGER) AS ball_in_over,
    TRY_CAST(TRIM(runs_off_bat) AS INTEGER) AS runs_off_bat,

    NULLIF(TRIM(extra_type), '') AS extra_type,

    COALESCE(
        TRY_CAST(TRIM(extra_runs) AS INTEGER),
        0
    ) AS extra_runs,

    TRY_CAST(TRIM(total_runs) AS INTEGER) AS total_runs,

    CASE
        WHEN UPPER(TRIM(is_wicket)) IN (
            'TRUE', 'Y', 'YES', '1', 'OUT'
        ) THEN TRUE

        WHEN UPPER(TRIM(is_wicket)) IN (
            'FALSE', 'N', 'NO', '0'
        ) THEN FALSE

        ELSE FALSE
    END AS is_wicket,

    NULLIF(TRIM(dismissal_type), '') AS dismissal_type,

    TRY_TO_DECIMAL(
        TRIM(ball_speed_kmph), 6, 2
    ) AS ball_speed_kmph,

    TRIM(non_striker_id) AS non_striker_id,
    NULLIF(TRIM(shot_type), '') AS shot_type,
    NULLIF(TRIM(shot_region), '') AS shot_region,
    TRIM(match_id) AS match_id,

    COALESCE(
        TRY_TO_DATE(TRIM(match_date), 'YYYY-MM-DD'),
        TRY_TO_DATE(TRIM(match_date), 'DD-MM-YYYY'),
        TRY_TO_DATE(TRIM(match_date), 'MM/DD/YYYY'),
        TRY_TO_DATE(TRIM(match_date))
    ) AS match_date,

    UPPER(TRIM(match_format)) AS match_format,
    TRIM(tournament_name) AS tournament_name,
    TRY_CAST(TRIM(season) AS INTEGER) AS season,
    TRIM(match_stage) AS match_stage,
    INITCAP(TRIM(toss_decision)) AS toss_decision,

    TRIM(venue_id) AS venue_id,
    TRIM(venue_name) AS venue_name,
    INITCAP(TRIM(venue_city)) AS venue_city,
    INITCAP(TRIM(venue_country)) AS venue_country,

    TRIM(batting_team_id) AS batting_team_id,
    TRIM(batting_team_name) AS batting_team_name,
    TRIM(bowling_team_id) AS bowling_team_id,
    TRIM(bowling_team_name) AS bowling_team_name,

    TRIM(batsman_id) AS batsman_id,
    INITCAP(TRIM(batsman_name)) AS batsman_name,

    COALESCE(
        TRY_TO_DATE(TRIM(batsman_dob), 'YYYY-MM-DD'),
        TRY_TO_DATE(TRIM(batsman_dob), 'DD-MM-YYYY'),
        TRY_TO_DATE(TRIM(batsman_dob), 'MM/DD/YYYY'),
        TRY_TO_DATE(TRIM(batsman_dob))
    ) AS batsman_dob,

    INITCAP(TRIM(batsman_nationality)) AS batsman_nationality,
    TRIM(batting_hand) AS batting_hand,
    INITCAP(TRIM(batsman_role)) AS batsman_role,
    LOWER(TRIM(batsman_email)) AS batsman_email,
    TRIM(batsman_phone) AS batsman_phone,

    TRY_CAST(
        TRIM(batsman_icc_rating) AS INTEGER
    ) AS batsman_icc_rating,

    TRIM(bowler_id) AS bowler_id,
    INITCAP(TRIM(bowler_name)) AS bowler_name,
    INITCAP(TRIM(bowler_nationality)) AS bowler_nationality,
    TRIM(bowling_style) AS bowling_style,

    TRY_CAST(
        TRIM(bowler_icc_rating) AS INTEGER
    ) AS bowler_icc_rating,

    NULLIF(TRIM(contract_id), '') AS contract_id,
    NULLIF(TRIM(contract_grade), '') AS contract_grade,

    TRY_TO_DECIMAL(
        TRIM(annual_retainer_usd), 18, 2
    ) AS annual_retainer_usd,

    NULLIF(
        INITCAP(TRIM(contract_status)), ''
    ) AS contract_status,

    NULLIF(TRIM(drs_review_id), '') AS drs_review_id,
    NULLIF(TRIM(drs_reviewed_by), '') AS drs_reviewed_by,

    NULLIF(
        INITCAP(TRIM(drs_original_decision)), ''
    ) AS drs_original_decision,

    NULLIF(
        INITCAP(TRIM(drs_outcome)), ''
    ) AS drs_outcome,

    _LOADED_AT AS _LOADED_AT

FROM source_data

