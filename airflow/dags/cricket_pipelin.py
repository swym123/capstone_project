
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
from pathlib import Path
import csv




def load_csv_files():

    SNOWFLAKE_CONN_ID = "snowflake_conn"
    DATA_PATH = Path("/opt/airflow/data")
    DATABASE = "cricket_DB"
    SCHEMA = "RAW"
    STAGE = "RAW_STAGE"
    TABLE = "RAW_TABLE"
    FILE_FORMAT = "INSERT_DATA_FILE_FORMAT"


    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        print('CHECKING DATABASE IS EXIST OR NOT ')
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
        cursor.execute(f"USE DATABASE {DATABASE}")

        schemas = list([SCHEMA, "BRONZE", "SILVER", "GOLD"])
        for s in schemas:
            print('CHECKING SCHEMA IS EXIST OR NOT ')
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.{s}")

        print('CHECKING FILE FORMAT IS EXIST OR NOT ')
        cursor.execute(f"CREATE STAGE IF NOT EXISTS {DATABASE}.{SCHEMA}.{STAGE}")
        cursor.execute(f"""
        CREATE FILE FORMAT IF NOT EXISTS {FILE_FORMAT}
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        NULL_IF = ('NULL', 'null')
        EMPTY_FIELD_AS_NULL = TRUE
        """)



        files = list(DATA_PATH.glob("*.csv"))
        COL = ( "delivery_id" , "delivery_timestamp",
        "innings_no",
        "over_no",
        'ball_in_over',
        'runs_off_bat',
        'extra_type',
        'extra_runs',
        'total_runs',
        'is_wicket',
        'dismissal_type',
        'ball_speed_kmph',
        'non_striker_id',
        'shot_type',
        'shot_region',
        'match_id',
        'match_date',
        'match_format',
        'tournament_name',
        'season',
        'match_stage',
        'toss_decision',
        'venue_id',
        'venue_name',
        'venue_city',
        'venue_country',
        'batting_team_id',
        'batting_team_name',
        'bowling_team_id',
        'bowling_team_name',
        'batsman_id',
        'batsman_name',
        'batsman_dob',
        'batsman_nationality',
        'batting_hand',
        'batsman_role',
        'batsman_email',
        'batsman_phone',
        'batsman_icc_rating',
        'bowler_id',
        'bowler_name',
        'bowler_nationality',
        'bowling_style',
        'bowler_icc_rating',
        'contract_id',
        'contract_grade',
        'annual_retainer_usd',
        'contract_status',
        'drs_review_id',
        'drs_reviewed_by',
        'drs_original_decision',
        'drs_outcome',
        )

        if not files:
            print("No CSV files found.")
            return

        
        column_sql = ", ".join(f'"{column}" VARCHAR'for column in COL)


        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS
        {DATABASE}.{SCHEMA}.{TABLE}
        ({column_sql},
          _LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())"""
        cursor.execute(create_table_sql)

        # PIUTTING INTO STAGE 
        for file in files:

            print(f"\nProcessing: {file.name}")

            put_sql = f"""
            PUT 'file://{file}'
            @{DATABASE}.{SCHEMA}.{STAGE}
            AUTO_COMPRESS=FALSE
            OVERWRITE=FALSE"""
            
            cursor.execute(put_sql)
            print(f"Uploaded to stage: {file.name}")

        target_columns = ", ".join(f'"{column}"' for column in COL)
        select_columns = ", ".join(f"${i}" for i in range(1, len(COL) + 1))

        copy_sql = f"""
        COPY INTO {DATABASE}.{SCHEMA}.{TABLE}
        ({target_columns},
          _LOADED_AT)
        FROM
        (SELECT
            {select_columns},
            CURRENT_TIMESTAMP()
        FROM @{DATABASE}.{SCHEMA}.{STAGE})
        FILE_FORMAT = (FORMAT_NAME = '{FILE_FORMAT}')
        PATTERN = '.*[.]csv'
        ON_ERROR = 'ABORT_STATEMENT'
        FORCE = FALSE
        """
        cursor.execute(copy_sql)
        results = cursor.fetchall()

    finally:
        cursor.close()
        conn.close()


with DAG(
dag_id="cricket_pipeline",
default_args={
"retries": 3,
"retry_delay": timedelta(minutes=1)
},
start_date=datetime(2026, 1, 1),
schedule=None,
catchup=False,
tags=["exam"],
) as dag:

    load_raw_data = PythonOperator(
        task_id="load_csv_to_raw",
        python_callable=load_csv_files,)
    
    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command="""
        cd "/airflow/dbt/analytics"
        dbt build \
          --project-dir "/airflow/dbt/analytics" \
          --profiles-dir "/airflow/.dbt" \
          --select "path:models/BRONZE"
        """,
    )

    load_raw_data >> dbt_staging  
