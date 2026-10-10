{{ config(
    schema = 'GOLD',
    materialized = 'table'
) }}




with data as(

SELECT
    batting_team_id as team_id,
    batting_team_name as team_name

    FROM {{ ref("TRANSFORM") }}

union all 

SELECT
    bowling_team_id,
    bowling_team_name 

    FROM {{ ref("TRANSFORM") }}

)


SELECT 
    team_id , team_name 
from  data
group by  team_id , team_name 



