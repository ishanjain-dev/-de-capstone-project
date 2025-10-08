{{
    config(
        materialized='view'
    )
}}

SELECT
    raw_json:userId::INTEGER as user_id,
    raw_json:id::INTEGER as post_id,
    raw_json:title::STRING as title,
    raw_json:body::STRING as body
FROM {{ source('raw_data', 'posts') }}