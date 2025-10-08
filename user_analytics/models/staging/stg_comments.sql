{{
    config(
        materialized='view'
    )
}}

SELECT
    raw_json:postId::INTEGER as post_id,
    raw_json:id::INTEGER as comment_id,
    raw_json:name::STRING as name,
    raw_json:email::STRING as email,
    raw_json:body::STRING as body
FROM {{ source('raw_data', 'comments') }}