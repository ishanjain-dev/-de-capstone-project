{{
    config(
        materialized='table'
    )
}}

SELECT
    post_id,
    COUNT(comment_id) as number_of_comments
FROM {{ ref('stg_comments') }}
GROUP BY post_id