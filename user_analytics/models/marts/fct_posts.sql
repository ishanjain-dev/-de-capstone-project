{{
    config(
        materialized='table'
    )
}}

SELECT
    p.post_id,
    p.user_id,
    p.title,
    COALESCE(c.number_of_comments, 0) as number_of_comments
FROM {{ ref('stg_posts') }} p
LEFT JOIN {{ ref('int_comments_per_post') }} c
    ON p.post_id = c.post_id