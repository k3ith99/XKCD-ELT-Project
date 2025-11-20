{{ config(
    materialized='table',
    schema='silver'
) }}

{% do ref('dim_comics') %}
{% do ref('dim_date') %}

SELECT
    ROW_NUMBER() OVER (ORDER BY num) AS id,
    num AS comic_key,
    (year * 10000 + month * 100 + day)::INT AS date_key,
    LENGTH(REGEXP_REPLACE(title, '[^A-Za-z]', '', 'g')) * 5 AS cost,
    FLOOR(RANDOM() * 10000)::INT AS views,
    RANDOM() * 9 + 1 AS customer_reviews
FROM {{ source('raw', 'raw_comics') }}
