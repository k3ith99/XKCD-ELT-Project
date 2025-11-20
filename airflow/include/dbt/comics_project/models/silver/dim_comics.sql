{{ config(
    materialized='table',
    schema='silver'
) }}

{% do source('raw','raw_comics') %}

SELECT
    num AS id, 
    news AS news_text,
    safe_title AS safe_title,
    transcript AS transcript,
    alt AS alt_text,
    img AS img_url,
    title AS title
FROM {{ source('raw', 'raw_comics') }} 
WHERE num IS NOT NULL