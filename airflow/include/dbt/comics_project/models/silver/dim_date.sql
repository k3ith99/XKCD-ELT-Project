{{ config(
    materialized='table',
    schema='silver'
) }}

{% do source('raw','raw_comics') %}

SELECT DISTINCT
    (year * 10000 + month * 100 + day)::INT AS date_key, --YYYYMMDD
    year,
    month,
    day
FROM {{ source('raw', 'raw_comics') }}  -- Reading from raw_comics table
