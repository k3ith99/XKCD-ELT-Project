{{ config(
    materialized='table',
    schema='gold'
) }}

{% do ref('fact_metrics') %}
{% do ref('dim_comics') %}
{% do ref('dim_date') %}

SELECT
    fact_metrics.id AS metrics_id,
    fact_metrics.cost AS cost,
    fact_metrics.views AS views,
    fact_metrics.customer_reviews AS customer_reviews,
    dim_comics.title AS title,
    dim_comics.news_text AS news_text,
    dim_comics.safe_title AS safe_title,
    dim_comics.transcript AS transcript,
    dim_comics.alt_text AS alt_text,
    dim_comics.img_url AS img_url,
    dim_date.year AS year,
    dim_date.month AS month,
    dim_date.day AS day
FROM {{ ref('fact_metrics') }} AS fact_metrics
INNER JOIN {{ ref('dim_comics') }} AS dim_comics
    ON fact_metrics.comic_key = dim_comics.id
INNER JOIN {{ ref('dim_date') }} AS dim_date
    ON fact_metrics.date_key = dim_date.date_key

