{{
  config(
    materialized='incremental',
    pre_hook="
      DELETE FROM {{this}}
      WHERE updated_on BETWEEN '{{ var('min_date') }}' AND '{{ var('max_date') }}';
    "
  )
}}

WITH raw_crimes AS (
    SELECT *
    FROM {{ source('chicago_crimes', 'crimes') }}
)

SELECT *
FROM raw_crimes
{% if is_incremental() %}
    WHERE updated_on BETWEEN '{{ var('min_date') }}' AND '{{ var('max_date') }}'
{% endif %}