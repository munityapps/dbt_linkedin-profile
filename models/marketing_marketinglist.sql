{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
)}}

SELECT 
    NOW() as created,
    NOW() as modified,
    md5(
      '{{ var("integration_id") }}' ||
      "{{ var("table_prefix") }}_lists".id ||
      'list' ||
      'mailchimp'
    )  as id,
    'mailchimp' as source,
    id as external_id,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    '{{ var("timestamp") }}' as sync_timestamp,
    _airbyte_raw_{{ var("table_prefix") }}_lists._airbyte_data as last_raw_data, 
    name,
    stats,
    contact,
    list_rating as rating,
    date_created::date as created_date,
    subscribe_url_long as url 
FROM "{{ var("table_prefix") }}_lists"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_lists
    ON _airbyte_raw_{{ var("table_prefix") }}_lists._airbyte_ab_id = "{{ var("table_prefix") }}_lists"._airbyte_ab_id

