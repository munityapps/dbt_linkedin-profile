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
      "{{ var("table_prefix") }}_contact_lists".listid ||
      'contactlist' ||
      'hubspot'
    )  as id,
    'hubspot' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_contact_lists._airbyte_data as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_contact_lists".listid as external_id,
    "{{ var("table_prefix") }}_contact_lists".name as name,
    "{{ var("table_prefix") }}_contact_lists".listtype as type,
    "{{ var("table_prefix") }}_contact_lists".archived::boolean as deleted,
    "{{ var("table_prefix") }}_contact_lists".metadata->'size' as size,
    NULL as campaign_id
FROM "{{ var("table_prefix") }}_contact_lists"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_contact_lists
ON _airbyte_raw_{{ var("table_prefix") }}_contact_lists._airbyte_ab_id = "{{ var("table_prefix") }}_contact_lists"._airbyte_ab_id
