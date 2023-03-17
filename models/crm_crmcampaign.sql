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
      "{{ var("table_prefix") }}_campaigns".id ||
      'campaign' ||
      'hubspot'
    )  as id,
    'hubspot' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_campaigns._airbyte_data as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_campaigns".id as external_id,
    "{{ var("table_prefix") }}_campaigns".name as name,
    "{{ var("table_prefix") }}_campaigns".type as type,
    NULL as actual_cost,
    NULL as budgeted_cost,
   "{{ var("table_prefix") }}_campaigns".subject as  description,
     NULL as expected_revenue,
     NULL as number_of_contacts,
     NULL as owner_id,
     NULL as status,
     NULL as start_date
FROM "{{ var("table_prefix") }}_campaigns"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_campaigns
ON _airbyte_raw_{{ var("table_prefix") }}_campaigns._airbyte_ab_id = "{{ var("table_prefix") }}_campaigns"._airbyte_ab_id
