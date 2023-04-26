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
      "{{ var("table_prefix") }}_me_lookup"."id" ||
      'user' ||
      'linkedin_profile'
    )  as id,
    'linkedin_profile' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_me_lookup._airbyte_data as last_raw_data, 
    "{{ var("table_prefix") }}_me_lookup"."id" as external_id,
    '{{ var("timestamp") }}' as sync_timestamp,
    firstname,
    lastname,
    firstname || ' ' || lastname as fullname,
    avatar_url as avatar,
    email,
    phone,
    emails::text as all_emails,
    phones::text as all_phones
FROM "{{ var("table_prefix") }}_me_lookup"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_me_lookup
ON _airbyte_raw_{{ var("table_prefix") }}_me_lookup._airbyte_ab_id = "{{ var("table_prefix") }}_me_lookup"._airbyte_ab_id
CROSS JOIN "{{ var("table_prefix") }}_primary_contact_lookup" as primary_contact

