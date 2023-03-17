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
      COALESCE("{{ var("table_prefix") }}_contacts_properties".hs_object_id, 0) ||
      COALESCE("{{ var("table_prefix") }}_contacts_properties".email, '') ||
      COALESCE("{{ var("table_prefix") }}_contacts_properties".firstname, '') ||
      COALESCE("{{ var("table_prefix") }}_contacts_properties".lastname, '') ||
      'contact' ||
      'hubspot'
    )  as id,
    'hubspot' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_contacts._airbyte_data as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_contacts".id as external_id,
    "{{ var("table_prefix") }}_contacts_properties".city as city,
    "{{ var("table_prefix") }}_contacts_properties".zip as zip,
    COALESCE("{{ var("table_prefix") }}_contacts_properties".email, "{{ var("table_prefix") }}_contacts_properties".work_email, NULL) as email,
    "{{ var("table_prefix") }}_contacts_properties".phone as phone,
    "{{ var("table_prefix") }}_contacts_properties".state as state,
    "{{ var("table_prefix") }}_contacts_properties".degree as degree,
    "{{ var("table_prefix") }}_contacts_properties".gender as gender,
    "{{ var("table_prefix") }}_contacts_properties".address as address,
    "{{ var("table_prefix") }}_contacts_properties".country as country,
    "{{ var("table_prefix") }}_contacts_properties".jobtitle as jobtitle,
    "{{ var("table_prefix") }}_contacts_properties".lastname as lastname,
    "{{ var("table_prefix") }}_contacts_properties".firstname as firstname,
    "{{ var("table_prefix") }}_contacts_properties".industry as industry,
    NULL as department,
    NULL as company_id,
    "{{ var("table_prefix") }}_contacts_properties".hs_lead_status as status,
    "{{ var("table_prefix") }}_contacts_properties".date_of_birth as date_of_birth,
    "{{ var("table_prefix") }}_contacts_properties".lifecyclestage as lifecycle,
    CONCAT(
      "{{ var("table_prefix") }}_contacts_properties".gender,
      ' ',
      "{{ var("table_prefix") }}_contacts_properties".firstname,
      ' ',
      "{{ var("table_prefix") }}_contacts_properties".lastname
    ) as fullname,
    NULL as photo,
    NULL as deleted,
    "{{ var("table_prefix") }}_contacts_properties".fax as fax,
    "{{ var("table_prefix") }}_contacts_properties".message as description,
    "{{ var("table_prefix") }}_contacts_properties".hs_latest_source as crm_source,
    "{{ var("table_prefix") }}_contacts_properties".hs_language as languages,
    "{{ var("table_prefix") }}_contacts_properties".total_revenue as revenue
FROM "{{ var("table_prefix") }}_contacts"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_contacts
ON _airbyte_raw_{{ var("table_prefix") }}_contacts._airbyte_ab_id = "{{ var("table_prefix") }}_contacts"._airbyte_ab_id
LEFT JOIN {{ var("table_prefix") }}_contacts_properties
ON {{ var("table_prefix") }}_contacts.id::varchar = "{{ var("table_prefix") }}_contacts_properties".hs_object_id::varchar
