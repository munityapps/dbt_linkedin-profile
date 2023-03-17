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
      "{{ var("table_prefix") }}_companies".id ||
      'company' ||
      'hubspot'
    )  as id,
    'hubspot' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_companies._airbyte_data as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_companies".id as external_id,
    "{{ var("table_prefix") }}_companies".properties->'name' as name,
    "{{ var("table_prefix") }}_companies".properties->'type' as type,
    "{{ var("table_prefix") }}_companies".properties->'hs_analytics_source' as company_source,
    NULL::boolean as active,
    "{{ var("table_prefix") }}_companies".properties->'annualrevenue' as annual_revenue,
    "{{ var("table_prefix") }}_companies".properties->'city' as billing_address_city,
    "{{ var("table_prefix") }}_companies".properties->'country' as billing_address_country,
    "{{ var("table_prefix") }}_companies".properties->'zip' as billing_address_zip,
    "{{ var("table_prefix") }}_companies".properties->'state' as billing_address_state,
    ("{{ var("table_prefix") }}_companies".properties->'address')::text || ' ' || ("{{ var("table_prefix") }}_companies".properties->'address2')::text as billing_address_street,
    "{{ var("table_prefix") }}_companies".properties->'city' as shipping_address_city,
    "{{ var("table_prefix") }}_companies".properties->'country' as shipping_address_country,
    "{{ var("table_prefix") }}_companies".properties->'zip' as shipping_address_zip,
    "{{ var("table_prefix") }}_companies".properties->'state' as shipping_address_state,
    ("{{ var("table_prefix") }}_companies".properties->'address')::text || ' ' || ("{{ var("table_prefix") }}_companies".properties->'address2')::text as shipping_address_street,
    "{{ var("table_prefix") }}_companies".properties->'hs_lead_status' as status,
    NULL as priority,
    "{{ var("table_prefix") }}_companies".properties->'notes_last_contacted' as description,
    NULL as fax,
    "{{ var("table_prefix") }}_companies".properties->'industry' as industry,
    NULL::boolean as deleted,
    "{{ var("table_prefix") }}_companies".properties->'numberofemployees' as number_of_employees,
    "{{ var("table_prefix") }}_companies".properties->'phone' as phone,
    NULL as photo,
    NULL as rating,
    COALESCE("{{ var("table_prefix") }}_companies".properties->'website', "{{ var("table_prefix") }}_companies".properties->'domain', NULL)  as website,
    "{{ var("table_prefix") }}_companies".properties->'lifecyclestage' as lifecycle
FROM "{{ var("table_prefix") }}_companies"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_companies
ON _airbyte_raw_{{ var("table_prefix") }}_companies._airbyte_ab_id = "{{ var("table_prefix") }}_companies"._airbyte_ab_id
