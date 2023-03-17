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
      "{{ var("table_prefix") }}_deals".id ||
      'company' ||
      'hubspot'
    )  as id,
    'hubspot' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_deals._airbyte_data as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_deals".id as external_id,
    "{{ var("table_prefix") }}_deals".properties->'dealname' as name,
    company.id as company_id,
    contact.id as contact_id,
    archived as archived,
    "{{ var("table_prefix") }}_deals".properties->'amount' as amount,
    COALESCE("{{ var("table_prefix") }}_deals".properties->'closed_lost_reason', "{{ var("table_prefix") }}_deals".properties->'closed_won_reason', NULL)  as closed_reason,
    "{{ var("table_prefix") }}_deals".properties->'closed_date' as closed_date,
    "{{ var("table_prefix") }}_deals".properties->'dealstage' as stage,
    "{{ var("table_prefix") }}_deals".properties->'dealtype' as type,
    "{{ var("table_prefix") }}_deals".properties->'description' as description,
    "{{ var("table_prefix") }}_deals".properties->'hs_analytics_source' as deal_source,
    owner.id as created_by,
    "{{ var("table_prefix") }}_deals".properties->'hs_createdate' as created_at,
    ("{{ var("table_prefix") }}_deals".properties->'hs_is_closed')::boolean as is_closed,
    ("{{ var("table_prefix") }}_deals".properties->'hs_is_closed_won')::boolean as is_won,
    "{{ var("table_prefix") }}_deals".properties->'hs_mrr' as mrr,
    "{{ var("table_prefix") }}_deals".properties->'hs_priority' as priority
FROM "{{ var("table_prefix") }}_deals"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_deals
ON _airbyte_raw_{{ var("table_prefix") }}_deals._airbyte_ab_id = "{{ var("table_prefix") }}_deals"._airbyte_ab_id
LEFT JOIN crm_crmcontact as contact
ON contact.external_id = ("{{ var("table_prefix") }}_deals".contacts->0)::text
LEFT JOIN crm_crmcompany as company
ON company.external_id = ("{{ var("table_prefix") }}_deals".companies->0)::text
LEFT JOIN crm_crmuser as owner
ON owner.external_id = ("{{ var("table_prefix") }}_deals".properties->'hubspot_owner_id')::text