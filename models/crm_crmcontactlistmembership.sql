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
      "{{ var("table_prefix") }}_contacts_list_memberships"."internal-list-id" ||
      "{{ var("table_prefix") }}_contacts_list_memberships"."canonical-vid" ||
      'contactlistmembership' ||
      'hubspot'
    )  as id,
    'hubspot' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_contacts_list_memberships._airbyte_data as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    md5(
      '{{ var("integration_id") }}' ||
      "{{ var("table_prefix") }}_contacts_list_memberships"."internal-list-id" ||
      "{{ var("table_prefix") }}_contacts_list_memberships"."canonical-vid" ||
      'contactlistmembership' ||
      'hubspot'
    )  as external_id,
    contactlist.id as contact_list_id,
    contact.id as contact_id
FROM "{{ var("table_prefix") }}_contacts_list_memberships"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_contacts_list_memberships
ON _airbyte_raw_{{ var("table_prefix") }}_contacts_list_memberships._airbyte_ab_id = "{{ var("table_prefix") }}_contacts_list_memberships"._airbyte_ab_id
LEFT JOIN {{ ref('crm_crmcontact') }} as contact
ON contact.external_id::bigint = "{{ var("table_prefix") }}_contacts_list_memberships"."canonical-vid" AND contact.integration_id = '{{ var("integration_id") }}'
LEFT JOIN {{ ref('crm_crmcontactlist') }} as contactlist
ON contactlist.external_id::bigint = "{{ var("table_prefix") }}_contacts_list_memberships"."static-list-id" AND contactlist.integration_id = '{{ var("integration_id") }}'
