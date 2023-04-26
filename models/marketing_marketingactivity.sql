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
      "{{ var("table_prefix") }}_email_activity".email_id ||
      "{{ var("table_prefix") }}_email_activity".campaign_id ||
      "{{ var("table_prefix") }}_email_activity".action ||
      'activity' ||
      'mailchimp'
    )  as id,
    'mailchimp' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_email_activity._airbyte_data as last_raw_data, 
    email_address as external_id,
    '{{ var("timestamp") }}' as sync_timestamp,
    action,
    email_address as email,
    ip,
    timestamp as created_time,
    campaign.id as campaign_id
FROM "{{ var("table_prefix") }}_email_activity"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_email_activity
    ON _airbyte_raw_{{ var("table_prefix") }}_email_activity._airbyte_ab_id = "{{ var("table_prefix") }}_email_activity"._airbyte_ab_id
LEFT JOIN {{ ref('marketing_marketingcampaign') }} as campaign
    ON campaign_id = campaign.external_id 
    AND campaign.integration_id = '{{var("integration_id")}}'


