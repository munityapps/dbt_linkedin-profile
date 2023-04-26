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
      "{{ var("table_prefix") }}_campaigns"."id" ||
      'campaign' ||
      'mailchimp'
    )  as id,
    'mailchimp' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_campaigns._airbyte_data as last_raw_data, 
    "{{ var("table_prefix") }}_campaigns"."id" as external_id,
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_campaigns"."type" as type,
    "{{ var("table_prefix") }}_campaigns"."status" as status,
    settings->>'title' as title,
    settings->>'from_name' as from_name,
    settings->>'reply_to' as reply_to,
    settings->>'preview_text' as preview_text,
    settings->>'subject_line' as subject,
    recipients->>'list_id' as recipient_list_id,
    CASE
        WHEN send_time != '' THEN send_time::date
        ELSE NULL
    END as send_time,
    "{{ var("table_prefix") }}_campaigns"."long_archive_url" as url,
    "{{ var("table_prefix") }}_campaigns"."create_time"::date as created_time,
    "{{ var("table_prefix") }}_campaigns"."emails_sent" as emails_sent,
    "{{ var("table_prefix") }}_campaigns"."report_summary"::text as report_summary,
    "{{ var("table_prefix") }}_campaigns"."parent_campaign_id" as parent_campaign_id
FROM "{{ var("table_prefix") }}_campaigns"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_campaigns
ON _airbyte_raw_{{ var("table_prefix") }}_campaigns._airbyte_ab_id = "{{ var("table_prefix") }}_campaigns"._airbyte_ab_id

