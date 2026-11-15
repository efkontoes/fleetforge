-- The two quality tables are computed separately and must agree per day and topic.
with q as (
    select
        ingest_date,
        source_topic,
        sum(records) as records,
        sum(case when outcome = 'ACCEPTED' then records else 0 end) as accepted,
        sum(case when outcome = 'DUPLICATE' then records else 0 end) as duplicates,
        sum(case when outcome = 'REJECTED' then records else 0 end) as rejected,
        sum(case when outcome = 'UNRESOLVED' then records else 0 end) as unresolved
    from {{ ref("pipeline_daily_quality") }}
    group by 1, 2
)
select r.ingest_date, r.source_topic
from {{ ref("pipeline_daily_rates") }} as r
full outer join q on r.ingest_date = q.ingest_date and r.source_topic = q.source_topic
where r.ingest_date is null
    or q.ingest_date is null
    or r.bronze_records <> q.records
    or r.accepted <> q.accepted
    or r.duplicates <> q.duplicates
    or r.rejected <> q.rejected
    or r.unresolved <> q.unresolved
