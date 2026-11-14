-- A release row's counts equal the observations stored under its id.
with stored as (
    select release_id, count(*) as n,
           count_if(split = 'train') as train,
           count_if(split = 'validation') as validation,
           count_if(split = 'test') as test
    from {{ source("gold_releases", "robot_state_observations") }}
    group by 1
)
select r.release_id
from {{ source("gold_releases", "training_dataset_releases") }} as r
left join stored as s on r.release_id = s.release_id
where coalesce(s.n, 0) <> r.rows_total
    or r.rows_total <> r.rows_train + r.rows_validation + r.rows_test
    or coalesce(s.train, 0) <> r.rows_train
    or coalesce(s.validation, 0) <> r.rows_validation
    or coalesce(s.test, 0) <> r.rows_test
