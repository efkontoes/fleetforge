-- No episode spans two splits, and no robot spans two robot-level splits, within a release.
select release_id, 'episode' as unit, episode_id as id
from {{ source("gold_releases", "robot_state_observations") }}
group by release_id, episode_id
having count(distinct split) > 1
union all
select release_id, 'robot', robot_id
from {{ source("gold_releases", "robot_state_observations") }}
group by release_id, robot_id
having count(distinct split_robot) > 1
