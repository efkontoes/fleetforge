# Data card: gate-r2

**Synthetic data.** Every row comes from FleetForge's fleet simulator, not from robots.

| | |
|---|---|
| Release | `gate-r2`, readiness `CANDIDATE` |
| Content SHA-256 | `a118829a6f547a909f30818dbe273309c94b4cbfef53bf0343d0fb3531baa88c` |
| Silver build | `76fd20fc7b7e509b9056286b` |
| Events snapshot | 7139470045663208338 |
| Transform / features | `silver-telemetry/3` / `robot-state/1` |
| Rows | train 20698, validation 3115, test 4125 |
| Split | hash of episode (`split`) or of robot (`split_robot`) |
| Split seed | `g2` |

## What a row is
One current telemetry event of one robot: battery, position, optional heading, motor
temperature and lidar flag, all in canonical units. Episodes are boots. `dt_prev_s`,
`vx_mps` and `vy_mps` are finite differences over the previous consecutive step and
are NULL, with a quality flag, when there is no valid previous step.

## Known limitations
Synthetic fleet data. Episodes are boots until missions are specified (gap G7). Velocities are finite differences of reported positions, not measurements. No labels: the release is a candidate until learning-readiness checks pass.

## Intended use
Teaching and testing the pipeline: baselines, leakage checks, drift checks. Not for any
decision about a physical robot.
