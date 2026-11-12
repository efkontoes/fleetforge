# ADR 0001 — Order telemetry by (server-assigned boot number, sequence number)

Status: accepted (Milestone 1)

## Context
"Latest state" needs a total order over each robot's observations. Robots
restart, and their sequence counter restarts at 0 with each boot. Boot IDs are
random UUIDs, so the event alone cannot say which boot is newer. Robot clocks
drift and jump.

## Decision
Each boot is registered with `POST /v1/robots/{id}/boots` before it sends
telemetry. The server assigns `boot_number = previous + 1`, serialized by a lock
on the robot row and guarded by `UNIQUE (robot_id, boot_number)`. The ordering key is
`(boot_number, sequence_number)`, compared lexicographically. `observed_at` is
data, never the ordering key.

## Alternatives
| Option | Why rejected |
|---|---|
| Order by `observed_at` | Clocks skew and jump, and reboots are when they are least trustworthy. Two events can share a timestamp. |
| Robot-side persistent boot counter | Lost on flash wipe or board swap. A counter that rolls back makes every new event look stale indefinitely. |
| Treat any boot change as newer | A delayed message from the *previous* boot would overwrite the new boot's state. |

## Consequences
* A robot must make one extra call per boot, and telemetry before that call is
  rejected `UNKNOWN_BOOT` (audited). The robot has to buffer briefly or retry.
* A composite FK makes it impossible to store a `boot_number` that does not
  belong to that `(robot, boot)`.
* In M2 the Kafka state projector needs the boot number as well. It will either
  look it up (cacheable: boot numbers never change) or receive boot
  registrations through CDC.
