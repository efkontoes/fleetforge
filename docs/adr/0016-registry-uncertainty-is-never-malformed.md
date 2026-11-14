# ADR 0016 — Registry uncertainty is never recorded as malformed data

Status: accepted (hotfix after Milestone 2). Amends ADR 0009. Numbered 0016 because
the unmerged Milestone 3 branch already uses 0012–0015.

## Context
CI on PR #2 (run 43526791136) quarantined a valid event as `MALFORMED_PAYLOAD`
during `test_registry_outage_pauses_projection_and_never_quarantines`. That is
permanent data loss for operational state: the event never reaches
`robot_latest_state`, and `MALFORMED_PAYLOAD` is not retry-eligible.

**Root cause (reproduced).** The decoder judged a failed schema lookup with two rules:
1. `_registry_down(exc)` recognised transport failures by class: `httpx.TransportError`,
   `OSError`, `TimeoutError`. confluent-kafka's registry client imports **httpx2**
   when it is installed (`confluent_kafka/schema_registry/common/_httpx_compat.py`),
   and `requirements.lock` installs it. `httpx2.RemoteProtocolError`, `ConnectError`,
   `ReadError` and `ReadTimeout` are none of those classes, so `_registry_down`
   returned False for every real transport failure.
2. Anything not "down" was treated as possibly the record's fault, and became
   permanent `DecodeError` whenever `get_subjects` listed the topic's subject.

During a registry restart, a `get_schema` call can hit a dropped connection while
the `get_subjects` call milliseconds later already succeeds. Running the production
decode path with a cold decoder at about 30 Hz through 6 real Apicurio restarts
produced exactly one permanent misclassification:
`schema id 1: RemoteProtocolError: Server disconnected without sending a response.`
The existing unit test exercised `httpx.ReadError`, not the class the client raises.

**The second rule is also unsound.** Polling the registry at 20 Hz through 5
restarts, subjects were listed and id 1 resolved while a later id (36) still
answered 404 for 0.1–0.3 s, in 5 of 5 restarts. KafkaSQL replays its journal in
order. A listed subject does not prove that every id has been restored. (In 3
cycles, an id's *own* subject was never seen listed while that id answered 404, but
nothing guarantees it, and the rule has no other basis.)

## Decision
* **Only an explicit 404 can implicate the record** (`schema_not_found`). Every other
  failure, of any class from any HTTP library, is `RegistryUnavailable`: the batch is
  retried, offsets are held, nothing is quarantined. Unknown failures default to
  "outage", not to "bad data".
* **A 404 is never proof by itself.** For `projector_schema_unresolved_hold_s`
  (120 s) after the first 404 for an id, it is treated as an outage. Measured restart
  404 windows were at most 0.3 s; a full Apicurio restart took about 17 s.
* **After the hold, the record is parked, not malformed:** reason
  `UNRESOLVED_SCHEMA_ID`, category `UNRESOLVED`, status `PARKED`, retry-eligible
  (migration 0009). The row holds the exact bytes and Kafka coordinates and is
  written in the transaction that advances the offset ledger, so every offset moved
  past a parked record is backed by a durable, replayable record. Parked rows are not
  in `fleet.telemetry_quarantine` (that view is invalid data only).
* **Bounded, visible, recoverable:**
  * a parked id is asked again at most every 5 s, so there is no request storm, and
    it decodes normally once the registry returns it;
  * metric `fleetforge_projector_parked_total` and alert `TelemetryRecordsParked`
    (page) fire;
  * `fleet-replay quarantine --reason UNRESOLVED_SCHEMA_ID` replays parked rows; a
    replayed row becomes `REPLAYED`, or `RESOLVED` when the record then turns out to
    have a real data error (recorded in its own row);
  * an id that never existed is `DISCARDED` by an operator. Nothing automatically
    declares it malformed.
* **Verified invalidity stays permanent** (`MALFORMED_PAYLOAD`): missing Confluent
  framing, a body that does not decode with the schema the registry returned, or a
  non-Avro schema type.

Invariant: every event is either processed, retained in place (offsets held), or
parked with its bytes. A registry outage cannot turn a valid event into permanent
corruption.

## Alternatives
| Option | Why not |
|---|---|
| Add `httpx2` classes to the allowlist | Fixes this instance; the next library change reopens it. The default must be "outage" |
| Keep "404 + subject listed = permanent" | Shown unsound above |
| Retry 404s forever | No terminal state: a rogue producer's bogus id would stall a partition indefinitely without a decision |
| Park without holding first | Every registry restart would park records and page someone |
| Separate parking table or dead-letter topic | A second write path; the rejection row is already atomic with the ledger |

## Consequences
* A record with a genuinely unknown id now stalls its partition for up to 120 s (once
  per id per process), then parks. Lag grows during that time: that is the signal.
* Parked records are not in operational state until replayed. The alert pages for
  that reason.
* Downgrading migration 0009 refuses while parked rows exist, instead of relabelling
  them malformed.
