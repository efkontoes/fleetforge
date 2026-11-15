Gate B evidence from the `full-system` workflow (label `gate-b`, ubuntu-24.04 runners).

| File | Run | Commit | Result |
|---|---|---|---|
| `gate-b.json` | 43704310730 | d49c19e (PR #8, the release) | passed: eight scenarios, every fault followed by the retry it should cause (Silver 2 after the scheduler kill and after Silver's container was killed, Bronze 2 with Garage down and with the registry down at a real schema lookup), reconciliation 11/11 |
| `gate-b-43701689766.json` | 43701689766 | 03f0a90 (PR #8) | passed, and proved the registry lookup for the first time; it also showed that the Spark kill missed Silver (every task ran once), which is D20 |
| `gate-b-43664801824.json` | 43664801824 | 464203f (PR #6) | passed, but its registry scenario never reached the registry (D19) |
