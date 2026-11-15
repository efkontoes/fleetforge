Run 43708265800 (`full-system` workflow dispatched on `main`, gates a b c, ubuntu-24.04 runner, commit 5c9b1fa, the merge of PR #8): Gates A, C and B passed.

* Gate A: the same build scopes, row counts (27,942 events, 46,696 outcomes), Gold rows and 10/10 reconciliation as this machine and run 43646391081. Incremental Silver, a full rebuild and the Python reference have the same digest within the run. Digests differ between runs because each run creates its own topics.
* Gate C: the same model metrics as the earlier runs; two trainings on one release give identical metrics.
* Gate B: every injected failure was followed by a retry, and reconciliation passed 11/11.
