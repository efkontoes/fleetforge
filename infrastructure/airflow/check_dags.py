"""DAG checks, run inside the Airflow image: imports, policies and task order."""

from airflow.dag_processing.dagbag import DagBag

bag = DagBag(dag_folder="/opt/airflow/dags")
assert not bag.import_errors, bag.import_errors
expected = {"fleetforge_lakehouse", "fleetforge_release", "fleetforge_maintenance",
            "fleetforge_backfill"}  # fmt: skip
assert expected <= set(bag.dag_ids), bag.dag_ids

for dag in bag.dags.values():
    assert dag.max_active_runs == 1 and not dag.catchup, dag.dag_id
    for t in dag.tasks:
        if t.task_type != "LakeJob":
            continue
        assert t.execution_timeout is not None, (dag.dag_id, t.task_id)
        assert t.retries >= 1 and t.retry_exponential_backoff, (dag.dag_id, t.task_id)
        assert t.auto_remove == "success" and not t.mount_tmp_dir, (dag.dag_id, t.task_id)

pipeline = bag.dags["fleetforge_lakehouse"]
chain = ["bronze", "silver", "gold", "reconcile", "freshness"]
assert set(pipeline.task_ids) == set(chain), pipeline.task_ids
for before, after in zip([None, *chain], chain, strict=False):
    upstream = pipeline.get_task(after).upstream_task_ids
    assert upstream == ({before} if before else set()), (after, upstream)
writers = {t.task_id for t in pipeline.tasks if t.pool == "lakehouse_writers"}
assert writers == {"bronze", "silver", "gold"}, writers
print(f"{len(bag.dag_ids)} DAGs valid: {sorted(bag.dag_ids)}")
