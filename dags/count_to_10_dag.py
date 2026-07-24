"""
count_to_10 DAG
----------------
Counts from 1 to 10, one task per number, using TaskFlow dynamic task
mapping. Each mapped task instance runs as its own Kubernetes pod when
this DAG is executed under the KubernetesExecutor, so it doubles as a
quick smoke test after a fresh Helm deployment.

Place this file in the DAGs folder configured for your Airflow deployment
(e.g. the git-sync repo referenced in values.yaml, or the dags/ ConfigMap).
"""
from __future__ import annotations

import logging
from datetime import datetime

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)


@dag(
    dag_id="count_to_10",
    description="Counts to 10, one mapped task per number",
    schedule=None,  # trigger manually
    start_date=datetime(2024, 1, 1),
    catchup=True,
    tags=["example", "k8s-executor"],
)
def count_to_10():

    @task
    def make_numbers() -> list[int]:
        return list(range(1, 11))

    @task
    def count(n: int) -> int:
        logger.info("Count: %s", n)
        print(f"Count: {n}")
        return n

    @task
    def report_done(counts: list[int]) -> None:
        logger.info("Finished counting: %s", sorted(counts))
        print(f"Finished counting to {max(counts)}: {sorted(counts)}")

    counted = count.expand(n=make_numbers())
    report_done(counted)


count_to_10()
