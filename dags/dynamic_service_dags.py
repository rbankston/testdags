"""
dynamic_service_dags DAG factory
---------------------------------
Fetches a JSON list of service status records from a remote gist and
dynamically generates one DAG per entry. Each generated DAG checks
whether the service's status is "active" and, if so, prints the service
name and cpu_load to stdout.

Expected JSON shape (list of objects):
    {"id": 1, "service": "node-01", "status": "active", "cpu_load": 0.45}
"""
from __future__ import annotations

import json
import logging
import urllib.request
from datetime import datetime

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

SERVICES_URL = "https://gist.githubusercontent.com/rbankston/2be5208992af7ed53c6a43ce7afe93db/raw/gistfile1.txt"


def _load_services() -> list[dict]:
    try:
        with urllib.request.urlopen(SERVICES_URL, timeout=10) as response:
            return json.load(response)
    except (OSError, json.JSONDecodeError):
        logger.warning("Failed to load services from %s", SERVICES_URL, exc_info=True)
        return []


def _build_dag(service: dict):

    @dag(
        dag_id=f"service_status_{service['service']}",
        description=f"Checks status and reports cpu_load for {service['service']}",
        schedule="@hourly",
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=["example", "dynamic", "service-status"],
    )
    def service_status_dag():

        @task
        def check_and_report(service: dict) -> None:
            if service.get("status") == "active":
                logger.info("%s cpu_load=%s", service["service"], service["cpu_load"])
                print(f"{service['service']} cpu_load={service['cpu_load']}")
            else:
                logger.info(
                    "%s is not active (status=%s), skipping",
                    service["service"],
                    service.get("status"),
                )

        check_and_report(service)

    return service_status_dag()


for _service in _load_services():
    _dag_obj = _build_dag(_service)
    globals()[_dag_obj.dag_id] = _dag_obj
