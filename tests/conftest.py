import pytest


@pytest.fixture(scope="module")
def airflow_statsd_socket():
    return [
        "airflow.scheduler.critical_section_duration:8.494474|ms",
        "airflow.executor.open_slots:32|g",
        "airflow.executor.queued_tasks:0|g",
        "airflow.executor.running_tasks:0|g",
        "airflow.scheduler_heartbeat:1|c",
        "airflow.pool.open_slots.default_pool:128|g",
        "airflow.pool.queued_slots.default_pool:0|g",
        "airflow.scheduler.critical_section_duration:8.162927|ms",
        "airflow.dag_processing.total_parse_time:11.112728387117386|g",
        "airflow.dagbag_size:22|g",
        "airflow.dag_processing.processes:1|c",
    ]
