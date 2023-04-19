Airflow Metrics to BigQuery
===

<p align="center">
    <a href="https://github.com/abyssnlp/airflow-metrics-gbq/actions/workflows/ci.yaml"><img alt="build" src="https://github.com/abyssnlp/airflow-metrics-gbq/actions/workflows/ci.yaml/badge.svg"/></a>
    <a href="https://github.com/abyssnlp/airflow-metrics-gbq/actions/workflows/release.yaml"><img alt="release" src="https://github.com/abyssnlp/airflow-metrics-gbq/actions/workflows/release.yaml/badge.svg"/></a>
    <a href="https://pypi.org/project/airflow-metrics-gbq"><img alt="PyPI" src="https://img.shields.io/pypi/v/airflow-metrics-gbq?style=plastic"></a>
    <img alt="PyPI - License" src="https://img.shields.io/pypi/l/airflow-metrics-gbq?color=blue&style=plastic">
</p>

Sends airflow metrics to Bigquery

---

### Installation
```bash
pip install airflow-metrics-gbq
```

### Usage
1. Activate statsd metrics in `airflow.cfg`
```ini
[metrics]
statsd_on = True
statsd_host = localhost
statsd_port = 8125
statsd_prefix = airflow
```
2. Restart the webserver and the scheduler
```bash
systemctl restart airflow-webserver.service
systemctl restart airflow-scheduler.service
```
3. Check that airflow is sending out metrics:
```bash
nc -l -u localhost 8125
```
4. Install this package
5. Create required tables (counters, gauges and timers), an example is shared [here](./scripts/sql/create_monitoring_tables.sql)
6. Create materialized views which refresh when the base table changes, as describe [here](./scripts/sql/mat_views.sql)
7. Create a simple python script `monitor.py` to provide configuration:
```python
from airflow_metrics_gbq.metrics import AirflowMonitor

if __name__ == '__main__':
    monitor = AirflowMonitor(
        host="localhost", # Statsd host (airflow.cfg)
        port=8125, # Statsd port (airflow.cfg)
        gcp_credentials="path/to/service/account.json",
        dataset_id="monitoring", # dataset where the monitoring tables are
        counts_table="counts", # counters table
        last_table="last", # gauges table
        timers_table="timers" # timers table
    )
    monitor.run()
```
8. Run the program, ideally in the background to start sending metrics to BigQuery:
```bash
python monitor.py &
```
9. The logs can be viewed in the GCP console under the `airflow_monitoring` app_name in Google Cloud Logging.


**Future releases**
- [ ] Add a buffer (pyzmq or mp queue)
- [ ] Run sending metrics to GBQ in another process
- [ ] Add proper typing and mypy support and checks
- [ ] Provide more configurable options
- [ ] Provide better documentation
