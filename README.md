Airflow Metrics to BigQuery
===

<p align="center">
    <a href="https://github.com/abyssnlp/airflow-metrics-gbq/actions/workflows/ci.yaml"><img alt="build" src="https://github.com/abyssnlp/airflow-metrics-gbq/actions/workflows/ci.yaml/badge.svg"/></a>
    <a href="https://github.com/abyssnlp/airflow-metrics-gbq/actions/workflows/release.yaml"><img alt="release" src="https://github.com/abyssnlp/airflow-metrics-gbq/actions/workflows/release.yaml/badge.svg"/></a>
    <a href="https://pypi.org/project/airflow-metrics-gbq"><img alt="PyPI" src="https://img.shields.io/pypi/v/airflow-metrics-gbq?style=plastic"></a>
    <img alt="PyPI - License" src="https://img.shields.io/pypi/l/airflow-metrics-gbq?color=blue&style=plastic">
</p>

Sends airflow metrics to Bigquery

**TODO**
- List steps to send (activate statsd in airflow.cfg etc)
- Better docs and sphinx docs from docstrings
- Add badges

**Enhance**
- Add a buffer (pyzmq or mp queue)
- Run sending metrics to GBQ in another process
- Add proper typing and mypy support and checks
