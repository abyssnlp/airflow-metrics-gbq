# Changelog

[//]: # (Next Release)

---

## v0.0.5 (2023-05-05)
### Feature
* Add exponential backoff retry on fetch ([`59e6a36`](https://github.com/abyssnlp/airflow-metrics-gbq/commit/59e6a36d6531148d50d633cc838e8df9ea7b3c9e))
* Adds the option to use synchronous fetch and flush in case of limited resources ([`c0e3c64`](https://github.com/abyssnlp/airflow-metrics-gbq/commit/c0e3c64ec3dff97eefe2a9099cf34b9af6697205))

### Fix
* Increase buffer capacity, Increased retry attempts and add retry statistics logging([`cf3b3eb`](https://github.com/abyssnlp/airflow-metrics-gbq/commit/cf3b3eb2ff820d89d30b7e4fbcf4f7f48a198b7d))
* Increase buffer flush timeout to 15 seconds ([`9c3ec4c`](https://github.com/abyssnlp/airflow-metrics-gbq/commit/9c3ec4cd53e24a25d728c85b8f49b20b6266533a))
* Remove stop clause for retry and increase max wait between retries from 10 to 20 seconds ([`9f18120`](https://github.com/abyssnlp/airflow-metrics-gbq/commit/9f1812003660d670177fee6d3f02f35461a94709))

---

## v0.0.4 (2023-04-26)
### Feature
* Better type annotations, Time and capacity based buffers, Background thread to fetch from socket, Adds retry for better
fault tolerance ([`a54b0db`](https://github.com/abyssnlp/airflow-metrics-gbq/commit/a54b0db1e55d7822476d7812cd3749a2f99cc7b4))

### Fix
* Fix Measure type enum([`234eecf`](https://github.com/abyssnlp/airflow-metrics-gbq/commit/234eecfaf6760037ca4e71d21da4ea746cc49797))
* Fix background processes([`3d8254e`](https://github.com/abyssnlp/airflow-metrics-gbq/commit/3d8254e5752ae37261d51843993a4eec2986419c))
* Minor code fixes

---

## v0.0.3 (2023-04-20)
### Feature
* Added better release processes ([`c0e9dfa`](https://github.com/abyssnlp/airflow-metrics-gbq/commit/c0e9dfaf3cf03708d4426b79768aa0947e44c340),
[`d682e32`](https://github.com/abyssnlp/airflow-metrics-gbq/commit/d682e32484252fcc99484f05cc4ec785bd81febd))

---

## v0.0.3a0 (2023-04-19)
### Feature(pre-release)
* Add shipping airflow metrics to BigQuery ([`101e7d8`](https://github.com/abyssnlp/airflow-metrics-gbq/commit/101e7d8d263dddfa93e261b838fa64af8b02e8a2))
* Add initial tests ([`2407378`](https://github.com/abyssnlp/airflow-metrics-gbq/commit/24073780795b9ad1a12d8a70c629d1e155895141))
* Add CI ([`9e9ea6f`](https://github.com/abyssnlp/airflow-metrics-gbq/commit/9e9ea6f61053df874e025c8780fe52bd69a173c9))
