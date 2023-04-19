-- Schedule from BigQuery UI/via Airflow
delete from monitoring.last
where timestamp_micros(cast(timestamp * 1000000 as int64)) < date_sub(current_timestamp(), interval 7 day);

delete from monitoring.counts
where timestamp_micros(cast(timestamp * 1000000 as int64)) < date_sub(current_timestamp(), interval 7 day);

delete from monitoring.timers
where timestamp_micros(cast(timestamp * 1000000 as int64)) < date_sub(current_timestamp(), interval 7 day);
