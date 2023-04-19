--  Materialized views
create materialized view monitoring.airflow_monitoring_counts
    cluster by app, domain
as
(
select app,
       domain,
       check,
       name,
       sum(value) as value
from monitoring.counts
where app = 'airflow'
group by 1, 2, 3, 4
    );

create materialized view monitoring.airflow_monitoring_last_interim
    cluster by app, domain
as
(
select app,
       domain,
       check,
       name,
       array_agg(value order by timestamp desc) as value_arr
from monitoring.last
group by 1, 2, 3, 4
    );

create view monitoring.airflow_monitoring_last as
select app,
       domain,
       check,
       name,
       value_arr[offset(0)] as value
from monitoring.airflow_monitoring_last_interim;
