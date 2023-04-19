-- Monitoring tables
-- counts, last, timers
create table monitoring.counts(
    app string,
    domain string,
    check string,
    value float64,
    timestamp float64,
    name string
);

create table monitoring.last(
    app string,
    domain string,
    check string,
    value float64,
    timestamp float64,
    name string
);

create table monitoring.timers(
    app string,
    domain string,
    check string,
    value float64,
    timestamp float64,
    name string
);
