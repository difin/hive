set metastore.metadata.transformer.class=org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer;
set hive.metastore.client.capabilities=HIVEFULLACIDREAD,HIVEFULLACIDWRITE,HIVECACHEINVALIDATE,HIVEMANAGESTATS,HIVEMANAGEDINSERTWRITE,HIVEMANAGEDINSERTREAD;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.stats.fetch.column.stats=true;

--! qt:dataset:impala_dataset
drop table if exists table_single_ts_n0;
create table table_single_ts_n0(
  tt_timestamp timestamp
) stored as parquet TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');

set explain_level=MINIMAL;

-- New syntax
explain
select extract(year from tt_timestamp) as year_from_ts, count(*)
from table_single_ts_n0
group by extract(year from tt_timestamp);

explain
select extract(month from tt_timestamp) as year_from_ts, count(*)
from table_single_ts_n0
group by extract(month from tt_timestamp);

explain
select extract(day from tt_timestamp) as year_from_ts, count(*)
from table_single_ts_n0
group by extract(day from tt_timestamp);

explain
select extract(hour from tt_timestamp) as year_from_ts, count(*)
from table_single_ts_n0
group by extract(hour from tt_timestamp);

explain
select extract(minute from tt_timestamp) as year_from_ts, count(*)
from table_single_ts_n0
group by extract(minute from tt_timestamp);

explain
select extract(second from tt_timestamp) as year_from_ts, count(*)
from table_single_ts_n0
group by extract(second from tt_timestamp);

-- Legacy syntax
explain
select extract(tt_timestamp, 'year') as year_from_ts, count(*)
from table_single_ts_n0
group by extract(tt_timestamp, 'year');

explain
select extract(tt_timestamp, 'month') as year_from_ts, count(*)
from table_single_ts_n0
group by extract(tt_timestamp, 'month');

explain
select extract(tt_timestamp, 'day') as year_from_ts, count(*)
from table_single_ts_n0
group by extract(tt_timestamp, 'day');

explain
select extract(tt_timestamp, 'hour') as year_from_ts, count(*)
from table_single_ts_n0
group by extract(tt_timestamp, 'hour');

explain
select extract(tt_timestamp, 'minute') as year_from_ts, count(*)
from table_single_ts_n0
group by extract(tt_timestamp, 'minute');

explain
select extract(tt_timestamp, 'second') as year_from_ts, count(*)
from table_single_ts_n0
group by extract(tt_timestamp, 'second');

drop table table_single_ts_n0;
