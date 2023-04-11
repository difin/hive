--! qt:dataset:impala_dataset
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.execution.engine=impala;

create table if not exists orc_tbl (
my_date timestamp,
my_id bigint,
my_id2 bigint,
environment string,
down_volume bigint,
up_volume bigint
)
stored as orc tblproperties ('transactional'='true', 'transactional_properties'='insert_only');

create table if not exists avro_tbl (
my_date timestamp,
my_id bigint,
my_id2 bigint,
environment string,
down_volume bigint,
up_volume bigint
)
stored as orc tblproperties ('transactional'='true', 'transactional_properties'='insert_only');

--! Insert should fail for tables that are neither of text nor Parquet format.
insert into table orc_tbl values ('2010-10-10 00:00:00', 1, 1, 'env', 1, 1);
insert into table avro_tbl values ('2010-10-10 00:00:00', 1, 1, 'env', 1, 1);

