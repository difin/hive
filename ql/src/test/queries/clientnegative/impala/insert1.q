--! qt:dataset:impala_dataset
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.execution.engine=tez;

CREATE TABLE full_acid_tbl (id int, value int) stored as orc TBLPROPERTIES ('transactional'='true') ;

set hive.execution.engine=impala;

--! Insert should fail for full acid table
insert into full_acid_tbl values (1, 2);