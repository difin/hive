--! qt:dataset:impala_dataset
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.execution.engine=tez;

CREATE TABLE full_acid_tbl (id int, value int) stored as orc TBLPROPERTIES ('transactional'='true') ;

set hive.execution.engine=impala;

--! Update should fail for full acid table
update full_acid_tbl set value=4 where id=1;
