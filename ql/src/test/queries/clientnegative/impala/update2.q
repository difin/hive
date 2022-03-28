--! qt:dataset:impala_dataset
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.execution.engine=tez;

CREATE TABLE insert_only_tbl (id int, value int) stored as orc TBLPROPERTIES ('transactional'='true', 'transactional_properties' = 'insert_only') ;

set hive.execution.engine=impala;

--! Update should fail for insert_only transactional table
update insert_only_tbl set value=4 where id=1;
