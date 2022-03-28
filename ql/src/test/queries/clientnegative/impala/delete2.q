--! qt:dataset:impala_dataset
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.execution.engine=tez;

CREATE TABLE insert_only_tbl (id int, value int) stored as orc TBLPROPERTIES ('transactional'='true', 'transactional_properties' = 'insert_only') ;

set hive.execution.engine=impala;

--! Delete should fail for insert_only table
delete from insert_only_tbl where id=1;
