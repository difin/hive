--! qt:dataset:impala_dataset
set hive.support.concurrency=true;
set hive.execution.engine=tez;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table source(id integer);

CREATE TABLE insert_only_target (id int, value int) stored as orc TBLPROPERTIES ('transactional'='true', 'transactional_properties' = 'insert_only') ;

set hive.execution.engine=impala;

--! Merge should fail for Impala
merge into insert_only_target t using source u on t.id=u.id when not matched then insert (id) values (u.id);
