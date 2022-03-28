--! qt:dataset:impala_dataset
set hive.support.concurrency=true;
set hive.execution.engine=tez;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table source(id integer);

create table full_acid_target(id integer, value string default 'def') stored as orc TBLPROPERTIES ('transactional'='true') ;

set hive.execution.engine=impala;

--! Merge should fail for Impala
merge into full_acid_target t using source u on t.id=u.id when not matched then insert (id) values (u.id);
