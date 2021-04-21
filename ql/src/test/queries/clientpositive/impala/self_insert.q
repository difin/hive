set metastore.metadata.transformer.class=org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer;
set hive.metastore.client.capabilities=HIVEFULLACIDREAD,HIVEFULLACIDWRITE,HIVECACHEINVALIDATE,HIVEMANAGESTATS,HIVEMANAGEDINSERTWRITE,HIVEMANAGEDINSERTREAD;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.stats.fetch.column.stats=true;

drop table t_one_col;
create table t_one_col (c1 int) TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');

explain
insert into t_one_col(c1) select c1 from t_one_col;
