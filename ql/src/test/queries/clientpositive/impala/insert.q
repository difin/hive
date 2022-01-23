set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.stats.fetch.column.stats=true;
set hive.materializedview.rewriting=true;

create table perm_nopart(int_col1 int, string_col string, int_col2 int) stored as parquet;
create table perm_part(int_col1 int, string_col string) partitioned by (p1 int, p2 string) stored as parquet;

explain insert into perm_nopart values(1,'str',2);

explain insert into perm_nopart(int_col2, string_col, int_col1) values(1,'str',2);

explain insert into perm_nopart(int_col2) values(1);

explain insert into perm_part(p1, string_col, int_col1, p2) values(10,'str',1, 'hello');

explain insert into perm_part(p2, string_col, int_col1, p1) values('hello','str',1, 10);

explain insert into perm_part(p2, p1) values('hello', 10);

explain insert into perm_nopart(int_col1, string_col, int_col2) select 1,'str',2;

explain insert into perm_nopart(int_col2, string_col, int_col1) select 1,'str',2;

explain insert into perm_nopart(int_col2) select 1;

explain insert into perm_part(p1, string_col, int_col1, p2) select 10,'str',1, 'hello';

explain insert into perm_part(p2, string_col, int_col1, p1) select 'hello','str',1, 10;

explain insert into perm_part(p2, p1) select 'hello', 10;

create table insertonly_nopart (i int)
tblproperties('transactional'='true', 'transactional_properties'='insert_only');

explain insert into insertonly_nopart values (1), (2);

explain insert into insertonly_nopart values (3);

explain insert overwrite insertonly_nopart values (10);

explain insert overwrite insertonly_nopart select 100;

explain insert overwrite insertonly_nopart
select * from insertonly_nopart limit 0;

create table if not exists insertonly_part (i int)
partitioned by (p int)
tblproperties('transactional'='true', 'transactional_properties'='insert_only');

explain insert into insertonly_part partition (p=2) values (21);

set hive.exec.dynamic.partition.mode=nonstrict

explain insert overwrite insertonly_part partition (p) values (1000, 1);


