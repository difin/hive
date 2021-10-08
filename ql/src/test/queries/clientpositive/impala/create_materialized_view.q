set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.stats.fetch.column.stats=true;
set hive.materializedview.rewriting=true;

create table emps_imp0 (
  empid int,
  deptno int,
  name varchar(256),
  salary float,
  commission int)
stored as parquet TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');

create table depts_imp0 (
  deptno int,
  name varchar(256),
  locationid int)
stored as parquet TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');

alter table emps_imp0 add constraint pk1 primary key (empid) disable novalidate rely;
alter table depts_imp0 add constraint pk2 primary key (deptno) disable novalidate rely;

alter table emps_imp0 add constraint fk1 foreign key (deptno) references depts_imp0(deptno) disable novalidate rely;

explain
create materialized view mv1_imp0
stored as parquet as
select empid, depts_imp0.deptno from emps_imp0
join depts_imp0 using (deptno) where depts_imp0.deptno > cast(ltrim('10', 'a') as integer)
group by empid, depts_imp0.deptno;

drop materialized view mv1_imp0;

explain
create materialized view mv1_imp0_with_partitions_in_order partitioned on (salary_alias, commission) 
stored as parquet as
select empid, deptno deptno_alias, name, salary salary_alias, commission from emps_imp0
where salary > 0.0;

drop materialized view mv1_imp0_with_partitions_in_order;
explain
create materialized view mv1_imp0_with_partitions_out_of_order partitioned on (salary_alias, deptno_alias) 
stored as parquet as
select empid, deptno deptno_alias, name, salary salary_alias, commission from emps_imp0
where salary > 0.0;

drop materialized view mv1_imp0_with_partitions_out_of_order;
drop table emps_imp0;
drop table depts_imp0;
