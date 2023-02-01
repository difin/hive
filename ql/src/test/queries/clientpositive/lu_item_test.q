
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.vectorized.execution.enabled=true; 

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/lu_item_ext;

dfs -copyFromLocal ../../data/files/lu_item_src/000000_0 ${system:test.tmp.dir}/lu_item_ext/000000_0;
dfs -copyFromLocal ../../data/files/lu_item_src/000001_0 ${system:test.tmp.dir}/lu_item_ext/000001_0;
dfs -copyFromLocal ../../data/files/lu_item_src/000002_0 ${system:test.tmp.dir}/lu_item_ext/000002_0;
dfs -copyFromLocal ../../data/files/lu_item_src/000003_0 ${system:test.tmp.dir}/lu_item_ext/000003_0;
dfs -copyFromLocal ../../data/files/lu_item_src/000004_0 ${system:test.tmp.dir}/lu_item_ext/000004_0;
dfs -copyFromLocal ../../data/files/lu_item_src/000005_0 ${system:test.tmp.dir}/lu_item_ext/000005_0;
dfs -copyFromLocal ../../data/files/lu_item_src/000006_0 ${system:test.tmp.dir}/lu_item_ext/000006_0;
 
dfs -ls ${system:test.tmp.dir}/lu_item_ext/;

CREATE EXTERNAL TABLE IF NOT EXISTS lu_item_for_managed
(   ITEM_ID int,
    ITEM_NAME string,
    ITEM_LONG_DESC string,
    ITEM_FOREIGN_NAME string,
    ITEM_URL string,
    DISC_CD float,
    WARRANTY string,
    UNIT_PRICE float,
    UNIT_COST float,
    SUBCAT_ID int,
    SUPPLIER_ID int,
    BRAND_ID int,
    ITEM_NAME_DE string,
    ITEM_NAME_FR string,
    ITEM_NAME_ES string,
    ITEM_NAME_IT string,
    ITEM_NAME_PO string,
    ITEM_NAME_JA string,
    ITEM_NAME_SCH string,
    ITEM_NAME_KO string,
    ITEM_LONG_DESC_DE string,
    ITEM_LONG_DESC_FR string,
    ITEM_LONG_DESC_ES string,
    ITEM_LONG_DESC_IT string,
    ITEM_LONG_DESC_PO string,
    ITEM_LONG_DESC_JA string,
    ITEM_LONG_DESC_SCH string,
    ITEM_LONG_DESC_KO string
) stored as ORC
LOCATION '${system:test.tmp.dir}/lu_item_ext';

CREATE transactional TABLE IF NOT EXISTS lu_item
stored as ORC 
AS SELECT * from lu_item_for_managed;

select max(item_name) from lu_item limit 40;

drop table lu_item;
drop table lu_item_ext;

dfs -rmr ${system:test.tmp.dir}/lu_item_ext;
