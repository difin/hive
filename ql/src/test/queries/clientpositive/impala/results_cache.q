--! qt:disabled:Re-enable this q test once IMPALA-2945 is merged.
--! qt:dataset:impala_dataset
set metastore.metadata.transformer.class=org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer;
set hive.metastore.client.capabilities=HIVEFULLACIDREAD,HIVEFULLACIDWRITE,HIVECACHEINVALIDATE,HIVEMANAGESTATS,HIVEMANAGEDINSERTWRITE,HIVEMANAGEDINSERTREAD;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.stats.fetch.column.stats=true;
set hive.impala.result.method=file;
set hive.query.results.cache.enabled=true;

CREATE TABLE test_store_sales(ss_item_sk int, ss_quantity int) STORED AS PARQUET
  TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');
CREATE TABLE test_store_items(i_item_sk int) STORED AS PARQUET
  TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');

-- should not cache
explain SELECT * FROM test_store_sales;
explain SELECT * FROM (SELECT * FROM test_store_sales) a;
explain SELECT * FROM test_store_sales WHERE ss_quantity > 10;
explain SELECT * FROM (SELECT * FROM test_store_sales) a WHERE a.ss_quantity > 10;
-- should cache
explain SELECT * FROM test_store_sales a, test_store_items b WHERE a.ss_item_sk = b.i_item_sk;
explain SELECT * FROM test_store_sales a, test_store_sales b;
explain SELECT ss_item_sk, max(ss_quantity) FROM test_store_sales GROUP BY ss_item_sk;
explain SELECT max(ss_quantity) FROM test_store_sales;
explain SELECT * FROM (SELECT * FROM test_store_sales) a, (SELECT * FROM test_store_items) b WHERE a.ss_item_sk = b.i_item_sk;

-- view tests
CREATE VIEW test_complex_view AS SELECT ss_item_sk, max(ss_quantity) FROM test_store_sales GROUP BY ss_item_sk;
-- should cache
EXPLAIN SELECT * FROM test_complex_view;
DROP VIEW test_complex_view;

CREATE VIEW test_simple_view AS SELECT * FROM test_store_sales;
-- should NOT cache
EXPLAIN SELECT * FROM test_simple_view;
DROP VIEW test_simple_view;

DROP TABLE test_store_sales;
DROP TABLE test_store_items;
