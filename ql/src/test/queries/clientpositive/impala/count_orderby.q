set metastore.metadata.transformer.class=org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer;

--! qt:dataset:impala_dataset

EXPLAIN CBO PHYSICAL
SELECT COUNT(*)
 FROM impala_tpch_lineitem
 GROUP BY l_quantity ORDER BY COUNT(*);
