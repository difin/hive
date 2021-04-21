set metastore.metadata.transformer.class=org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer;

--! qt:dataset:impala_dataset

-- Check if normalization of columns is ok on a partition pruned column
EXPLAIN
SELECT
  ss_sold_date_sk
FROM
  impala_tpcds_store_sales
WHERE
  cast(10 as bigint) < cast(ss_sold_date_sk as bigint) + 1;
