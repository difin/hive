set metastore.metadata.transformer.class=org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer;

--! qt:dataset:impala_dataset

-- verifies we throw an exception when the value of the query option is invalid.
set enabled_runtime_filter_types=3;
explain select 1;
