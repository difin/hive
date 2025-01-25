--! qt:disabled:Re-enable this q test once IMPALA-2945 is merged.
set metastore.metadata.transformer.class=org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer;

--! qt:dataset:impala_dataset

explain cbo physical
select ndv(l_returnflag) from `impala_tpch_lineitem`;

explain
select ndv(l_returnflag) from `impala_tpch_lineitem`;
