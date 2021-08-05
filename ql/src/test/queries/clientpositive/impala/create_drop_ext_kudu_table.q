set metastore.metadata.transformer.class=org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer;

--! qt:dataset:impala_dataset

CREATE EXTERNAL TABLE fe_kudu_ext_table (
id BIGINT, name STRING, PRIMARY KEY(id) DISABLE NOVALIDATE)
STORED AS KUDU TBLPROPERTIES ('kudu.table_name'='default.fe_kudu_table');

DROP TABLE fe_kudu_ext_table;
