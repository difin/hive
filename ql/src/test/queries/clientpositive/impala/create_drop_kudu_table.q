set metastore.metadata.transformer.class=org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer;

--! qt:dataset:impala_dataset

CREATE TABLE fe_kudu_table (id BIGINT, name STRING, PRIMARY KEY(id) DISABLE NOVALIDATE)
STORED AS KUDU;

DROP TABLE fe_kudu_table;
