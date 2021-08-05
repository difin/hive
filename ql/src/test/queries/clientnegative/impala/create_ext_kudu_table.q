--! qt:dataset:impala_dataset

CREATE EXTERNAL TABLE fe_kudu_ext_table (
id BIGINT, name STRING, PRIMARY KEY(id) DISABLE NOVALIDATE)
STORED AS KUDU;
