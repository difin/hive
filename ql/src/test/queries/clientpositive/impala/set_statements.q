--! qt:disabled:Re-enable this q test once IMPALA-2945 is merged.
set metastore.metadata.transformer.class=org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer;

--! qt:dataset:impala_dataset

-- verifies execution.engine and hive.execution.engine are consistent before any SET statement
set execution.engine;
set hive.execution.engine;

set execution.engine=tez;
-- verifies both properties are updated if execution.engine is changed
set execution.engine;
set hive.execution.engine;

set hive.execution.engine=impala;
-- verifies both properties are updated if hive.execution.engine is changed
set hive.execution.engine;
set execution.engine;

set num_nodes=1;
set num_nodes;
set impala.num_nodes;
explain select count(*) from impala_tpch_lineitem;
-- verifies that the change to impala.num_nodes takes effect after a query is executed
set impala.num_nodes;

set impala.num_nodes=3;
set impala.num_nodes;
-- verifies that the change to num_nodes takes effect immediately
set num_nodes;

set impala.num_nodes=1;
set num_nodes=3;
-- verifies that impala.num_nodes and num_nodes are different at this point
set impala.num_nodes;
set num_nodes;
-- executes a query
explain select count(*) from impala_tpch_lineitem;
-- verifies that a non-prefixed option takes precedence over the prefixed one after a query is executed
set impala.num_nodes;
set num_nodes;

set impala.dummy_option=aaa;
set impala.dummy_option;
set dummy_option;
explain select count(*) from impala_tpch_lineitem;
-- verifies that unsupported Impala query options are removed after a query is executed
set impala.dummy_option;
set dummy_option;

-- At the moment, ("impala.explain_level", "VERBOSE") is in 'properties' of HiveConf,
-- which was loaded from data/conf/impala/hive-site.xml.
-- 'lowercaseProperties' of HiveConf is empty.
set explain_level=STANDARD;
-- ("impala.explain_level", "VERBOSE") is in 'properties' of HiveConf.
-- ("explain_level", "STANDARD") is in 'properties' of HiveConf.
-- ("explain_level", "STANDARD") is in 'lowercaseProperties' of HiveConf.
set EXPLAIN_LEVEL=MINIMAL;
-- ("impala.explain_level", "VERBOSE") is in 'properties' of HiveConf.
-- ("explain_level", "STANDARD") is in 'properties' field of HiveConf.
-- ("explain_level", "MINIMAL") is in 'lowercaseProperties' of HiveConf.
-- verifies that impala.explain_level is still "VERBOSE".
set impala.explain_level;
explain select 1;
-- ("impala.explain_level", "MINIMAL") is in 'properties' of HiveConf.
-- ("explain_level", "STANDARD") is in 'properties' of HiveConf.
-- ("explain_level", "MINIMAL") is in 'lowercaseProperties' of HiveConf.
set impala.explain_level;

set explain_level=STANDARD;
-- ("impala.explain_level", "MINIMAL") is in 'properties' of HiveConf.
-- ("explain_level", "STANDARD") is in 'properties' of HiveConf.
-- ("explain_level", "STANDARD") is in 'lowercaseProperties' of HiveConf.
set impala.explain_level;
explain select 2;
-- ("impala.explain_level", "STANDARD") is in 'properties' of HiveConf.
-- ("explain_level", "STANDARD") is in 'properties' of HiveConf.
-- ("explain_level", "STANDARD") is in 'lowercaseProperties' of HiveConf.
set impala.explain_level;

set impala.explain_level=MINIMAL;
set explain_level=MINIMAL;
-- ("impala.explain_level", "MINIMAL") is in 'properties' of HiveConf.
-- ("explain_level", "MINIMAL") is in 'properties' of HiveConf.
-- ("explain_level", "MINIMAL") is in 'lowercaseProperties' of HiveConf.
set explain_level=VERBOSE;
-- ("impala.explain_level", "MINIMAL") is in 'properties' of HiveConf.
-- ("explain_level", "VERBOSE") is in 'properties' of HiveConf.
-- ("explain_level", "VERBOSE") is in 'lowercaseProperties' of HiveConf.
set impala.explain_level=MINIMAL;
-- ("impala.explain_level", "MINIMAL") is in 'properties' of HiveConf.
-- ("explain_level", "MINIMAL") is in 'properties' of HiveConf.
-- ("explain_level", "MINIMAL") is in 'lowercaseProperties' of HiveConf.
explain select 3;
-- ("impala.explain_level", "MINIMAL") is in 'properties' of HiveConf.
-- ("explain_level", "MINIMAL") is in 'properties' of HiveConf.
-- ("explain_level", "MINIMAL") is in 'lowercaseProperties' of HiveConf.
set impala.explain_level;

set enabled_runtime_filter_types=BLOOM,MIN_MAX,IN_LIST;
-- executes a query
explain select count(*) from impala_tpch_lineitem;
-- verifies that the value of the prefixed option is updated after a query is executed.
set impala.enabled_runtime_filter_types;

set enabled_runtime_filter_types=1,2;
-- executes a query
explain select count(*) from impala_tpch_lineitem;
-- verifies that the value of the prefixed option is updated after a query is executed
-- and that the value of each runtime filter type could be numerical.
set impala.enabled_runtime_filter_types;

set enabled_runtime_filter_types=BLOOM,2;
-- executes a query
explain select count(*) from impala_tpch_lineitem;
-- verifies that the value of the prefixed option is updated after a query is executed
-- and that the value of each runtime filter type on the right-hand side of the
-- assignment could either be represented by an integer or a string.
set impala.enabled_runtime_filter_types;

set impala.num_nodes="3";
-- verifies the double quotation marks are removed in the prefixed option.
set impala.num_nodes;
-- verifies the double quotation marks are not removed for the non-prefixed option.
set num_nodes;

set num_nodes="4";
-- verifies the value of 'impala.num_nodes' is still 3 before the execution of the next query.
set impala.num_nodes;
-- executes a query
explain select count(*) from impala_tpch_lineitem;
-- verifies the double quotation marks are removed in the prefixed option.
set impala.num_nodes;
-- verifies the double quotation marks are not removed for the non-prefixed option.
set num_nodes;
