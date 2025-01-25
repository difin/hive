--! qt:disabled:Re-enable this q test once IMPALA-2945 is merged.
set metastore.metadata.transformer.class=org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer;

--! qt:dataset:impala_dataset

explain cbo physical select
  100.00 * sum(case
    when p_type like 'PROMO%'
    then l_extendedprice * (1 - l_discount)
    else 0.0
    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
  impala_tpch_lineitem,
  impala_tpch_part
where 
  l_partkey = p_partkey 
  and l_shipdate >= '1995-09-01'
  and l_shipdate < '1995-10-01';

explain select
  100.00 * sum(case
    when p_type like 'PROMO%'
    then l_extendedprice * (1 - l_discount)
    else 0.0
    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
  impala_tpch_lineitem,
  impala_tpch_part
where 
  l_partkey = p_partkey 
  and l_shipdate >= '1995-09-01'
  and l_shipdate < '1995-10-01';
