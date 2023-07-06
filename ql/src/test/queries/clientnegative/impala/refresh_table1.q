--! qt:disabled:Re-enable this q test once IMPALA-11619 is merged.
--! qt:dataset:impala_dataset
		
		
create table managed_tbl_neg (c1 int)
		
partitioned by (p_double double , p_boolean boolean, p_bigint bigint, p_float float, p_tinyint tinyint, p_smallint smallint, p_date date);
		
		
--! Check we get Impala syntax for a refresh failure
		
refresh  
  improper_syntax `managed_tbl_neg`;
