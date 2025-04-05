--! qt:replace:/(MINOR\s+succeeded\s+)[a-zA-Z0-9\-\.\s+]+(\s+manual)/$1#Masked#$2/

set hive.explain.user=true;
set hive.auto.convert.join=true;
set hive.optimize.dynamic.partition.hashjoin=false;
set hive.convert.join.bucket.mapjoin.tez=true;

CREATE TABLE srcbucket_big(key int, value string, id int)
PARTITIONED BY SPEC(bucket(4, key)) STORED BY ICEBERG
TBLPROPERTIES ('compactor.threshold.min.input.files'='1');
INSERT INTO srcbucket_big VALUES
(101, 'val_101', 1),
(null, 'val_102', 2),
(103, 'val_103', 3),
(104, null, 4),
(105, 'val_105', 5),
(null, null, 6);
ALTER TABLE srcbucket_big CREATE TAG bucket_4;

ALTER TABLE srcbucket_big SET PARTITION SPEC (bucket(8, key));
INSERT INTO srcbucket_big VALUES
(101, 'val_101', 7),
(null, 'val_102', 8),
(103, 'val_103', 9),
(104, null, 10),
(105, 'val_105', 11),
(null, null, 12);
ALTER TABLE srcbucket_big CREATE TAG bucket_4_and_8;

CREATE TABLE src_small(key int, value string);
INSERT INTO src_small VALUES
(101, 'val_101'),
(null, 'val_102'),
(103, 'val_103'),
(104, null),
(105, 'val_105'),
(null, null);

select `partition`, spec_id, record_count
from default.srcbucket_big.partitions
order by `partition`, spec_id, record_count;

SELECT * FROM default.srcbucket_big ORDER BY id;
SELECT * FROM default.srcbucket_big.tag_bucket_4 ORDER BY id;
SELECT * FROM default.srcbucket_big.tag_bucket_4_and_8 ORDER BY id;

alter table srcbucket_big compact 'minor' and wait;
show compactions order by 'partition';

select `partition`, spec_id, record_count
from default.srcbucket_big.partitions
order by `partition`, spec_id, record_count;

SELECT * FROM default.srcbucket_big ORDER BY id;
SELECT * FROM default.srcbucket_big.tag_bucket_4 ORDER BY id;
SELECT * FROM default.srcbucket_big.tag_bucket_4_and_8 ORDER BY id;
