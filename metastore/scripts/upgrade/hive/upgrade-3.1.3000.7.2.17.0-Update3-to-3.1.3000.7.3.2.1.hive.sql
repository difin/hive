SELECT 'Upgrading MetaStore schema from 3.1.3000.7.2.17.0-Update3 to 3.1.3000.7.3.2.1';

USE SYS;

DROP TABLE IF EXISTS `PROTO_HIVE_QUERY_DATA`;
CREATE EXTERNAL TABLE IF NOT EXISTS `PROTO_HIVE_QUERY_DATA`
PARTITIONED BY (
  `date` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.protobuf.LenientProtobufMessageInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '_REPLACE_WITH_QUERY_DATA_LOCATION_'
TBLPROPERTIES (
  'proto.class'='org.apache.hadoop.hive.ql.hooks.proto.HiveHookEvents$HiveHookEventProto',
  'proto.maptypes'='org.apache.hadoop.hive.ql.hooks.proto.MapFieldEntry'
);


DROP TABLE IF EXISTS `PROTO_TEZ_APP_DATA`;
CREATE EXTERNAL TABLE IF NOT EXISTS `PROTO_TEZ_APP_DATA`
PARTITIONED BY (
  `date` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.protobuf.LenientProtobufMessageInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '_REPLACE_WITH_APP_DATA_LOCATION_'
TBLPROPERTIES (
  'proto.class'='org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos$HistoryEventProto',
  'proto.maptypes'='KVPair'
);

DROP TABLE IF EXISTS `PROTO_TEZ_DAG_DATA`;
CREATE EXTERNAL TABLE IF NOT EXISTS `PROTO_TEZ_DAG_DATA`
PARTITIONED BY (
  `date` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.protobuf.LenientProtobufMessageInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '_REPLACE_WITH_DAG_DATA_LOCATION_'
TBLPROPERTIES (
  'proto.class'='org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos$HistoryEventProto',
  'proto.maptypes'='KVPair'
);


DROP TABLE IF EXISTS `PROTO_TEZ_DAG_META`;
CREATE EXTERNAL TABLE IF NOT EXISTS `PROTO_TEZ_DAG_META`
PARTITIONED BY (
  `date` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.protobuf.LenientProtobufMessageInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '_REPLACE_WITH_DAG_META_LOCATION_'
TBLPROPERTIES (
  'proto.class'='org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos$ManifestEntryProto'
);

CREATE OR REPLACE VIEW SYS.CDH_VERSION AS SELECT 1 AS VER_ID, '3.1.3000.7.3.2.1' AS SCHEMA_VERSION,
                                                 'Hive release version 3.1.3000 for CDH 7.3.2.1' AS VERSION_COMMENT;

SELECT 'Finished upgrading MetaStore schema from 3.1.3000.7.2.17.0-Update3 to 3.1.3000.7.3.2.1';
