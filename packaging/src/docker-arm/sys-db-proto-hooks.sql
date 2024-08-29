-- Copyright (c) 2020 Cloudera, Inc. All rights reserved.
drop table if exists sys.query_data;
drop table if exists sys.dag_data;

create external table if not exists sys.query_data (
	eventType string,
	hiveQueryId string,
	`timestamp` BIGINT,
	executionMode string,
	requestUser string,
	queue string,
	`user` string,
	operationId string,
	tablesWritten string,
	tablesRead string,
	otherInfo map<string, string>
	)
partitioned by (`date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageSerDe'
WITH SERDEPROPERTIES ('proto.class'='org.apache.hadoop.hive.ql.hooks.proto.HiveHookEvents$HiveHookEventProto', 'proto.maptypes'='org.apache.hadoop.hive.ql.hooks.proto.MapFieldEntry')
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageInputFormat'
   OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveNullValueSequenceFileOutputFormat'
TBLPROPERTIES ('proto.class'='org.apache.hadoop.hive.ql.hooks.proto.HiveHookEvents$HiveHookEventProto');

create external table if not exists sys.dag_data (
    event_type string,
    event_time bigint,
    `user` string,
    app_id string,
    app_attempt_id string,
    dag_id string,
    vertex_id string,
    task_id string,
    task_attempt_id string,
    event_data map<string, string>
  )
partitioned by (`date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageSerDe'
WITH SERDEPROPERTIES ('proto.class'='org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos$HistoryEventProto', 'proto.maptypes'='KVPair')
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageInputFormat'
   OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveNullValueSequenceFileOutputFormat'
TBLPROPERTIES ('proto.class'='org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos$HistoryEventProto');

MSCK REPAIR TABLE sys.query_data;
MSCK REPAIR TABLE sys.dag_data;
