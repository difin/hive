SELECT 'Upgrading MetaStore schema from 3.1.3000.7.2.15.0-Update5 to 3.1.3000.7.2.15.0-Update6';

-- altering a table column length thats part of a view in not allowed in postgres.
-- DWX-13007 So drop views and recreate them after alter. CDH problem only.
drop view if exists txn_components;
drop view if exists hive_locks;
drop view if exists compaction_queue;
drop view if exists completed_compactions;
drop view if exists write_set;

-- HIVE-26049
ALTER TABLE "TXN_COMPONENTS" ALTER "TC_TABLE" TYPE varchar(256);
ALTER TABLE "HIVE_LOCKS" ALTER "HL_TABLE" TYPE varchar(256);
ALTER TABLE "COMPACTION_QUEUE" ALTER "CQ_TABLE" TYPE varchar(256);
ALTER TABLE "COMPLETED_COMPACTIONS" ALTER "CC_TABLE" TYPE varchar(256);
ALTER TABLE "WRITE_SET" ALTER "WS_TABLE" TYPE varchar(256);
ALTER TABLE "TXN_WRITE_NOTIFICATION_LOG" ALTER "WNL_TABLE" TYPE varchar(256);

-- recreate the dropped views.
CREATE OR REPLACE VIEW txn_components
    AS SELECT
        "TC_TXNID" tc_txnid,
        "TC_DATABASE" tc_database,
        "TC_TABLE" tc_table,
        "TC_PARTITION" tc_partition,
        "TC_OPERATION_TYPE" tc_operation_type,
        "TC_WRITEID" tc_writeid
    FROM "TXN_COMPONENTS";

CREATE OR REPLACE VIEW hive_locks
    AS SELECT
        "HL_LOCK_EXT_ID" hl_lock_ext_id,
        "HL_LOCK_INT_ID" hl_lock_int_id,
        "HL_TXNID" hl_txnid,
        "HL_DB" hl_db,
        "HL_TABLE" hl_table,
        "HL_PARTITION" hl_partition,
        "HL_LOCK_STATE" hl_lock_state,
        "HL_LOCK_TYPE" hl_lock_type,
        "HL_LAST_HEARTBEAT" hl_last_heartbeat,
        "HL_ACQUIRED_AT" hl_acquired_at,
        "HL_USER" hl_user,
        "HL_HOST" hl_host,
        "HL_HEARTBEAT_COUNT" hl_heartbeat_count,
        "HL_AGENT_INFO" hl_agent_info,
        "HL_BLOCKEDBY_EXT_ID" hl_blockedby_ext_id,
        "HL_BLOCKEDBY_INT_ID" hl_blockedby_int_id
    FROM "HIVE_LOCKS";

CREATE OR REPLACE VIEW compaction_queue
    AS SELECT
        "CQ_ID" cq_id,
        "CQ_DATABASE" cq_database,
        "CQ_TABLE" cq_table,
        "CQ_PARTITION" cq_partition,
        "CQ_STATE" cq_state,
        "CQ_TYPE" cq_type,
        "CQ_TBLPROPERTIES" cq_tblproperties,
        "CQ_WORKER_ID" cq_worker_id,
        "CQ_START" cq_start,
        "CQ_RUN_AS" cq_run_as,
        "CQ_HIGHEST_WRITE_ID" cq_highest_write_id,
        "CQ_META_INFO" cq_meta_info,
        "CQ_HADOOP_JOB_ID" cq_hadoop_job_id,
        "CQ_ERROR_MESSAGE" cq_error_message
    FROM "COMPACTION_QUEUE";

CREATE OR REPLACE VIEW completed_compactions
    AS SELECT
        "CC_ID" cc_id,
        "CC_DATABASE" cc_database,
        "CC_TABLE" cc_table,
        "CC_PARTITION" cc_partition,
        "CC_STATE" cc_state,
        "CC_TYPE" cc_type,
        "CC_TBLPROPERTIES" cc_tblproperties,
        "CC_WORKER_ID" cc_worker_id,
        "CC_START" cc_start,
        "CC_END" cc_end,
        "CC_RUN_AS" cc_run_as,
        "CC_HIGHEST_WRITE_ID" cc_highest_write_id,
        "CC_META_INFO" cc_meta_info,
        "CC_HADOOP_JOB_ID" cc_hadoop_job_id,
        "CC_ERROR_MESSAGE"  cc_error_message
    FROM "COMPLETED_COMPACTIONS";

CREATE OR REPLACE VIEW write_set
    AS SELECT
        "WS_DATABASE" ws_database,
        "WS_TABLE" ws_table,
        "WS_PARTITION" ws_partition,
        "WS_TXNID" ws_txnid,
        "WS_COMMIT_ID" ws_commit_id,
        "WS_OPERATION_TYPE" ws_operation_type
    FROM "WRITE_SET";

-- These lines need to be last.  Insert any changes above.
UPDATE "CDH_VERSION" SET "SCHEMA_VERSION"='3.1.3000.7.2.15.0-Update6', "VERSION_COMMENT"='Hive release version 3.1.3000 for CDH 7.2.15.0-Update6' where "VER_ID"=1;
SELECT 'Finished upgrading MetaStore schema from 3.1.3000.7.2.15.0-Update5 to 3.1.3000.7.2.15.0-Update6';
