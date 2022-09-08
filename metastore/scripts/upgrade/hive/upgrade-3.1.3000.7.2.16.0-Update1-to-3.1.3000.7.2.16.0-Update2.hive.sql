SELECT 'Upgrading MetaStore schema from 3.1.3000.7.2.15.0-Update5 to 3.1.3000.7.2.15.0-Update6';

USE SYS;

CREATE OR REPLACE VIEW `COMPACTIONS`
            (
             `C_ID`,
             `C_CATALOG`,
             `C_DATABASE`,
             `C_TABLE`,
             `C_PARTITION`,
             `C_TYPE`,
             `C_STATE`,
             `C_WORKER_HOST`,
             `C_WORKER_ID`,
             `C_WORKER_VERSION`,
             `C_ENQUEUE_TIME`,
             `C_START`,
             `C_DURATION`,
             `C_HADOOP_JOB_ID`,
             `C_RUN_AS`,
             `C_ERROR_MESSAGE`,
             `C_NEXT_TXN_ID`,
             `C_TXN_ID`,
             `C_COMMIT_TIME`,
             `C_HIGHEST_WRITE_ID`,
             `C_INITIATOR_HOST`,
             `C_INITIATOR_ID`,
             `C_INITIATOR_VERSION`,
             `C_CLEANER_START`,
             `C_TBLPROPERTIES`
                ) AS
SELECT
    CC_ID,
    'default',
    CC_DATABASE,
    CC_TABLE,
    CC_PARTITION,
    CASE WHEN CC_TYPE = 'i' THEN 'minor' WHEN CC_TYPE = 'a' THEN 'major' ELSE 'UNKNOWN' END,
    CASE WHEN CC_STATE = 'f' THEN 'failed' WHEN CC_STATE = 's' THEN 'succeeded'
         WHEN CC_STATE = 'a' THEN 'did not initiate' WHEN CC_STATE = 'c' THEN 'refused' ELSE 'UNKNOWN' END,
    CASE WHEN CC_WORKER_ID IS NULL THEN cast (null as string) ELSE split(CC_WORKER_ID,"-")[0] END,
    CASE WHEN CC_WORKER_ID IS NULL THEN cast (null as string) ELSE split(CC_WORKER_ID,"-")[size(split(CC_WORKER_ID,"-"))-1] END,
    CC_WORKER_VERSION,
    FROM_UNIXTIME(CC_ENQUEUE_TIME DIV 1000),
    FROM_UNIXTIME(CC_START DIV 1000),
    CASE WHEN CC_END IS NULL THEN cast (null as string) ELSE CC_END-CC_START END,
    CC_HADOOP_JOB_ID,
    CC_RUN_AS,
    CC_ERROR_MESSAGE,
    CC_NEXT_TXN_ID,
    CC_TXN_ID,
    FROM_UNIXTIME(CC_COMMIT_TIME DIV 1000),
    CC_HIGHEST_WRITE_ID,
    CASE WHEN CC_INITIATOR_ID IS NULL THEN cast (null as string) ELSE split(CC_INITIATOR_ID,"-")[0] END,
    CASE WHEN CC_INITIATOR_ID IS NULL THEN cast (null as string) ELSE split(CC_INITIATOR_ID,"-")[size(split(CC_INITIATOR_ID,"-"))-1] END,
    CC_INITIATOR_VERSION,
    NULL,
    CC_TBLPROPERTIES
FROM COMPLETED_COMPACTIONS
UNION ALL
SELECT
    CQ_ID,
    'default',
    CQ_DATABASE,
    CQ_TABLE,
    CQ_PARTITION,
    CASE WHEN CQ_TYPE = 'i' THEN 'minor' WHEN CQ_TYPE = 'a' THEN 'major' ELSE 'UNKNOWN' END,
    CASE WHEN CQ_STATE = 'i' THEN 'initiated' WHEN CQ_STATE = 'w' THEN 'working' WHEN CQ_STATE = 'r' THEN 'ready for cleaning' ELSE 'UNKNOWN' END,
    CASE WHEN CQ_WORKER_ID IS NULL THEN NULL ELSE split(CQ_WORKER_ID,"-")[0] END,
    CASE WHEN CQ_WORKER_ID IS NULL THEN NULL ELSE split(CQ_WORKER_ID,"-")[size(split(CQ_WORKER_ID,"-"))-1] END,
    CQ_WORKER_VERSION,
    FROM_UNIXTIME(CQ_ENQUEUE_TIME DIV 1000),
    FROM_UNIXTIME(CQ_START DIV 1000),
    cast (null as string),
    CQ_HADOOP_JOB_ID,
    CQ_RUN_AS,
    CQ_ERROR_MESSAGE,
    CQ_NEXT_TXN_ID,
    CQ_TXN_ID,
    FROM_UNIXTIME(CQ_COMMIT_TIME DIV 1000),
    CQ_HIGHEST_WRITE_ID,
    CASE WHEN CQ_INITIATOR_ID IS NULL THEN NULL ELSE split(CQ_INITIATOR_ID,"-")[0] END,
    CASE WHEN CQ_INITIATOR_ID IS NULL THEN NULL ELSE split(CQ_INITIATOR_ID,"-")[size(split(CQ_INITIATOR_ID,"-"))-1] END,
    CQ_INITIATOR_VERSION,
    FROM_UNIXTIME(CQ_CLEANER_START DIV 1000),
    CQ_TBLPROPERTIES
FROM COMPACTION_QUEUE;

CREATE OR REPLACE VIEW `TRANSACTIONS` (
                                       `TXN_ID`,
                                       `STATE`,
                                       `STARTED`,
                                       `LAST_HEARTBEAT`,
                                       `USER`,
                                       `HOST`,
                                       `AGENT_INFO`,
                                       `META_INFO`,
                                       `HEARTBEAT_COUNT`,
                                       `TYPE`,
                                       `TC_DATABASE`,
                                       `TC_TABLE`,
                                       `TC_PARTITION`,
                                       `TC_OPERATION_TYPE`,
                                       `TC_WRITEID`
    ) AS
SELECT DISTINCT
    T.`TXN_ID`,
    CASE WHEN T.`TXN_STATE` = 'o' THEN 'open' WHEN T.`TXN_STATE` = 'a' THEN 'aborted' WHEN T.`TXN_STATE` = 'c' THEN 'commited' ELSE 'UNKNOWN' END  AS TXN_STATE,
    FROM_UNIXTIME(T.`TXN_STARTED` DIV 1000) AS TXN_STARTED,
    FROM_UNIXTIME(T.`TXN_LAST_HEARTBEAT` DIV 1000) AS TXN_LAST_HEARTBEAT,
    T.`TXN_USER`,
    T.`TXN_HOST`,
    T.`TXN_AGENT_INFO`,
    T.`TXN_META_INFO`,
    T.`TXN_HEARTBEAT_COUNT`,
    CASE WHEN T.`TXN_TYPE` = 0 THEN 'DEFAULT' WHEN T.`TXN_TYPE` = 1 THEN 'REPL_CREATED' WHEN T.`TXN_TYPE` = 2 THEN 'READ_ONLY' WHEN T.`TXN_TYPE` = 3 THEN 'COMPACTION' END AS TXN_TYPE,
    TC.`TC_DATABASE`,
    TC.`TC_TABLE`,
    TC.`TC_PARTITION`,
    CASE WHEN TC.`TC_OPERATION_TYPE` = 's' THEN 'SELECT' WHEN TC.`TC_OPERATION_TYPE` = 'i' THEN 'INSERT' WHEN TC.`TC_OPERATION_TYPE` = 'u' THEN 'UPDATE' WHEN TC.`TC_OPERATION_TYPE` = 'c' THEN 'COMPACT' END AS OPERATION_TYPE,
    TC.`TC_WRITEID`
FROM `SYS`.`TXNS` AS T
         LEFT JOIN `SYS`.`TXN_COMPONENTS` AS TC ON T.`TXN_ID` = TC.`TC_TXNID`;

CREATE OR REPLACE VIEW SYS.CDH_VERSION AS SELECT 1 AS VER_ID, '3.1.3000.7.2.16.0-Update2' AS SCHEMA_VERSION,
                                                 'Hive release version 3.1.3000 for CDH 7.2.16.0-Update2' AS VERSION_COMMENT;

SELECT 'Finished upgrading MetaStore schema from 3.1.3000.7.2.16.0-Update1 to 3.1.3000.7.2.16.0-Update2';
