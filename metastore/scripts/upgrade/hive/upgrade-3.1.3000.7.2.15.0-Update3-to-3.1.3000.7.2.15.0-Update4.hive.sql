SELECT 'Upgrading MetaStore schema from 3.1.3000.7.2.15.0-Update3 to 3.1.3000.7.2.15.0-Update4';

USE SYS;

CREATE EXTERNAL TABLE IF NOT EXISTS `MIN_HISTORY_LEVEL` (
    `MHL_TXNID` bigint,
    `MHL_MIN_OPEN_TXNID` bigint
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
    \"MHL_TXNID\",
    \"MHL_MIN_OPEN_TXNID\",
FROM \"MIN_HISTORY_LEVEL\""
);

CREATE OR REPLACE VIEW SYS.CDH_VERSION AS SELECT 1 AS VER_ID, '3.1.3000.7.2.15.0-Update4' AS SCHEMA_VERSION,
  'Hive release version 3.1.3000 for CDH 7.2.15.0-Update4' AS VERSION_COMMENT;

SELECT 'Finished upgrading MetaStore schema from 3.1.3000.7.2.15.0-Update3 to 3.1.3000.7.2.15.0-Update4';
