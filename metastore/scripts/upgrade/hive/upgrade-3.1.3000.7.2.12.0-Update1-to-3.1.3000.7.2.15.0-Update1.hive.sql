SELECT 'Upgrading MetaStore schema from 3.1.3000.7.2.12.0-Update1 to 3.1.3000.7.2.15.0-Update1';

USE SYS;

CREATE EXTERNAL TABLE IF NOT EXISTS `NOTIFICATION_LOG` (
  `NL_ID` bigint,
  `EVENT_ID` bigint,
  `EVENT_TIME` int,
  `EVENT_TYPE` varchar(32),
  `CAT_NAME` varchar(256),
  `DB_NAME` varchar(128),
  `TBL_NAME` varchar(256),
  `MESSAGE` string,
  `MESSAGE_FORMAT` varchar(16)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" =
"SELECT
    \"NL_ID\",
    \"EVENT_ID\",
    \"EVENT_TIME\",
    \"EVENT_TYPE\",
    \"CAT_NAME\",
    \"DB_NAME\",
    \"TBL_NAME\",
    \"MESSAGE\",
    \"MESSAGE_FORMAT\"
FROM \"NOTIFICATION_LOG\""
);

CREATE OR REPLACE VIEW SYS.CDH_VERSION AS SELECT 1 AS VER_ID, '3.1.3000.7.2.15.0-Update1' AS SCHEMA_VERSION,
  'Hive release version 3.1.3000 for CDH 7.2.15.0-Update1' AS VERSION_COMMENT;

SELECT 'Finished upgrading MetaStore schema from 3.1.3000.7.2.12.0-Update1 to 3.1.3000.7.2.15.0-Update1';
