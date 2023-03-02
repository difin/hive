SELECT 'Upgrading MetaStore schema from 3.1.3000.7.2.16.0-Update4 to 3.1.3000.7.2.17.0-Update1' AS MESSAGE;

-- HIVE-26704
CREATE TABLE MIN_HISTORY_WRITE_ID (
  MH_TXNID bigint NOT NULL REFERENCES TXNS (TXN_ID),
  MH_DATABASE nvarchar(128) NOT NULL,
  MH_TABLE nvarchar(256) NOT NULL,
  MH_WRITEID bigint NOT NULL
);

CREATE UNIQUE INDEX MIN_HISTORY_WRITE_ID_IDX ON MIN_HISTORY_WRITE_ID (MH_DATABASE, MH_TABLE, MH_WRITEID);

-- These lines need to be last.  Insert any changes above.
UPDATE CDH_VERSION SET SCHEMA_VERSION='3.1.3000.7.2.17.0-Update1', VERSION_COMMENT='Hive release version 3.1.3000 for CDH 7.2.17.0-Update1' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 3.1.3000.7.2.16.0-Update4 to 3.1.3000.7.2.17.0-Update1' AS MESSAGE;
