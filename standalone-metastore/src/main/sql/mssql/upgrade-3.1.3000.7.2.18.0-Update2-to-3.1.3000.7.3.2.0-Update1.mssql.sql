SELECT 'Upgrading MetaStore schema from 3.1.3000.7.2.18.0-Update2 to 3.1.3000.7.3.2.0-Update1' AS MESSAGE;

DROP INDEX IF EXISTS MIN_HISTORY_WRITE_ID.MIN_HISTORY_WRITE_ID_IDX;
CREATE INDEX MIN_HISTORY_WRITE_ID_IDX ON MIN_HISTORY_WRITE_ID (MH_DATABASE, MH_TABLE, MH_WRITEID);

-- These lines need to be last.  Insert any changes above.
UPDATE CDH_VERSION SET SCHEMA_VERSION='3.1.3000.7.3.2.0-Update1', VERSION_COMMENT='Hive release version 3.1.3000 for CDH 7.3.2.0-Update1' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 3.1.3000.7.2.18.0-Update2 to 3.1.3000.7.3.2.0-Update1' AS MESSAGE;
