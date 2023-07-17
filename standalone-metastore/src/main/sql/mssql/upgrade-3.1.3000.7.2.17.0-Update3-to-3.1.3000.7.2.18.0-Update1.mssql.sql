SELECT 'Upgrading MetaStore schema from 3.1.3000.7.2.17.0-Update3-to-3.1.3000.7.2.18.0-Update1' AS MESSAGE;

-- HIVE-26221
ALTER TABLE TAB_COL_STATS ADD HISTOGRAM varbinary(max);
ALTER TABLE PART_COL_STATS ADD HISTOGRAM varbinary(max);

-- These lines need to be last.  Insert any changes above.
UPDATE CDH_VERSION SET SCHEMA_VERSION='3.1.3000.7.2.18.0-Update1', VERSION_COMMENT='Hive release version 3.1.3000 for CDH 7.2.18.0-Update1' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 3.1.3000.7.2.17.0-Update3 -> 3.1.3000.7.2.18.0-Update1' AS MESSAGE;
