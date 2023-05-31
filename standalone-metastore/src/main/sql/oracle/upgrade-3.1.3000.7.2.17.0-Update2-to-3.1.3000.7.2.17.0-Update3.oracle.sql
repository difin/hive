SELECT 'Upgrading MetaStore schema from 3.1.3000.7.2.17.0-Update2 to 3.1.3000.7.2.17.0-Update3' AS Status from dual;

-- HIVE-26221
ALTER TABLE TAB_COL_STATS ADD HISTOGRAM BLOB;
ALTER TABLE PART_COL_STATS ADD HISTOGRAM BLOB;

-- These lines need to be last.  Insert any changes above.
UPDATE CDH_VERSION SET SCHEMA_VERSION='3.1.3000.7.2.17.0-Update3', VERSION_COMMENT='Hive release version 3.1.3000 for CDH 7.2.17.0-Update3' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 3.1.3000.7.2.17.0-Update2 to 3.1.3000.7.2.17.0-Update3' AS Status from dual;
