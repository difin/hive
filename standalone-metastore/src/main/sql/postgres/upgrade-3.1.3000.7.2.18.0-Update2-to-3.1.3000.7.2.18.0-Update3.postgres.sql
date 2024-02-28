SELECT 'Upgrading MetaStore schema from 3.1.3000.7.2.18.0-Update2 to 3.1.3000.7.2.18.0-Update3';

-- CDPD-23041: HIVE-24815: Remove "IDXS" Table from Metastore Schema
DROP TABLE IF EXISTS "INDEX_PARAMS";
DROP TABLE IF EXISTS "IDXS";

-- These lines need to be last.  Insert any changes above.
UPDATE "CDH_VERSION" SET "SCHEMA_VERSION"='3.1.3000.7.2.18.0-Update3', "VERSION_COMMENT"='Hive release version 3.1.3000 for CDH 7.2.18.0-Update3' where "VER_ID"=1;
SELECT 'Finished upgrading MetaStore schema from 3.1.3000.7.2.18.0-Update2 to 3.1.3000.7.2.18.0-Update3';
