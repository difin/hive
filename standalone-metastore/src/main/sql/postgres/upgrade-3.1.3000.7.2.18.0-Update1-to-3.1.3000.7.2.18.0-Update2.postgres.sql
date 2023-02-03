SELECT 'Upgrading MetaStore schema from 3.1.3000.7.2.18.0-Update1 to 3.1.3000.7.2.18.0-Update2';

--CDPD-49032
DROP INDEX "TAB_COL_STATS_IDX";
CREATE INDEX "TAB_COL_STATS_IDX" ON "TAB_COL_STATS" USING btree ("DB_NAME","TABLE_NAME","COLUMN_NAME", "CAT_NAME");
DROP INDEX "PCS_STATS_IDX";
CREATE INDEX "PCS_STATS_IDX" ON "PART_COL_STATS" USING btree ("DB_NAME","TABLE_NAME","COLUMN_NAME","PARTITION_NAME","CAT_NAME");

-- These lines need to be last.  Insert any changes above.
UPDATE "CDH_VERSION" SET "SCHEMA_VERSION"='3.1.3000.7.2.18.0-Update2', "VERSION_COMMENT"='Hive release version 3.1.3000 for CDH 7.2.18.0-Update2' where "VER_ID"=1;
SELECT 'Finished upgrading MetaStore schema from 3.1.3000.7.2.18.0-Update1 to 3.1.3000.7.2.18.0-Update2';
