SELECT 'Upgrading MetaStore schema from 3.1.3000.7.3.2.0-Update1 to 3.1.3000.7.3.2.0-Update2' AS MESSAGE;

ALTER TABLE "COLUMNS_V2" ALTER COLUMN "COMMENT" nvarchar(4000);

-- These lines need to be last.  Insert any changes above.
UPDATE CDH_VERSION SET SCHEMA_VERSION='3.1.3000.7.3.2.0-Update2', VERSION_COMMENT='Hive release version 3.1.3000 for CDH 7.3.2.0-Update2' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 3.1.3000.7.3.2.0-Update1 to 3.1.3000.7.3.2.0-Update2' AS MESSAGE;
