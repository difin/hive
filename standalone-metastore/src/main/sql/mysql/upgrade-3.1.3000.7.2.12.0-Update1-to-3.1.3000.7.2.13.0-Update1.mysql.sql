--Increase the size of RM_PROGRESS to accomodate the replication statistics
ALTER TABLE REPLICATION_METRICS MODIFY RM_PROGRESS varchar(24000);

-- These lines need to be last.  Insert any changes above.
UPDATE CDH_VERSION SET SCHEMA_VERSION='3.1.3000.7.2.13.0-Update1', VERSION_COMMENT='Hive release version 3.1.3000 for CDH 7.2.13.0-Update1' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 3.1.3000.7.2.12.0-Update1 to 3.1.3000.7.2.13.0-Update1';
