SELECT 'Upgrading MetaStore schema from 3.1.3000.7.2.16.0-Update2 to 7.2.16.0-Update3' AS MESSAGE;

-- HIVE-26443
ALTER TABLE COMPACTION_QUEUE ADD CQ_POOL_NAME VARCHAR(128);
ALTER TABLE COMPLETED_COMPACTIONS ADD CC_POOL_NAME VARCHAR(128);

-- These lines need to be last.  Insert any changes above.
UPDATE CDH_VERSION SET SCHEMA_VERSION='3.1.3000.7.2.16.0-Update3', VERSION_COMMENT='Hive release version 3.1.3000 for CDH 7.2.16.0-Update3' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 3.1.3000.7.2.16.0-Update2 to 3.1.3000.7.2.16.0-Update3' AS MESSAGE;