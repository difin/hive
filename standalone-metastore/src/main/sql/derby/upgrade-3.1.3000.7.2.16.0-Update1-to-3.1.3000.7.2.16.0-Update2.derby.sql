-- Upgrade MetaStore schema from 3.1.3000.7.2.15.0-Update1 to 3.1.3000.7.2.16.0-Update2

-- HIVE-26280
ALTER TABLE COMPLETED_COMPACTIONS ADD CC_NEXT_TXN_ID bigint;
ALTER TABLE COMPLETED_COMPACTIONS ADD CC_TXN_ID bigint;
ALTER TABLE COMPLETED_COMPACTIONS ADD CC_COMMIT_TIME bigint;

-- This needs to be the last thing done.  Insert any changes above this line.
UPDATE "APP".CDH_VERSION SET SCHEMA_VERSION='3.1.3000.7.2.16.0-Update2', VERSION_COMMENT='Hive release version 3.1.3000 for CDH 7.2.16.0-Update2' where VER_ID=1;
