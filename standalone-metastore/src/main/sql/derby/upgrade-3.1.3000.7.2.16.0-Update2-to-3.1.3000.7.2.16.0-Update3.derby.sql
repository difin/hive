-- Upgrade MetaStore schema from 3.1.3000.7.2.15.0-Update2 to 3.1.3000.7.2.16.0-Update3

ALTER TABLE "APP"."COMPACTION_QUEUE" ADD COLUMN "CQ_POOL_NAME" VARCHAR(128);
ALTER TABLE "APP"."COMPLETED_COMPACTIONS" ADD COLUMN "CC_POOL_NAME" VARCHAR(128);

-- This needs to be the last thing done.  Insert any changes above this line.
UPDATE "APP"."CDH_VERSION" SET "SCHEMA_VERSION"='3.1.3000.7.2.16.0-Update3', "VERSION_COMMENT"='Hive release version 3.1.3000 for CDH 7.2.16.0-Update3' where "VER_ID"=1;
