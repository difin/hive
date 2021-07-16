-- The file has some overlapping with upgrade-3.2.0-to-4.0.0.postgres.sql
SELECT 'Upgrading MetaStore schema from 3.1.3000 to 4.0.0';

-- HIVE-20793
ALTER TABLE "WM_RESOURCEPLAN" ADD "NS" character varying(128);
UPDATE "WM_RESOURCEPLAN" SET "NS" = 'default' WHERE "NS" IS NULL;
ALTER TABLE "WM_RESOURCEPLAN" DROP CONSTRAINT "UNIQUE_WM_RESOURCEPLAN";
ALTER TABLE ONLY "WM_RESOURCEPLAN" ADD CONSTRAINT "UNIQUE_WM_RESOURCEPLAN" UNIQUE ("NS", "NAME");

-- HIVE-22046 (DEFAULT HIVE)
UPDATE "TAB_COL_STATS" SET "ENGINE" = 'hive' WHERE "ENGINE" IS NULL;
UPDATE "PART_COL_STATS" SET "ENGINE" = 'hive' WHERE "ENGINE" IS NULL;

-- HIVE-23683
ALTER TABLE "COMPACTION_QUEUE" ADD "CQ_ENQUEUE_TIME" bigint;
ALTER TABLE "COMPLETED_COMPACTIONS" ADD "CC_ENQUEUE_TIME" bigint;

-- HIVE-22728
ALTER TABLE "KEY_CONSTRAINTS" DROP CONSTRAINT "KEY_CONSTRAINTS_pkey";
ALTER TABLE "KEY_CONSTRAINTS" ADD CONSTRAINT "CONSTRAINTS_PK" PRIMARY KEY ("PARENT_TBL_ID", "CONSTRAINT_NAME", "POSITION");

-- HIVE-21487
CREATE INDEX "COMPLETED_COMPACTIONS_RES" ON "COMPLETED_COMPACTIONS" ("CC_DATABASE","CC_TABLE","CC_PARTITION");


-- HIVE-23107
ALTER TABLE "COMPACTION_QUEUE" ADD "CQ_NEXT_TXN_ID" bigint;
--DROP TABLE "MIN_HISTORY_LEVEL";

-- HIVE-23048
INSERT INTO "TXNS" ("TXN_ID", "TXN_STATE", "TXN_STARTED", "TXN_LAST_HEARTBEAT", "TXN_USER", "TXN_HOST")
SELECT COALESCE(MAX("CTC_TXNID"),0), 'c', 0, 0, '', '' FROM "COMPLETED_TXN_COMPONENTS";
CREATE SEQUENCE "TXNS_TXN_ID_SEQ" MINVALUE 0 OWNED BY "TXNS"."TXN_ID";
select setval('"TXNS_TXN_ID_SEQ"',  (SELECT MAX("TXN_ID") FROM "TXNS"));
ALTER TABLE "TXNS" ALTER "TXN_ID" SET DEFAULT nextval('"TXNS_TXN_ID_SEQ"');

ALTER TABLE "NEXT_TXN_ID" RENAME TO "TXN_LOCK_TBL";
ALTER TABLE "TXN_LOCK_TBL" RENAME COLUMN "NTXN_NEXT" TO "TXN_LOCK";


--Create table replication metrics
CREATE TABLE "REPLICATION_METRICS" (
                                       "RM_SCHEDULED_EXECUTION_ID" bigint NOT NULL,
                                       "RM_POLICY" varchar(256) NOT NULL,
    "RM_DUMP_EXECUTION_ID" bigint NOT NULL,
    "RM_METADATA" varchar(4000),
    "RM_PROGRESS" varchar(4000),
    PRIMARY KEY("RM_SCHEDULED_EXECUTION_ID")
    );

--Create indexes for the replication metrics table
CREATE INDEX "POLICY_IDX" ON "REPLICATION_METRICS" ("RM_POLICY");
CREATE INDEX "DUMP_IDX" ON "REPLICATION_METRICS" ("RM_DUMP_EXECUTION_ID");

-- Create stored procedure tables
CREATE TABLE "STORED_PROCS" (
                                "SP_ID" BIGINT NOT NULL,
                                "CREATE_TIME" INTEGER NOT NULL,
                                "DB_ID" BIGINT NOT NULL,
                                "NAME" VARCHAR(256) NOT NULL,
    "OWNER_NAME" VARCHAR(128) NOT NULL,
    "SOURCE" TEXT NOT NULL,
    PRIMARY KEY ("SP_ID")
    );

CREATE UNIQUE INDEX "UNIQUESTOREDPROC" ON "STORED_PROCS" ("NAME", "DB_ID");
ALTER TABLE ONLY "STORED_PROCS" ADD CONSTRAINT "STOREDPROC_FK1" FOREIGN KEY ("DB_ID") REFERENCES "DBS" ("DB_ID") DEFERRABLE;

-- HIVE-24291
ALTER TABLE "COMPACTION_QUEUE" ADD "CQ_TXN_ID" bigint;

-- HIVE-24275
ALTER TABLE "COMPACTION_QUEUE" ADD "CQ_COMMIT_TIME" bigint;

-- HIVE-24880
ALTER TABLE "COMPACTION_QUEUE" ADD "CQ_INITIATOR_ID" varchar(128);
ALTER TABLE "COMPACTION_QUEUE" ADD "CQ_INITIATOR_VERSION" varchar(128);
ALTER TABLE "COMPACTION_QUEUE" ADD "CQ_WORKER_VERSION" varchar(128);
ALTER TABLE "COMPLETED_COMPACTIONS" ADD "CC_INITIATOR_ID" varchar(128);
ALTER TABLE "COMPLETED_COMPACTIONS" ADD "CC_INITIATOR_VERSION" varchar(128);
ALTER TABLE "COMPLETED_COMPACTIONS" ADD "CC_WORKER_VERSION" varchar(128);

-- HIVE-24770
UPDATE "SERDES" SET "SLIB"='org.apache.hadoop.hive.serde2.MultiDelimitSerDe' where "SLIB"='org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe';

-- HIVE-24396
-- Create DataConnectors and DataConnector_Params tables
CREATE TABLE "DATACONNECTORS" (
  "NAME" character varying(128) NOT NULL,
  "TYPE" character varying(32) NOT NULL,
  "URL" character varying(4000) NOT NULL,
  "COMMENT" character varying(256),
  "OWNER_NAME" character varying(256),
  "OWNER_TYPE" character varying(10),
  "CREATE_TIME" INTEGER NOT NULL,
  PRIMARY KEY ("NAME")
);

CREATE TABLE "DATACONNECTOR_PARAMS" (
  "NAME" character varying(128) NOT NULL,
  "PARAM_KEY" character varying(180) NOT NULL,
  "PARAM_VALUE" character varying(4000),
  PRIMARY KEY ("NAME", "PARAM_KEY"),
  CONSTRAINT "DATACONNECTOR_NAME_FK1" FOREIGN KEY ("NAME") REFERENCES "DATACONNECTORS"("NAME") ON DELETE CASCADE
);
ALTER TABLE "DBS" ADD "TYPE" character varying(32) DEFAULT 'NATIVE' NOT NULL;
ALTER TABLE "DBS" ADD "DATACONNECTOR_NAME" character varying(128);
ALTER TABLE "DBS" ADD "REMOTE_DBNAME" character varying(128);
UPDATE "DBS" SET "TYPE"= 'NATIVE' WHERE "TYPE" IS NULL;

-- These lines need to be last. Insert any changes above.
UPDATE "VERSION" SET "SCHEMA_VERSION"='4.0.0', "VERSION_COMMENT"='Hive release version 4.0.0' where "VER_ID"=1;
SELECT 'Finished upgrading MetaStore schema from 3.1.3000 to 4.0.0';