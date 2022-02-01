-- HIVE-25737
ALTER TABLE COMPACTION_QUEUE ADD CQ_CLEANER_START bigint NULL;

-- HIVE-25842
CREATE TABLE COMPACTION_METRICS_CACHE (
                                          CMC_DATABASE nvarchar(128) NOT NULL,
                                          CMC_TABLE nvarchar(128) NOT NULL,
                                          CMC_PARTITION nvarchar(767) NULL,
                                          CMC_METRIC_TYPE nvarchar(128) NOT NULL,
                                          CMC_METRIC_VALUE int NOT NULL,
                                          CMC_VERSION int NOT NULL
);

-- These lines need to be last.  Insert any changes above.
UPDATE CDH_VERSION SET SCHEMA_VERSION='3.1.3000.7.2.15.0-Update5', VERSION_COMMENT='Hive release version 3.1.3000 for CDH 7.2.15.0-Update5' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 3.1.3000.7.2.15.0-Update4 to 3.1.3000.7.2.15.0-Update5' AS MESSAGE;
