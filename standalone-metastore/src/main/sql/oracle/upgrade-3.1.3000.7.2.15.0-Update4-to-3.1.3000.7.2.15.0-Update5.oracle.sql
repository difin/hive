-- HIVE-25737
ALTER TABLE COMPACTION_QUEUE ADD CQ_CLEANER_START NUMBER(19);

-- HIVE-25842
CREATE TABLE COMPACTION_METRICS_CACHE (
                                          CMC_DATABASE varchar(128) NOT NULL,
                                          CMC_TABLE varchar(128) NOT NULL,
                                          CMC_PARTITION varchar(767),
                                          CMC_METRIC_TYPE varchar(128) NOT NULL,
                                          CMC_METRIC_VALUE number(10) NOT NULL,
                                          CMC_VERSION number(10) NOT NULL
) ROWDEPENDENCIES;


-- These lines need to be last.  Insert any changes above.
UPDATE CDH_VERSION SET SCHEMA_VERSION='3.1.3000.7.2.15.0-Update5', VERSION_COMMENT='Hive release version 3.1.3000 for CDH 7.2.15.0-Update5' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 3.1.3000.7.2.15.0-Update4 to 3.1.3000.7.2.15.0-Update5' AS Status from dual;
