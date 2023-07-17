SELECT 'Upgrading MetaStore schema from 3.1.3000.7.2.17.0-Update2 to 3.1.3000.7.2.17.0-Update3' AS Status from dual;

-- HIVE-27457
UPDATE "SDS"
    SET "INPUT_FORMAT" = 'org.apache.hadoop.hive.kudu.KuduInputFormat', "OUTPUT_FORMAT" = 'org.apache.hadoop.hive.kudu.KuduOutputFormat'
    WHERE "SD_ID" IN (
        SELECT "TBLS"."SD_ID" FROM "TBLS"
            INNER JOIN "TABLE_PARAMS" ON "TBLS"."TBL_ID" = "TABLE_PARAMS"."TBL_ID"
                WHERE "TABLE_PARAMS"."PARAM_VALUE" LIKE '%KuduStorageHandler%'
        );
UPDATE "SERDES"
    SET "SERDES"."SLIB" = 'org.apache.hadoop.hive.kudu.KuduSerDe'
    WHERE "SERDE_ID" IN (
        SELECT "SDS"."SERDE_ID"
            FROM "TBLS"
            INNER JOIN "SDS" ON "TBLS"."SD_ID" = "SDS"."SD_ID"
            WHERE "TBLS"."TBL_ID" IN (SELECT "TBL_ID" FROM "TABLE_PARAMS" WHERE "PARAM_VALUE" LIKE '%KuduStorageHandler%')
    );

-- These lines need to be last.  Insert any changes above.
UPDATE CDH_VERSION SET SCHEMA_VERSION='3.1.3000.7.2.17.0-Update3', VERSION_COMMENT='Hive release version 3.1.3000 for CDH 7.2.17.0-Update3' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 3.1.3000.7.2.17.0-Update2 to 3.1.3000.7.2.17.0-Update3' AS Status from dual;
