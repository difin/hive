CREATE OR REPLACE VIEW SYS.CDH_VERSION AS SELECT 1 AS VER_ID, '3.1.3000.7.3.2.2' AS SCHEMA_VERSION,
                    'Hive release version 3.1.3000 for CDH 7.3.2.2' AS VERSION_COMMENT;

SELECT 'Finished upgrading MetaStore schema from 3.1.3000.7.2.17.0-Update3 to 3.1.3000.7.3.2';
