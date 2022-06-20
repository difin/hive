 -- Empty upgrade files to keep the metastore schema version and the sysdb/information_schema version consistent.
 -- These lines need to be last.  Insert any changes above.
 UPDATE CDH_VERSION SET SCHEMA_VERSION='3.1.3000.7.2.15.0-Update4', VERSION_COMMENT='Hive release version 3.1.3000 for CDH 7.2.15.0-Update4' where VER_ID=1;
 SELECT 'Finished upgrading MetaStore schema from 3.1.3000.7.2.15.0-Update3 to 3.1.3000.7.2.15.0-Update4' AS Status from dual;

