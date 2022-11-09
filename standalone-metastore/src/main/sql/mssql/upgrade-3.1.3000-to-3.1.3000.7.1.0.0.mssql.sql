SELECT 'Upgrading MetaStore schema from 3.1.3000 to 3.1.3000.7.1.0.0';
CREATE TABLE "CDH_VERSION" (
"VER_ID" bigint NOT NULL, CONSTRAINT CDH_VERSION_pkey PRIMARY KEY ("VER_ID"),
  "SCHEMA_VERSION" character varying(127) NOT NULL,
  "VERSION_COMMENT" character varying(255) NOT NULL
);


INSERT INTO "CDH_VERSION" ("VER_ID", "SCHEMA_VERSION", "VERSION_COMMENT") VALUES (1, '3.1.3000.7.1.0.0', 'Hive release version 3.1.3000 for CDH 7.1.0.0');

SELECT 'Finished upgrading MetaStore schema from 3.1.3000 to 3.1.3000.7.1.0.0';