SELECT 'Upgrading MetaStore schema from 3.1.3000.7.2.15.0-Update6 to 7.2.16.0-Update1';

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
ALTER TABLE "DBS" ADD COLUMN "TYPE" character varying(32);
UPDATE "DBS" SET "TYPE"= 'NATIVE' WHERE "TYPE" IS NULL;
ALTER TABLE "DBS" ALTER COLUMN "TYPE" SET DEFAULT 'NATIVE', ALTER COLUMN "TYPE" SET NOT NULL;
ALTER TABLE "DBS" ADD "DATACONNECTOR_NAME" character varying(128);
ALTER TABLE "DBS" ADD "REMOTE_DBNAME" character varying(128);

CREATE TABLE "DC_PRIVS" (
  "DC_GRANT_ID" bigint NOT NULL,
  "CREATE_TIME" bigint NOT NULL,
  "NAME" character varying(128),
  "GRANT_OPTION" smallint NOT NULL,
  "GRANTOR" character varying(128) DEFAULT NULL::character varying,
  "GRANTOR_TYPE" character varying(128) DEFAULT NULL::character varying,
  "PRINCIPAL_NAME" character varying(128) DEFAULT NULL::character varying,
  "PRINCIPAL_TYPE" character varying(128) DEFAULT NULL::character varying,
  "DC_PRIV" character varying(128) DEFAULT NULL::character varying,
  "AUTHORIZER" character varying(128) DEFAULT NULL::character varying
);

ALTER TABLE ONLY "DC_PRIVS"
ADD CONSTRAINT "DC_PRIVS_pkey" PRIMARY KEY ("DC_GRANT_ID");

ALTER TABLE ONLY "DC_PRIVS"
ADD CONSTRAINT "DC_PRIVS_DC_ID_fkey" FOREIGN KEY ("NAME") REFERENCES "DATACONNECTORS"("NAME") DEFERRABLE;

ALTER TABLE ONLY "DC_PRIVS"
ADD CONSTRAINT "DCPRIVILEGEINDEX" UNIQUE ("AUTHORIZER", "NAME", "PRINCIPAL_NAME", "PRINCIPAL_TYPE", "DC_PRIV", "GRANTOR", "GRANTOR_TYPE");

CREATE INDEX "DC_PRIVS_N49" ON "DC_PRIVS" USING btree ("NAME");

-- These lines need to be last.  Insert any changes above.
UPDATE "CDH_VERSION" SET "SCHEMA_VERSION"='3.1.3000.7.2.16.0-Update1', "VERSION_COMMENT"='Hive release version 3.1.3000 for CDH 7.2.16.0-Update1' where "VER_ID"=1;
SELECT 'Finished upgrading MetaStore schema from 3.1.3000.7.2.15.0-Update6 to 3.1.3000.7.2.16.0-Update1';
