-- CREATE IF NOT EXISTS already
CREATE CONNECTOR IF NOT EXISTS mysql_test
TYPE 'mysql'
-- URL 'jdbc:mysql://nightly1.apache.org:3306/hive1'
URL 'jdbc:mysql://nightly7x-us-ja-1.nightly7x-us-ja.root.hwx.site:3306/hive1?useUnicode=true&amp;characterEncoding=UTF-8'
COMMENT 'test connector'
WITH DCPROPERTIES (
"hive.sql.dbcp.username"="hive1",
"hive.sql.dbcp.password"="hive1");

-- test data connector authorization feature
SET hive.security.temporary.authorization.for.data.connectors=true;
SET hive.security.authorization.enabled=true;

-- DESCRIBE fail: reading a connector definition is expected to carry the same
-- requirement as the create / alter / drop counterparts
DESCRIBE CONNECTOR EXTENDED mysql_test;
