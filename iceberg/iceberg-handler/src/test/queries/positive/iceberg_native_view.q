-- SORT_QUERY_RESULTS

CREATE DATABASE ice_native_view_db;
USE ice_native_view_db;

CREATE TABLE src_ice (id int, name string) stored by iceberg;
INSERT INTO src_ice VALUES (10, 'x'), (20, 'y');

CREATE VIEW v_ice AS SELECT * FROM src_ice stored by iceberg;

SELECT * FROM v_ice ORDER BY id, name;

DROP VIEW v_ice;
DROP TABLE src_ice;
USE default;
DROP DATABASE ice_native_view_db CASCADE;
