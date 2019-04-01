--! qt:dataset:src

set hive.fetch.task.conversion=none;

SELECT NOT (key <=> key) from src tablesample (1 rows);

CREATE TABLE IF NOT EXISTS t1(c0 DOUBLE PRIMARY KEY DISABLE NOVALIDATE, c1 integer );
CREATE VIEW v0 AS (SELECT DISTINCT t1.c1 AS cv0 FROM t1 WHERE (((t1.c0)<(t1.c1))) IS TRUE);
INSERT INTO t1 VALUES(0, 1);

SELECT cast((('F' LIKE '') OR (12 IS DISTINCT FROM v0.cv0)) AS int) AS count FROM v0;
