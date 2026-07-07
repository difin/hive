-- Create test table with unknown placeholder column
CREATE EXTERNAL TABLE unknown_test_basic (
    id INT,
    placeholder UNKNOWN
) STORED BY ICEBERG tblproperties('format-version'='3');

-- Unknown columns are not stored; only NULL values are accepted
INSERT INTO unknown_test_basic VALUES
(1, NULL),
(2, NULL);

SELECT id, placeholder FROM unknown_test_basic ORDER BY id;

-- Add another unknown column to an existing table
ALTER TABLE unknown_test_basic ADD COLUMNS (extra UNKNOWN);

INSERT INTO unknown_test_basic VALUES
(3, NULL, NULL);

SELECT id, placeholder, extra FROM unknown_test_basic ORDER BY id;

DESC FORMATTED unknown_test_basic;
