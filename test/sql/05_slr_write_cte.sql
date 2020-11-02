-- Test rollback at statement level with nested write statements from a function
LOAD 'pg_statement_rollback.so';
SET pg_statement_rollback.enabled TO on;
SET pg_statement_rollback.savepoint_name TO 'aze';
SET pg_statement_rollback.enable_writeonly TO on;

DROP SCHEMA IF EXISTS testrsl CASCADE;
CREATE SCHEMA testrsl;

SET search_path TO testrsl,public;

SET log_min_duration_statement TO -1;
SET log_statement TO 'all';
SET log_duration TO off;
SET client_min_messages TO LOG;

\echo Test write CTE and internal automatic savepoint 
BEGIN;
DROP TABLE IF EXISTS tbl_rsl;
CREATE TABLE tbl_rsl(id integer, val varchar(256));
INSERT INTO tbl_rsl VALUES (1, 'one');
WITH write AS (INSERT INTO tbl_rsl VALUES (2, 'two') RETURNING id, val) SELECT * FROM write;
UPDATE tbl_rsl SET id = 'two', val = 2 WHERE id = 1; -- will fail
ROLLBACK TO SAVEPOINT aze;
DELETE FROM tbl_rsl WHERE id = 1;
SELECT * FROM tbl_rsl; -- Should show records id 2
COMMIT;

DROP SCHEMA testrsl CASCADE;
