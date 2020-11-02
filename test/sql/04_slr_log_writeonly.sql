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

CREATE FUNCTION test_insert() RETURNS integer AS $$
BEGIN
    RAISE NOTICE 'Call an insert from a select to see behavior of automatic savepoint with nested statement.';
    INSERT INTO tbl_rsl VALUES (3, 'three');
    INSERT INTO tbl_rsl VALUES (4, 'four');
    RETURN 1;
END
$$ LANGUAGE plpgsql;

\echo Test log_statement to show internal automatic savepoint
BEGIN;
DROP TABLE IF EXISTS tbl_rsl;
CREATE TABLE tbl_rsl(id integer, val varchar(256));
INSERT INTO tbl_rsl VALUES (1, 'one');
SELECT * FROM tbl_rsl; -- No automatic savepoint on SELECT
UPDATE tbl_rsl SET id = 2, val = 'two' WHERE id = 1;
DELETE FROM tbl_rsl WHERE id = 2;
COMMIT;

\echo Test to show internal automatic savepoint and call to a modifying function
BEGIN;
DROP TABLE IF EXISTS tbl_rsl;
CREATE TABLE tbl_rsl(id integer, val varchar(256));
INSERT INTO tbl_rsl VALUES (1, 'one');
SELECT test_insert();
UPDATE tbl_rsl SET id = 2, val = 'two' WHERE id = 1;
DELETE FROM tbl_rsl WHERE id = 2;
SELECT * FROM tbl_rsl;
COMMIT;

\echo Test nested statement from function call and internal automatic savepoint
BEGIN;
DROP TABLE IF EXISTS tbl_rsl;
CREATE TABLE tbl_rsl(id integer, val varchar(256));
INSERT INTO tbl_rsl VALUES (1, 'one');
SELECT test_insert();
UPDATE tbl_rsl SET id = 'two', val = 2 WHERE id = 1; -- will fail
ROLLBACK TO SAVEPOINT aze;
DELETE FROM tbl_rsl WHERE id = 1;
SELECT * FROM tbl_rsl; -- Should show records id 3 and 4 from test_insert() call
COMMIT;

\echo Test nested statement from function call and internal automatic savepoint after ProcessUtility
BEGIN;
DROP TABLE IF EXISTS tbl_rsl;
CREATE TABLE tbl_rsl(id integer, val varchar(256));
INSERT INTO tbl_rsl VALUES (1, 'one');
SELECT test_insert();
CREATE TABLE tbl_rsl(id integer, val varchar(256)); -- will fail
ROLLBACK TO SAVEPOINT aze;
DELETE FROM tbl_rsl WHERE id = 1;
SELECT * FROM tbl_rsl; -- Should show records id 3 and 4 from test_insert() call
COMMIT;

\echo Test automatic savepoint after select when write only is off
SET pg_statement_rollback.enable_writeonly TO off;
BEGIN;
DROP TABLE IF EXISTS tbl_rsl;
CREATE TABLE tbl_rsl(id integer, val varchar(256));
INSERT INTO tbl_rsl VALUES (1, 'one');
SELECT * FROM tbl_rsl; -- Automatic savepoint on SELECT
UPDATE tbl_rsl SET id = 2, val = 'two' WHERE id = 1;
DELETE FROM tbl_rsl WHERE id = 2;
COMMIT;

DROP SCHEMA testrsl CASCADE;
