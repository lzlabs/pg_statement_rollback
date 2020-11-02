\set VERBOSITY default
\set ECHO all
LOAD 'pg_statement_rollback.so';
SET pg_statement_rollback.enabled = 0;
SET pg_statement_rollback.savepoint_name TO 'aze';

DROP SCHEMA IF EXISTS testrsl CASCADE;
CREATE SCHEMA testrsl;

SET search_path TO testrsl,public;

\echo Setup table
BEGIN;
    DROP TABLE IF EXISTS tbl_rsl;
    CREATE TABLE tbl_rsl(id integer, val varchar(256));
    INSERT INTO tbl_rsl VALUES (1, 'one');
COMMIT;

\echo Check behavior when lzas.enabled is disabled
BEGIN;
DECLARE c CURSOR FOR SELECT id FROM tbl_rsl ORDER BY id;
SAVEPOINT one;
FETCH 1 FROM c;
ROLLBACK TO SAVEPOINT one;
FETCH 1 FROM c;
RELEASE SAVEPOINT one;
FETCH 1 FROM c;
CLOSE c;
DECLARE c CURSOR FOR SELECT id/0 FROM tbl_rsl ORDER BY id;
SAVEPOINT two;
FETCH 1 FROM c;
ROLLBACK TO SAVEPOINT two;
FETCH 1 FROM c;
ROLLBACK TO SAVEPOINT two;
RELEASE SAVEPOINT two;
FETCH 1 FROM c;
ROLLBACK;

DROP SCHEMA testrsl CASCADE;
