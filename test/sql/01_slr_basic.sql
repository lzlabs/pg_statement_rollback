\set VERBOSITY default
\set ECHO all
LOAD 'pg_statement_rollback.so';
SET pg_statement_rollback.enabled = 1;
SET pg_statement_rollback.savepoint_name TO 'aze';

DROP SCHEMA IF EXISTS testrsl CASCADE;
CREATE SCHEMA testrsl;

SET search_path TO testrsl,public;

DROP TABLE IF EXISTS tbl_rsl;

\echo Setup table
BEGIN;
    DROP TABLE IF EXISTS tbl_rsl;
    CREATE TABLE tbl_rsl(id integer, val varchar(256));
COMMIT;

\echo Setup triggers
BEGIN;
DROP FUNCTION IF EXISTS trg_before_stmt();
DROP FUNCTION IF EXISTS trg_before_row();
DROP FUNCTION IF EXISTS trg_after_stmt();
DROP FUNCTION IF EXISTS trg_after_row();

CREATE FUNCTION trg_before_stmt() RETURNS TRIGGER AS $$
BEGIN
    raise notice 'trg_before_stmt';
    IF (TG_OP = 'DELETE') THEN
        RETURN old;
    ELSE
        RETURN new;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION trg_before_row() RETURNS TRIGGER AS $$
BEGIN
    raise notice 'trg_before_row';
    IF (TG_OP = 'DELETE') THEN
        RETURN old;
    ELSE
        RETURN new;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION trg_after_stmt() RETURNS TRIGGER AS $$
BEGIN
    raise notice 'trg_after_stmt';
    IF (TG_OP = 'DELETE') THEN
        RETURN old;
    ELSE
        RETURN new;
    END IF;
end;
$$ LANGUAGE plpgsql;

CREATE FUNCTION trg_after_row() RETURNS TRIGGER AS $$
BEGIN
    raise notice 'trg_after_row';
    IF (TG_OP = 'DELETE') THEN
        RETURN old;
    ELSE
        RETURN new;
    END IF;
end;
$$ LANGUAGE plpgsql;

ROLLBACK TO aze /* here ROLLBACK TO is a no-op */;

CREATE FUNCTION trg_after_row() RETURNS TRIGGER AS $$
BEGIN
    raise notice 'trg_after_row';
    RETURN new;
end;
$$ LANGUAGE plpgsql;

ROLLBACK TO aze /* here ROLLBACK TO resolves failed create */;
CREATE OR REPLACE FUNCTION trg_after_row() RETURNS TRIGGER AS $$
BEGIN
    raise notice 'trg_after_row';
    RETURN new;
end;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_before_upd_stmt BEFORE UPDATE ON tbl_rsl FOR EACH STATEMENT EXECUTE PROCEDURE trg_before_stmt();
CREATE TRIGGER trg_before_upd_row BEFORE UPDATE ON tbl_rsl FOR EACH ROW EXECUTE PROCEDURE trg_before_row();
CREATE TRIGGER trg_before_ins_stmt BEFORE INSERT ON tbl_rsl FOR EACH STATEMENT EXECUTE PROCEDURE trg_before_stmt();
CREATE TRIGGER trg_before_ins_row BEFORE INSERT ON tbl_rsl FOR EACH ROW EXECUTE PROCEDURE trg_before_row();
CREATE TRIGGER trg_before_del_stmt BEFORE DELETE ON tbl_rsl FOR EACH STATEMENT EXECUTE PROCEDURE trg_before_stmt();
CREATE TRIGGER trg_before_del_row BEFORE DELETE ON tbl_rsl FOR EACH ROW EXECUTE PROCEDURE trg_before_row();
CREATE TRIGGER trg_after_upd_stmt AFTER UPDATE ON tbl_rsl FOR EACH STATEMENT EXECUTE PROCEDURE trg_after_stmt();
CREATE TRIGGER trg_after_upd_row AFTER UPDATE ON tbl_rsl FOR EACH ROW EXECUTE PROCEDURE trg_after_row();
CREATE TRIGGER trg_after_ins_stmt AFTER INSERT ON tbl_rsl FOR EACH STATEMENT EXECUTE PROCEDURE trg_after_stmt();
CREATE TRIGGER trg_after_ins_row AFTER INSERT ON tbl_rsl FOR EACH ROW EXECUTE PROCEDURE trg_after_row();
CREATE TRIGGER trg_after_del_stmt AFTER DELETE ON tbl_rsl FOR EACH STATEMENT EXECUTE PROCEDURE trg_after_stmt();
CREATE TRIGGER trg_after_del_row AFTER DELETE ON tbl_rsl FOR EACH ROW EXECUTE PROCEDURE trg_after_row();
COMMIT;

\echo Test 1
BEGIN;
    INSERT INTO tbl_rsl SELECT 1, 'line 1';
    INSERT INTO tbl_rsl SELECT 'error', 'line 2';
    ROLLBACK TO aze;
    SELECT 1 as exp, COUNT(*) FROM tbl_rsl;
    SAVEPOINT s1;
    INSERT INTO tbl_rsl SELECT 2, 'line 2';
    SELECT 2 as exp, COUNT(*) FROM tbl_rsl;
    ROLLBACK TO s1;
    SAVEPOINT aze; /* client savepoint with same name as internal */;
    SELECT 1 as exp, COUNT(*) FROM tbl_rsl;
    INSERT INTO tbl_rsl SELECT 'error', 'line 2';
    ROLLBACK TO aze;
    SELECT * FROM tbl_rsl;
COMMIT;

\echo Test 2
BEGIN;
    INSERT INTO tbl_rsl SELECT 'err', 'line 2';
    ROLLBACK TO aze;
    INSERT INTO tbl_rsl SELECT 'err', 'line 2';
    ROLLBACK TO aze;
    INSERT INTO tbl_rsl SELECT 'err', 'line 2';
    ROLLBACK TO aze;
    INSERT INTO tbl_rsl SELECT 'err', 'line 2';
    ROLLBACK TO aze;
    INSERT INTO tbl_rsl SELECT 'err', 'line 2';
    ROLLBACK TO aze;
    INSERT INTO tbl_rsl SELECT 'err', 'line 2';
    ROLLBACK TO aze;
    INSERT INTO tbl_rsl SELECT 'err', 'line 2';
    ROLLBACK TO aze;
    INSERT INTO tbl_rsl SELECT 'err', 'line 2';
    ROLLBACK TO aze;
    INSERT INTO tbl_rsl SELECT 'err', 'line 2';
    ROLLBACK TO aze;
    INSERT INTO tbl_rsl SELECT 'err', 'line 2';
    ROLLBACK TO aze;
    INSERT INTO tbl_rsl SELECT 'err', 'line 2';
    ROLLBACK TO aze;
    INSERT INTO tbl_rsl SELECT 'err', 'line 2';
    ROLLBACK TO aze;
    INSERT INTO tbl_rsl SELECT 'err', 'line 2';
    ROLLBACK TO aze;
    INSERT INTO tbl_rsl SELECT 'err', 'line 2';
    ROLLBACK TO aze;
    INSERT INTO tbl_rsl SELECT 'err', 'line 2';
    ROLLBACK TO aze;
    INSERT INTO tbl_rsl SELECT 'err', 'line 2';
    ROLLBACK TO aze;
    INSERT INTO tbl_rsl SELECT 'err', 'line 2';
    ROLLBACK TO aze;
    INSERT INTO tbl_rsl SELECT 'err', 'line 2';
    ROLLBACK TO aze;
    INSERT INTO tbl_rsl SELECT 'err', 'line 2';
    ROLLBACK TO aze;
    INSERT INTO tbl_rsl SELECT 'err', 'line 2';
    ROLLBACK TO aze;
    INSERT INTO tbl_rsl SELECT 'err', 'line 2';
    ROLLBACK TO aze;
    INSERT INTO tbl_rsl SELECT i, 'line ' || i FROM generate_series(2, 10) i;
    UPDATE tbl_rsl SET id = id + 1;
    SELECT 10 as exp, COUNT(*) FROM tbl_rsl;
    SELECT * FROM tbl_rsl;
    DELETE FROM tbl_rsl;
    SELECT 0 as exp, COUNT(*) FROM tbl_rsl;
COMMIT;

\echo Test 3
BEGIN;
    SAVEPOINT s1;
    DO LANGUAGE plpgsql $$
        DECLARE i integer; j integer;
        BEGIN
            FOR i in SELECT * FROM generate_series(1, 3)
            LOOP
                FOR j IN SELECT * FROM generate_series(1, 3)
                LOOP
                    INSERT INTO tbl_rsl VALUES (i, j::varchar);

                    IF ((i+j) % 4 = 0) THEN
                        RAISE NOTICE 'UPDATE % %', i, j;
                        UPDATE tbl_rsl SET id = i*2, val = (j*3)::varchar WHERE id = i AND val = j::varchar;
                    END IF;
                    IF ((i+j) % 5 = 0) THEN
                        RAISE NOTICE 'DELETE % %', i, j;
                        DELETE FROM tbl_rsl WHERE id = i AND val = j::varchar;
                    END IF;
                END LOOP;
            END LOOP;
        END;
    $$;
    SELECT * FROM tbl_rsl;
    SELECT 7 as exp, COUNT(*) FROM tbl_rsl;
    ROLLBACK TO s1;
    SAVEPOINT aze; /* client savepoint with same name as internal */
    SELECT 0 as exp, COUNT(*) FROM tbl_rsl;
    SELECT ooops_error FROM tbl_rsl;
    ROLLBACK TO aze;
    SELECT 0 as exp, COUNT(*) FROM tbl_rsl;
COMMIT;

DROP SCHEMA testrsl CASCADE;
