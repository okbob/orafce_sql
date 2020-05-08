/* dbms_sql.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION dbms_sql" to load this file. \quit
CREATE SCHEMA dbms_sql;

/*
 * temp solution, at the end varchar2 from orafce will be used
 */
CREATE DOMAIN varchar2 AS text;

/*
CREATE PROCEDURE dbms_sql.bind_variable(c int, name varchar2, value "any");
CREATE PROCEDURE dbms_sql.define_column(c int, pos int, value "any", int size default -1);
CREATE PROCEDURE dbms_sql.column_value(c int, pos int, INOUT value "any");

CREATE FUNCTION dbms_sql.execute(c int) RETURNS bigint;
CREATE FUNCTION dbms_sql.fetch_row(c int) RETURNS int;


*/

CREATE FUNCTION dbms_sql.open_cursor() RETURNS int AS 'MODULE_PATHNAME', 'dbms_sql_open_cursor' LANGUAGE C;
CREATE PROCEDURE dbms_sql.close_cursor(c int) AS 'MODULE_PATHNAME', 'dbms_sql_close_cursor' LANGUAGE C;
CREATE PROCEDURE dbms_sql.debug_cursor(c int) AS 'MODULE_PATHNAME', 'dbms_sql_debug_cursor' LANGUAGE C;
CREATE PROCEDURE dbms_sql.parse(c int, stmt varchar2) AS 'MODULE_PATHNAME', 'dbms_sql_parse' LANGUAGE C;
CREATE PROCEDURE dbms_sql.bind_variable(c int, name varchar2, value "any") AS 'MODULE_PATHNAME', 'dbms_sql_bind_variable' LANGUAGE C;
CREATE PROCEDURE dbms_sql.define_column(c int, col int, value "any", size int DEFAULT -1) AS 'MODULE_PATHNAME', 'dbms_sql_define_column' LANGUAGE C;
CREATE FUNCTION dbms_sql.execute(c int) RETURNS bigint AS 'MODULE_PATHNAME', 'dbms_sql_execute' LANGUAGE C;

COMMENT ON FUNCTION dbms_sql.open_cursor() IS 'returns first handler of dbms_sql''s cursor';
COMMENT ON PROCEDURE dbms_sql.parse(int, varchar2) IS 'prepare sql statement';
COMMENT ON PROCEDURE dbms_sql.bind_variable(int, varchar2, "any") IS 'assign a value to bind variable';


