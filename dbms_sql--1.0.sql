/* dbms_sql.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION dbms_sql" to load this file. \quit
CREATE SCHEMA dbms_sql;

/*
 * temp solution, at the end varchar2 from orafce will be used
 */
CREATE DOMAIN varchar2 AS text;

CREATE FUNCTION dbms_sql.open_cursor() RETURNS int AS 'MODULE_PATHNAME', 'dbms_sql_open_cursor' LANGUAGE c;
CREATE PROCEDURE dbms_sql.close_cursor(c int) AS 'MODULE_PATHNAME', 'dbms_sql_close_cursor' LANGUAGE c;
CREATE PROCEDURE dbms_sql.debug_cursor(c int) AS 'MODULE_PATHNAME', 'dbms_sql_debug_cursor' LANGUAGE c;
CREATE PROCEDURE dbms_sql.parse(c int, stmt varchar2) AS 'MODULE_PATHNAME', 'dbms_sql_parse' LANGUAGE c;
CREATE PROCEDURE dbms_sql.bind_variable(c int, name varchar2, value "any") AS 'MODULE_PATHNAME', 'dbms_sql_bind_variable' LANGUAGE c;
CREATE FUNCTION dbms_sql.bind_variable_f(c int, name varchar2, value "any") RETURNS void AS 'MODULE_PATHNAME', 'dbms_sql_bind_variable_f' LANGUAGE c;
CREATE PROCEDURE dbms_sql.bind_array(c int, name varchar2, value anyarray) AS 'MODULE_PATHNAME', 'dbms_sql_bind_array_3' LANGUAGE c;
CREATE PROCEDURE dbms_sql.define_column(c int, col int, value "any", size int DEFAULT -1) AS 'MODULE_PATHNAME', 'dbms_sql_define_column' LANGUAGE c;
CREATE FUNCTION dbms_sql.execute(c int) RETURNS bigint AS 'MODULE_PATHNAME', 'dbms_sql_execute' LANGUAGE c;
CREATE FUNCTION dbms_sql.fetch_rows(c int) RETURNS int AS 'MODULE_PATHNAME', 'dbms_sql_fetch_rows' LANGUAGE c;
CREATE PROCEDURE dbms_sql.column_value(c int, pos int, INOUT value anyelement) AS 'MODULE_PATHNAME', 'dbms_sql_column_value' LANGUAGE c;
CREATE FUNCTION dbms_sql.column_value_f(c int, pos int, value anyelement) RETURNS anyelement AS 'MODULE_PATHNAME', 'dbms_sql_column_value_f' LANGUAGE c;