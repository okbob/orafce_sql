# DBMS_SQL

This is implementation of Oracle's API of package DBMS_SQL

It doesn't ensure full compatibility, but should to decrease a work necessary for
successful migration.

## Functionality

This extension implements subset of Oracle's dbms_sql interface. The goal of this extension
is not a compatibility with Oracle, it is designed to reduce some work related migration
Oracle's applications to Postgres. Some basic bulk DML functionality is supported:

    do $$
    declare
      c int;
      a int[];
      b varchar[];
      ca numeric[];
    begin
      c := dbms_sql.open_cursor();
      call dbms_sql.parse(c, 'insert into foo values(:a, :b, :c)');
      a := ARRAY[1, 2, 3, 4, 5];
      b := ARRAY['Ahoj', 'Nazdar', 'Bazar'];
      ca := ARRAY[3.14, 2.22, 3.8, 4];
    
      call dbms_sql.bind_array(c, 'a', a, 2, 3);
      call dbms_sql.bind_array(c, 'b', b, 3, 4);
      call dbms_sql.bind_array(c, 'c', ca);
      raise notice 'inserted rows %d', dbms_sql.execute(c);
    end;
    $$;


    do $$
    declare
      c int;
      a int[];
      b varchar[];
      ca numeric[];
    begin
      c := dbms_sql.open_cursor();
      call dbms_sql.parse(c, 'select i, ''Ahoj'' || i, i + 0.003 from generate_series(1, 35) g(i)');
      call dbms_sql.define_array(c, 1, a, 10, 1);
      call dbms_sql.define_array(c, 2, b, 10, 1);
      call dbms_sql.define_array(c, 3, ca, 10, 1);

      perform dbms_sql.execute(c);
      while dbms_sql.fetch_rows(c) > 0
      loop
        call dbms_sql.column_value(c, 1, a);
        call dbms_sql.column_value(c, 2, b);
        call dbms_sql.column_value(c, 3, ca);
        raise notice 'a = %', a;
        raise notice 'b = %', b;
        raise notice 'c = %', ca;
      end loop;
      call dbms_sql.close_cursor(c);
    end;
    $$;

## Dependency

When you plan to use dbms_sql extension together with Orafce, then you have to remove line
with `CREATE DOMAIN varchar2 AS text;` statement from install sql script.
