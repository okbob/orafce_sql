do $$
declare
  c int;
  strval varchar;
  intval int;
  nrows int default 30;
begin
  c := dbms_sql.open_cursor();
  call dbms_sql.parse(c, 'select ''ahoj'' || i, i from generate_series(1, :nrows) g(i)');
  call dbms_sql.bind_variable(c, 'nrows', nrows);
  call dbms_sql.define_column(c, 1, strval);
  call dbms_sql.define_column(c, 2, intval);
  perform dbms_sql.execute(c);
  while dbms_sql.fetch_rows(c) > 0
  loop
    call dbms_sql.column_value(c, 1, strval);
    call dbms_sql.column_value(c, 2, intval);
    raise notice 'c1: %, c2: %', strval, intval;
  end loop;
  call dbms_sql.close_cursor(c);
end;
$$;

do $$
declare
  c int;
  strval varchar;
  intval int;
  nrows int default 30;
begin
  c := dbms_sql.open_cursor();
  call dbms_sql.parse(c, 'select ''ahoj'' || i, i from generate_series(1, :nrows) g(i)');
  call dbms_sql.bind_variable(c, 'nrows', nrows);
  call dbms_sql.define_column(c, 1, strval);
  call dbms_sql.define_column(c, 2, intval);
  perform dbms_sql.execute(c);
  while dbms_sql.fetch_rows(c) > 0
  loop
    strval := dbms_sql.column_value_f(c, 1, strval);
    intval := dbms_sql.column_value_f(c, 2, intval);
    raise notice 'c1: %, c2: %', strval, intval;
  end loop;
  call dbms_sql.close_cursor(c);
end;
$$;
