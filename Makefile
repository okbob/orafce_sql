# $PostgreSQL: pgsql/contrib/plpgsql_check/Makefile

MODULE_big = dbms_sql
OBJS = dbms_sql.o
DATA = dbms_sql--1.0.sql
EXTENSION = dbms_sql

REGRESS = init dbms_sql

ifdef NO_PGXS
subdir = contrib/dbms_sql
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
else
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
endif

override CFLAGS += -Wextra

