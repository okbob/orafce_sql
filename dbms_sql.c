#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"

#include "access/tupconvert.h"
#include "catalog/pg_type_d.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "parser/parse_coerce.h"
#include "parser/scansup.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/elog.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"


PG_MODULE_MAGIC;

#define MAX_CURSORS			100

/*
 * bind variable data
 */
typedef struct
{
	char	   *refname;
	int			position;

	Datum		value;

	Oid			typoid;
	bool		typbyval;
	int16		typlen;

	bool		isnull;
	int			varno;		/* number of assigned placeholder of parsed query */
} VariableData;

/*
 * Query result column definition
 */
typedef struct
{
	int			position;

	Oid			typoid;
	bool		typbyval;
	int16		typlen;
	int32		typmod;
	bool		typisstr;
} ColumnData;

typedef struct
{
	bool		isvalid;			/* true, when this cast can be used */
	bool		without_cast;		/* true, when cast is not necessary */
	Oid		funcoid;
	Oid		funcoid_typmod;
	CoercionPathType path;
	CoercionPathType path_typmod;
	FmgrInfo	finfo;
	FmgrInfo	finfo_typmod;
	FmgrInfo	finfo_out;
	FmgrInfo	finfo_in;
	Oid			typIOParam;
	bool		check_domain;
} CastCacheData;

/*
 * dbms_sql cursor definition
 */
typedef struct
{
	int16		cid;
	char	   *parsed_query;
	char	   *original_query;
	int			nvariables;
	int			max_colpos;
	List	   *variables;
	List	   *columns;
	char		cursorname[32];
	Portal		portal;
	MemoryContext cursor_cxt;
	MemoryContext cursor_xact_cxt;
	MemoryContext tuples_cxt;
	HeapTuple	tuples[1000];
	TupleDesc	coltupdesc;
	TupleDesc	tupdesc;
	CastCacheData *casts;
	int			processed;
	int			nread;
	bool		assigned;
	bool		executed;
} CursorData;

typedef enum
{
	TOKEN_SPACES,
	TOKEN_COMMENT,
	TOKEN_NUMBER,
	TOKEN_BIND_VAR,
	TOKEN_STR,
	TOKEN_EXT_STR,
	TOKEN_DOLAR_STR,
	TOKEN_IDENTIF,
	TOKEN_QIDENTIF,
	TOKEN_DOUBLE_COLON,
	TOKEN_OTHER,
	TOKEN_NONE
} TokenType;

static MemoryContext	persist_cxt;
static CursorData		cursors[MAX_CURSORS];

static char *next_token(char *str, char **start, size_t *len, TokenType *typ, char **sep, size_t *seplen);

PG_FUNCTION_INFO_V1(dbms_sql_open_cursor);
PG_FUNCTION_INFO_V1(dbms_sql_close_cursor);
PG_FUNCTION_INFO_V1(dbms_sql_parse);
PG_FUNCTION_INFO_V1(dbms_sql_bind_variable);
PG_FUNCTION_INFO_V1(dbms_sql_define_column);
PG_FUNCTION_INFO_V1(dbms_sql_execute);
PG_FUNCTION_INFO_V1(dbms_sql_fetch_rows);
PG_FUNCTION_INFO_V1(dbms_sql_column_value);

PG_FUNCTION_INFO_V1(dbms_sql_debug_cursor);

void _PG_init(void);

void
_PG_init(void)
{
	memset(cursors, 0, sizeof(cursors));

	persist_cxt = AllocSetContextCreate(NULL,
										"dbms_sql persist context",
										ALLOCSET_DEFAULT_SIZES);
}

static void
open_cursor(CursorData *c, int cid)
{
	c->cid = cid;

	c->cursor_cxt = AllocSetContextCreate(persist_cxt,
														   "dbms_sql cursor context",
														   ALLOCSET_DEFAULT_SIZES);
	c->assigned = true;
}

/*
 * FUNCTION dbms_sql.open_cursor() RETURNS int
 */
Datum
dbms_sql_open_cursor(PG_FUNCTION_ARGS)
{
	int		i;

	(void) fcinfo;

	/* find and initialize first free slot */
	for (i = 0; i < MAX_CURSORS; i++)
	{
		if (!cursors[i].assigned)
		{
			open_cursor(&cursors[i], i);

			return i;
		}
	}

	elog(ERROR, "there is not free cursor");
}

static CursorData *
get_cursor(FunctionCallInfo fcinfo, bool should_be_assigned)
{
	CursorData	   *cursor;
	int				cid;

	if (PG_ARGISNULL(0))
		elog(ERROR, "cursor id cannot be NULL");

	cid = PG_GETARG_INT32(0);
	if (cid < 0 && cid >= MAX_CURSORS)
		elog(ERROR, "a value of cursor id is out of range");

	cursor = &cursors[cid];
	if (!cursor->assigned && should_be_assigned)
		elog(ERROR, "cursor is not opened");

	return cursor;
}

static void
close_cursor(CursorData *c)
{
	if (c->executed && c->portal)
		SPI_cursor_close(c->portal);

	/* release all assigned memory */
	if (c->cursor_cxt)
		MemoryContextDelete(c->cursor_cxt);

	if (c->cursor_xact_cxt)
		MemoryContextDelete(c->cursor_xact_cxt);

	memset(c, 0, sizeof(CursorData));
}

/*
 * PROCEDURE dbms_sql.close_cursor(c int)
 */
Datum
dbms_sql_close_cursor(PG_FUNCTION_ARGS)
{
	CursorData	   *c;

	c = get_cursor(fcinfo, false);

	close_cursor(c);

	return (Datum) 0;
}

/*
 * Print state of cursor - just for debug purposes
 */
Datum
dbms_sql_debug_cursor(PG_FUNCTION_ARGS)
{
	CursorData	   *c;
	ListCell	   *lc;

	c = get_cursor(fcinfo, false);

	if (c->assigned)
	{
		if (c->original_query)
			elog(NOTICE, "orig query: \"%s\"", c->original_query);

		if (c->parsed_query)
			elog(NOTICE, "parsed query: \"%s\"", c->parsed_query);

	}
	else
		elog(NOTICE, "cursor is not assigned");

	foreach(lc, c->variables)
	{
		VariableData *var = (VariableData *) lfirst(lc);

		if (var->typoid != InvalidOid)
		{
			Oid		typOutput;
			bool	isVarlena;
			char   *str;

			getTypeOutputInfo(var->typoid, &typOutput, &isVarlena);
			str = OidOutputFunctionCall(typOutput, var->value);

			elog(NOTICE, "variable \"%s\" is assigned to \"%s\"", var->refname, str);
		}
		else
			elog(NOTICE, "variable \"%s\" is not assigned", var->refname);
	}

	foreach(lc, c->columns)
	{
		ColumnData *col = (ColumnData *) lfirst(lc);

		elog(NOTICE, "column definition for position %d is %s",
					  col->position,
					  format_type_with_typemod(col->typoid, col->typmod));
	}

	return (Datum) 0;
}

/*
 * Search a variable in cursor's variable list
 */
static VariableData *
get_var(CursorData *c, char *refname, int position, bool append)
{
	ListCell	   *lc;
	VariableData   *nvar;
	MemoryContext	oldcxt;

	foreach(lc, c->variables)
	{
		VariableData *var = (VariableData *) lfirst(lc);

		if (strcmp(var->refname, refname) == 0)
			return var;
	}

	if (append)
	{
		oldcxt = MemoryContextSwitchTo(c->cursor_cxt);
		nvar = palloc0(sizeof(VariableData));

		nvar->refname = pstrdup(refname);
		nvar->varno = c->nvariables + 1;
		nvar->position = position;

		c->variables = lappend(c->variables, nvar);
		c->nvariables += 1;

		MemoryContextSwitchTo(oldcxt);

		return nvar;
	}
	else
		elog(ERROR, "bind variable \"%s\" not found", refname);
}

/*
 * PROCEDURE dbms_sql.parse(c int, stmt varchar)
 */
Datum
dbms_sql_parse(PG_FUNCTION_ARGS)
{
	char	   *query,
			   *ptr;
	char	   *start;
	size_t		len;
	TokenType	typ;
	StringInfoData	sinfo;
	CursorData *c;
	MemoryContext oldcxt;

	c = get_cursor(fcinfo, true);

	if (PG_ARGISNULL(1))
		elog(ERROR, "parsed query cannot be NULL");

	if (c->parsed_query)
	{
		int		cid = c->cid;

		close_cursor(c);
		open_cursor(c, cid);
	}

	query = text_to_cstring(PG_GETARG_TEXT_P(1));
	ptr = query;

	initStringInfo(&sinfo);

	while (ptr)
	{
		char	   *startsep;
		char	   *next_ptr;
		size_t		seplen;

		next_ptr = next_token(ptr, &start, &len, &typ, &startsep, &seplen);
		if (next_ptr)
		{
			if (typ == TOKEN_DOLAR_STR)
			{
				appendStringInfo(&sinfo, "%.*s", (int) seplen, startsep);
				appendStringInfo(&sinfo, "%.*s", (int) len, start);
				appendStringInfo(&sinfo, "%.*s", (int) seplen, startsep);
			}
			else if (typ == TOKEN_BIND_VAR)
			{
				char	   *name = downcase_identifier(start, len, false, true);
				VariableData *var = get_var(c, name, ptr - query, true);

				appendStringInfo(&sinfo, "$%d", var->varno);

				pfree(name);
			}
			else if (typ == TOKEN_EXT_STR)
			{
				appendStringInfo(&sinfo, "e\'%.*s\'", (int) len, start);
			}
			else if (typ == TOKEN_STR)
			{
				appendStringInfo(&sinfo, "\'%.*s\'", (int) len, start);
			}
			else if (typ == TOKEN_QIDENTIF)
			{
				appendStringInfo(&sinfo, "\"%.*s\"", (int) len, start);
			}
			else if (typ != TOKEN_NONE)
			{
				appendStringInfo(&sinfo, "%.*s", (int) len, start);
			}
		}

		ptr = next_ptr;
	}

	/* save result to persist context */
	oldcxt = MemoryContextSwitchTo(c->cursor_cxt);
	c->original_query = pstrdup(query);
	c->parsed_query = pstrdup(sinfo.data);

	MemoryContextSwitchTo(oldcxt);

	pfree(query);
	pfree(sinfo.data);

	return (Datum) 0;
}

/*
 * CREATE PROCEDURE dbms_sql.bind_variable(c int, name varchar2, value "any");
 */
Datum
dbms_sql_bind_variable(PG_FUNCTION_ARGS)
{
	CursorData *c;
	VariableData *var;
	char *varname, *varname_downcase;
	Oid			valtype;
	bool		is_unknown = false;

	c = get_cursor(fcinfo, true);

	if (PG_ARGISNULL(1))
		elog(ERROR, "name is NULL");

	varname = text_to_cstring(PG_GETARG_TEXT_P(1));
	if (*varname == ':')
		varname += 1;

	varname_downcase = downcase_identifier(varname, strlen(varname), false, true);
	var = get_var(c, varname_downcase, -1, false);

	valtype = get_fn_expr_argtype(fcinfo->flinfo, 2);
	if (valtype == RECORDOID)
		elog(ERROR, "cannot to assign a value of record type");

	valtype = getBaseType(valtype);
	if (valtype == UNKNOWNOID)
	{
		is_unknown = true;
		valtype = TEXTOID;
	}

	if (var->typoid != InvalidOid)
	{
		if (!var->typbyval)
			pfree(DatumGetPointer(var->value));

		var->isnull = true;
	}

	var->typoid = valtype;

	if (!PG_ARGISNULL(2))
	{
		MemoryContext	oldcxt;

		get_typlenbyval(var->typoid, &var->typlen, &var->typbyval);

		oldcxt = MemoryContextSwitchTo(c->cursor_cxt);

		if (is_unknown)
			var->value = CStringGetTextDatum(DatumGetPointer(PG_GETARG_DATUM(2)));
		else
			var->value = datumCopy(PG_GETARG_DATUM(2), var->typbyval, var->typlen);

		MemoryContextSwitchTo(oldcxt);
	}
	else
		var->isnull = true;

	return (Datum) 0;
}

static ColumnData *
get_col(CursorData *c, int position, bool append)
{
	ListCell	   *lc;
	ColumnData	   *ncol;
	MemoryContext	oldcxt;

	foreach(lc, c->columns)
	{
		ColumnData *col = (ColumnData *) lfirst(lc);

		if (col->position == position)
			return col;
	}

	if (append)
	{
		oldcxt = MemoryContextSwitchTo(c->cursor_cxt);
		ncol = palloc0(sizeof(ColumnData));

		ncol->position = position;
		if (c->max_colpos < position)
			c->max_colpos = position;

		c->columns = lappend(c->columns, ncol);

		MemoryContextSwitchTo(oldcxt);

		return ncol;
	}
	else
		elog(ERROR, "column definition on position \"%d\" not found", position);
}

/*
 * CREATE PROCEDURE dbms_sql.define_column(c int, col int, value "any", scale int DEFAULT -1);
 */
Datum
dbms_sql_define_column(PG_FUNCTION_ARGS)
{
	CursorData *c;
	ColumnData *col;
	Oid			valtype;
	int		position;
	int		colsize;
	TYPCATEGORY category;
	bool	ispreferred;

	c = get_cursor(fcinfo, true);

	if (PG_ARGISNULL(1))
		elog(ERROR, "position is NULL");

	position = PG_GETARG_INT32(1);
	col = get_col(c, position, true);

	valtype = get_fn_expr_argtype(fcinfo->flinfo, 2);
	if (valtype == RECORDOID)
		elog(ERROR, "cannot to define a column of record type");

	valtype = getBaseType(valtype);
	if (valtype == UNKNOWNOID)
		valtype = TEXTOID;

	if (col->typoid != InvalidOid)
		elog(WARNING, "column is defined already");

	col->typoid = valtype;

	if (PG_ARGISNULL(3))
		elog(ERROR, "size cannot be a NULL");

	colsize = PG_GETARG_INT32(3);

	get_type_category_preferred(col->typoid, &category, &ispreferred);
	col->typisstr = category == TYPCATEGORY_STRING;
	col->typmod = (col->typisstr && colsize != -1) ? colsize + 4 : -1;

	get_typlenbyval(col->typoid, &col->typlen, &col->typbyval);

	return (Datum) 0;
}

static void
cursor_xact_cxt_deletion_callback(void *arg)
{
	CursorData *cur = (CursorData *) arg;

	cur->cursor_xact_cxt = NULL;
	cur->tuples_cxt = NULL;

	cur->processed = 0;
	cur->nread = 0;
	cur->executed = false;
	cur->tupdesc = NULL;
	cur->coltupdesc = NULL;
	cur->casts = NULL;
}

/*
 * CREATE FUNCTION dbms_sql.execute(c int) RETURNS bigint;
 */
Datum
dbms_sql_execute(PG_FUNCTION_ARGS)
{
	CursorData *c;

	c = get_cursor(fcinfo, true);

	/* clean space with saved result */
	if (!c->cursor_xact_cxt)
	{
		MemoryContextCallback *mcb;
		MemoryContext oldcxt;

		c->cursor_xact_cxt = AllocSetContextCreate(TopTransactionContext,
															   "dbms_sql persist context",
															   ALLOCSET_DEFAULT_SIZES);

		oldcxt = MemoryContextSwitchTo(c->cursor_xact_cxt);
		mcb = palloc0(sizeof(MemoryContextCallback));

		mcb->func = cursor_xact_cxt_deletion_callback;
		mcb->arg = c;

		MemoryContextRegisterResetCallback(c->cursor_xact_cxt, mcb);

		MemoryContextSwitchTo(oldcxt);
	}
	else
	{
		MemoryContext	save_cxt = c->cursor_xact_cxt;

		MemoryContextReset(c->cursor_xact_cxt);
		c->cursor_xact_cxt = save_cxt;

		c->casts = NULL;
		c->tupdesc = NULL;
		c->tuples_cxt = NULL;
	}

	/*
	 * When column definitions are available, build final query
	 * and open cursor for fetching.
	 */
	if (c->columns)
	{
		Datum	   *values;
		Oid		   *types;
		char *nulls;
		ListCell   *lc;
		int		i;
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(c->cursor_xact_cxt);

		/* prepare query arguments */
		values = palloc(sizeof(Datum) * c->nvariables);
		types = palloc(sizeof(Oid) * c->nvariables);
		nulls = palloc(sizeof(char) * c->nvariables);

		i = 0;
		foreach(lc, c->variables)
		{
			VariableData *var = (VariableData *) lfirst(lc);

			if (!var->isnull)
			{
				/* copy a value to xact memory context, to be independent on a outside */
				values[i] = datumCopy(var->value, var->typbyval, var->typlen);
				nulls[i] = ' ';
			}
			else
				nulls[i] = 'n';

			if (var->typoid == InvalidOid)
				elog(ERROR, "variable \"%s\" has not bind a value", var->refname);

			types[i] = var->typoid;
		}

		/* prepare or refresh target tuple descriptor, used for final tupconversion */
		if (c->tupdesc)
			FreeTupleDesc(c->tupdesc);


		c->coltupdesc = CreateTemplateTupleDesc(c->max_colpos);

		/* prepare current result column tupdesc */
		for (i = 1; i <= c->max_colpos; i++)
		{
			ColumnData *col = get_col(c, i, false);
			char genname[32];

			snprintf(genname, 32, "col%d", i);
			TupleDescInitEntry(c->coltupdesc, (AttrNumber) i, genname, col->typoid, col->typmod, 0);
		}

		c->casts = palloc0(sizeof(CastCacheData) * c->coltupdesc->natts);

		MemoryContextSwitchTo(oldcxt);

		snprintf(c->cursorname, sizeof(c->cursorname), "__orafce_dbms_sql_cursor_%d", c->cid);

		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "SPI_connact failed");

		c->portal = SPI_cursor_open_with_args(c->cursorname,
											  c->parsed_query,
											  c->nvariables,
											  types,
											  values,
											  nulls,
											  false,
											  0);

		if (c->portal == NULL)
			elog(ERROR, "could not open cursor for query \"%s\": %s",
				  c->parsed_query, SPI_result_code_string(SPI_result));

		SPI_finish();

		/* Describe portal and prepare cast cache */
		if (c->portal->tupDesc)
		{
			int		natts = 0;
			TupleDesc tupdesc = c->portal->tupDesc;

			for (i = 0; i < tupdesc->natts; i++)
			{
				Form_pg_attribute att = TupleDescAttr(tupdesc, i);

				if (att->attisdropped)
					continue;

				natts += 1;
			}

			if (natts != c->coltupdesc->natts)
				elog(ERROR, "returned query has different number of columns than number of defined columns");
		}

		c->executed = true;
	}

	PG_RETURN_INT64(0);
}

/*
 * CREATE FUNCTION dbms_sql.fetch_rows(c int) RETURNS int;
 */
Datum
dbms_sql_fetch_rows(PG_FUNCTION_ARGS)
{
	CursorData *c;

	c = get_cursor(fcinfo, true);

	if (!c->executed)
		elog(ERROR, "cursor is not executed");

	if (!c->portal)
		elog(ERROR, "there is not a active portal");

	if (c->nread == c->processed)
	{
		MemoryContext	oldcxt;
		uint64		i;

		/* create or reset context for tuples */
		if (!c->tuples_cxt)
			c->tuples_cxt = AllocSetContextCreate(c->cursor_xact_cxt,
												  "dbms_sql tuples context",
		  										  ALLOCSET_DEFAULT_SIZES);
		else
			MemoryContextReset(c->tuples_cxt);

		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "SPI_connact failed");

		/* try to fetch data from cursor */
		SPI_cursor_fetch(c->portal, true, 10);

		if (SPI_tuptable == NULL)
			elog(ERROR, "cannot fetch data");

		oldcxt = MemoryContextSwitchTo(c->tuples_cxt);

		c->tupdesc = CreateTupleDescCopy(SPI_tuptable->tupdesc);

		for (i = 0; i < SPI_processed; i++)
			c->tuples[i] = heap_copytuple(SPI_tuptable->vals[i]);

		MemoryContextSwitchTo(oldcxt);

		c->processed = SPI_processed;
		c->nread = 0;

		SPI_finish();
	}

	if (c->nread <= c->processed)
		c->nread += 1;

	PG_RETURN_INT32(c->nread <= c->processed ? 1 : 0);
}

/*
 * Initialize cast case entry.
 */
static void
init_cast_cache_entry(CastCacheData *ccast,
					  Oid targettypid,
					  int32 targettypmod,
					  Oid sourcetypid)
{
	Oid		funcoid;
	Oid		basetypid;

	basetypid = getBaseType(targettypid);

	ccast->check_domain = basetypid != targettypid;

	if (sourcetypid == basetypid)
		ccast->without_cast = targettypmod == -1;
	else
		ccast->without_cast = false;

	if (!ccast->without_cast)
	{
		ccast->path = find_coercion_pathway(targettypid,
											sourcetypid,
											COERCION_ASSIGNMENT,
											&funcoid);

		if (ccast->path == COERCION_PATH_NONE)
			elog(ERROR, "cannot to find cast from source type to target type");

		if (ccast->path == COERCION_PATH_FUNC)
		{
			fmgr_info(funcoid, &ccast->finfo);
		}
		else if (ccast->path == COERCION_PATH_COERCEVIAIO)
		{
			bool	typisvarlena;

			getTypeOutputInfo(sourcetypid, &funcoid, &typisvarlena);
			fmgr_info(funcoid, &ccast->finfo_out);

			getTypeInputInfo(basetypid, &funcoid, &ccast->typIOParam);
			fmgr_info(funcoid, &ccast->finfo_in);
		}

		if (targettypmod != -1)
		{
			ccast->path_typmod = find_typmod_coercion_function(targettypid,
															   &funcoid);
			if (ccast->path_typmod == COERCION_PATH_FUNC)
				fmgr_info(funcoid, &ccast->finfo_typmod);
		}
	}

	ccast->isvalid = true;
}

/*
 * CREATE PROCEDURE dbms_sql.column_value(c int, pos int, INOUT value "any");
 */
Datum
dbms_sql_column_value(PG_FUNCTION_ARGS)
{
	Datum		value;
	Datum		result;
	CursorData *c;
	int			pos;
	int32		columnTypeMode;
	bool		isnull;
	Oid			targetTypeId;
	Oid			resultTypeId;
	Oid			columnTypeId;
	TupleDesc	resulttupdesc;
	HeapTuple	resulttuple;
	CastCacheData *ccast;
	Oid			typoid;

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connact failed");

	c = get_cursor(fcinfo, true);

	if (!c->executed)
		elog(ERROR, "cursor is not executed");

	if (!c->tupdesc)
		elog(ERROR, "cursor is not fetched");

	if (PG_ARGISNULL(1))
		elog(ERROR, "position is NULL");

	pos = PG_GETARG_INT32(1);

	if (!c->coltupdesc)
		elog(ERROR, "there are not defined columns");

	if (pos < 1 && pos > c->coltupdesc->natts)
		elog(ERROR, "position is out of [1, %d]", c->coltupdesc->natts);

	/*
	 * Setting of OUT field is little bit more complex, because although
	 * there is only one output field, the result should be compisite type.
	 */
	if (get_call_result_type(fcinfo, &resultTypeId, &resulttupdesc) == TYPEFUNC_COMPOSITE)
	{
		/* check target types */
		if (resulttupdesc->natts != 1)
			elog(ERROR, "unexpected number of result compisite fields");

		targetTypeId = get_fn_expr_argtype(fcinfo->flinfo, 2);
		Assert((TupleDescAttr(resulttupdesc, 0))->atttypid == targetTypeId);

		columnTypeId = (TupleDescAttr(c->coltupdesc, pos - 1))->atttypid;
		columnTypeMode = (TupleDescAttr(c->coltupdesc, pos - 1))->atttypmod;

		/* Maybe it can be solved by uncached slower cast */
		if (targetTypeId != columnTypeId)
			elog(ERROR, "internal: expected type and tupdesc are not consistent %d %d", targetTypeId, columnTypeId);
	}
	else
		elog(ERROR, "unexpected function result type");

	Assert(c->casts);

	value = SPI_getbinval(c->tuples[c->nread - 1], c->tupdesc, pos, &isnull);
	typoid = SPI_gettypeid(c->tupdesc, pos);

	ccast = &c->casts[pos - 1];

	if (!ccast->isvalid)
		init_cast_cache_entry(ccast,
							  columnTypeId,
							  columnTypeMode,
							  typoid);

	if (!ccast->without_cast)
	{
		if (!isnull)
		{
			if (ccast->path == COERCION_PATH_FUNC)
				value = FunctionCall1(&ccast->finfo, value);
			else if (ccast->path == COERCION_PATH_RELABELTYPE)
				value = value;
			else if (ccast->path == COERCION_PATH_COERCEVIAIO)
			{
				char *str;

				str = OutputFunctionCall(&ccast->finfo_out, value);
				value = InputFunctionCall(&ccast->finfo_in,
										  str,
										  ccast->typIOParam,
										  columnTypeMode);
			}
			else
				elog(ERROR, "unsupported cast yet %d", ccast->path);

			if (columnTypeMode != -1 && ccast->path_typmod == COERCION_PATH_FUNC)
				value = FunctionCall3(&ccast->finfo_typmod,
									  value,
									  Int32GetDatum(columnTypeMode),
									  BoolGetDatum(true));
		}
	}

	if (ccast->check_domain)
		domain_check(value, isnull, columnTypeId, NULL, NULL);

	resulttuple = heap_form_tuple(resulttupdesc, &value, &isnull);
	result = PointerGetDatum(SPI_returntuple(resulttuple, CreateTupleDescCopy(resulttupdesc)));

	SPI_finish();

	PG_RETURN_DATUM(result);
}


/******************************************************************
 * Simple parser - just for replacement of bind variables by
 * PostgreSQL $ param placeholders.
 *
 ******************************************************************
 */

/*
 * It doesn't work for multibyte encodings, but same implementation
 * is in Postgres too.
 */
static bool
is_identif(unsigned char c)
{
	if (c >= 'a' && c <= 'z')
		return true;
	else if (c >= 'A' && c <= 'Z')
		return true;
	else if (c >= 0200)
		return true;
	else
		return false;
}

/*
 * simple parser to detect :identif symbols in query
 */
static char *
next_token(char *str, char **start, size_t *len, TokenType *typ, char **sep, size_t *seplen)
{
	if (*str == '\0')
	{
		*typ = TOKEN_NONE;
		return NULL;
	}

	/* reduce spaces */
	if (*str == ' ')
	{
		*start = str++;
		while (*str == ' ')
			str++;

		*typ = TOKEN_SPACES; *len = 1;
		return str;
	}

	/* Postgres's dolar strings */
	if (*str == '$' && (str[1] == '$' || is_identif(str[1]) || str[1] == '_'))
	{
		char	   *aux = str + 1;
		char	   *endstr;
		bool		is_valid = false;
		char	   *buffer;

		/* try to find end of separator */
		while (*aux)
		{
			if (*aux == '$')
			{
				is_valid = true;
				aux++;
				break;
			}
			else if (is_identif(*aux) ||
					 isdigit(*aux) ||
					 *aux == '_')
			{
				aux++;
			}
			else
				break;
		}

		if (!is_valid)
		{
			*typ = TOKEN_OTHER; *len = 1;
			*start = str;
			return str + 1;
		}

		/* now it looks like correct $ separator */
		*start = aux; *sep = str; *seplen = aux - str; *typ = TOKEN_DOLAR_STR;

		/* try to find second instance */
		buffer = palloc(*seplen + 1);
		strncpy(buffer, *sep, *seplen);
		buffer[*seplen] = '\0';

		endstr = strstr(aux, buffer);
		if (endstr)
		{
			*len = endstr - *start;
			return endstr + *seplen;
		}
		else
		{
			while (*aux)
				aux++;
			*len = aux - *start;
			return aux;
		}

		return aux;
	}

	/* Pair comments */
	if (*str == '/' && str[1] == '*')
	{
		*start = str; str += 2;
		while (*str)
		{
			if (*str == '*' && str[1] == '/')
			{
				str += 2;
				break;
			}
			str++;
		}
		*typ = TOKEN_COMMENT; *len = str - *start;
		return str;
	}

	/* Number */
	if (isdigit(*str) || (*str == '.' && isdigit(str[1])))
	{
		bool	point = *str == '.';

		*start = str++;
		while (*str)
		{
			if (isdigit(*str))
				str++;
			else if (*str == '.' && !point)
			{
				str++; point = true;
			}
			else
				break;
		}
		*typ = TOKEN_NUMBER; *len = str - *start;
		return str;
	}

	/* Double colon :: */
	if (*str == ':' && str[1] == ':')
	{
		*start = str; *typ = TOKEN_DOUBLE_COLON; *len = 2;
		return str + 2;
	}

	/* Bind variable placeholder */
	if (*str == ':' &&
		(is_identif(str[1]) || str[1] == '_'))
	{
		*start = &str[1]; str += 2;
		while (*str)
		{
			if (is_identif(*str) ||
				isdigit(*str) ||
				*str == '_')
				str++;
			else
				break;
		}
		*typ = TOKEN_BIND_VAR; *len = str - *start;
		return str;
	}

	/* Extended string literal */
	if ((*str == 'e' || *str == 'E') && str[1] == '\'')
	{
		*start = &str[2]; str += 2;
		while (*str)
		{
			if (*str == '\'')
			{
				*typ = TOKEN_EXT_STR; *len = str - *start;
				return str + 1;
			}
			if (*str == '\\' && str[1] == '\'')
				str += 2;
			else if (*str == '\\' && str[1] == '\\')
				str += 2;
			else
				str += 1;
		}

		*typ = TOKEN_EXT_STR; *len = str - *start;
		return str;
	}

	/* String literal */
	if (*str == '\'')
	{
		*start = &str[1]; str += 1;
		while (*str)
		{
			if (*str == '\'')
			{
				if (str[1] != '\'')
				{
					*typ = TOKEN_STR; *len = str - *start;
					return str + 1;
				}
				str += 2;
			}
			else
				str += 1;
		}
		*typ = TOKEN_STR; *len = str - *start;
		return str;
	}

	/* Quoted identifier */
	if (*str == '"')
	{
		*start = &str[1]; str += 1;
		while (*str)
		{
			if (*str == '"')
			{
				if (str[1] != '"')
				{
					*typ = TOKEN_QIDENTIF; *len = str - *start;
					return str + 1;
				}
				str += 2;
			}
			else
				str += 1;
		}
		*typ = TOKEN_QIDENTIF; *len = str - *start;
		return str;
	}

	/* Identifiers */
	if (is_identif(*str) || *str == '_')
	{
		*start = str++;
		while (*str)
		{
			if (is_identif(*str) ||
				isdigit(*str) ||
				*str == '_')
				str++;
			else
				break;
		}
		*typ = TOKEN_IDENTIF; *len = str - *start;
		return str;
	}

	/* Others */
	*typ = TOKEN_OTHER; *start = str; *len = 1;
	return str + 1;
}
