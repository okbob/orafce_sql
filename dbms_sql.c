#include "postgres.h"
#include "fmgr.h"

#include "catalog/pg_type_d.h"
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
} ColumnData;

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
	MemoryContext cursor_cxt;
	bool		assigned;
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
	TOKEN_OTHER,
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

PG_FUNCTION_INFO_V1(dbms_sql_debug_cursor);

void _PG_init(void);

void
_PG_init(void)
{
	memset(cursors, 0, sizeof(cursors));

	persist_cxt = AllocSetContextCreate(TopMemoryContext,
										"dbms_sql persist context",
										ALLOCSET_DEFAULT_SIZES);
}

/*
 * FUNCTION dbms_sql.open_cursor() RETURNS int
 */
Datum
dbms_sql_open_cursor(PG_FUNCTION_ARGS)
{
	int		i;

	/* find and initialize first free slot */
	for (i = 0; i < MAX_CURSORS; i++)
	{
		if (!cursors[i].assigned)
		{
			memset(&cursors[i], 0, sizeof(CursorData));

			cursors[i].cid = i;

			cursors[i].cursor_cxt = AllocSetContextCreate(persist_cxt,
														   "dbms_sql cursor context",
														   ALLOCSET_DEFAULT_SIZES);
			cursors[i].assigned = true;

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

/*
 * PROCEDURE dbms_sql.close_cursor(c int)
 */
Datum
dbms_sql_close_cursor(PG_FUNCTION_ARGS)
{
	CursorData	   *c;

	c = get_cursor(fcinfo, false);

	/* release all assigned memory */
	if (c->assigned)
		MemoryContextDelete(c->cursor_cxt);

	c->assigned = false;

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

		elog(NOTICE, "column definotion for position %d is %s",
					  col->position,
					  format_type_with_typemod(col->typoid, col->typmod));


	}

elog(NOTICE, "%d %d", TopTransactionContext, CurTransactionContext);

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
		elog(ERROR, "cursor has assigned parsed query already");

	query = text_to_cstring(PG_GETARG_TEXT_P(1));
	ptr = query;

	initStringInfo(&sinfo);

	while (ptr)
	{
		char	   *startsep;
		char	   *next_ptr;
		size_t		seplen;

		next_ptr = next_token(ptr, &start, &len, &typ, &startsep, &seplen);
		if (ptr)
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
			else
				appendStringInfo(&sinfo, "%.*s", (int) len, start);
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
		elog(WARNING, "bind variable is assigned already");

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
	if (category == TYPCATEGORY_STRING)
		col->typmod = colsize != -1 ? colsize + 4 : -1;
	else
		col->typmod = -1;

	get_typlenbyval(col->typoid, &col->typlen, &col->typbyval);

	return (Datum) 0;
}

/*
 * CREATE FUNCTION dbms_sql.execute(c int) RETURNS bigint;
 */
Datum
dbms_sql_execute(PG_FUNCTION_ARGS)
{
	CursorData *c;
	ColumnData *col;
	StringInfoData sinfo;

	c = get_cursor(fcinfo, true);


	/*
	 * When column definitions are available, build final query
	 * and open cursor for fetching.
	 */
	if (c->columns)
	{
		int		i;

		initStringInfo(&sinfo);

		appendStringInfo(&sinfo, "with __orafce_dbms_sql_cursor_%d as (%s) select ", c->cid, c->parsed_query);

		for (i = 1; i < c->max_colpos; i++)
		{
			ColumnData *col = get_col(c, i, false);

			if (i > 1)
				appendStringInfoString(&sinfo, ", ");

			

			
		}


	}

	PG_RETURN_INT64(0);
}


/******************************************************************
 * Simple parser
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
	else if (c >= 128 && c <= 255)
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
		return NULL;

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
				*typ = 6; *len = str - *start;
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
					*typ = 7; *len = str - *start;
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
					*typ = 8; *len = str - *start;
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
	*typ = TOKEN_OTHER;
	*start = str;
	return str + 1;
}
