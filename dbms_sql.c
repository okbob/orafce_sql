#include "postgres.h"
#include "fmgr.h"

#include "lib/stringinfo.h"

#include "nodes/pg_list.h"

#include "parser/scansup.h"
#include "utils/builtins.h"
#include "utils/elog.h"

PG_MODULE_MAGIC;


PG_FUNCTION_INFO_V1(dbms_sql_parse);

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
next_token(char *str, char **start, size_t *len, int *typ, char **sep, size_t *seplen)
{
	if (*str == '\0')
		return NULL;

	/* reduce spaces */
	if (*str == ' ')
	{
		*start = str++;
		while (*str == ' ')
			str++;

		*typ = 1; *len = 1;
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
			*typ = 9; *len = 1;
			*start = str;
			return str + 1;
		}

		/* now it looks like correct $ separator */
		*start = aux; *sep = str; *seplen = aux - str; *typ = 10;

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
		*typ = 2; *len = str - *start;
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
		*typ = 3; *len = str - *start;
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
		*typ = 5; *len = str - *start;
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
		*typ = 6; *len = str - *start;
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
		*typ = 7; *len = str - *start;
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
		*typ = 8; *len = str - *start;
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
		*typ = 4; *len = str - *start;
		return str;
	}

	/* Others */
	*typ = 9;
	*start = str;
	return str + 1;
}


typedef struct
{
	char	   *name;
	Datum		value;
	bool		isnull;
	Oid			typ;
	int32		typmod;
	bool		valid;
	int			varno;
} Variable;


/*
 * PROCEDURE dbms_sql.parse(c int, stmt varchar)
 */
Datum
dbms_sql_parse(PG_FUNCTION_ARGS)
{
	char	   *src;
	char	   *start;
	size_t		len;
	int			typ;
	int			varno = 0;
	int			varnos = 0;
	List	   *variables = NIL;
	StringInfoData	sinfo;

	if (PG_ARGISNULL(1))
		elog(ERROR, "statement cannot be NULL");

	src = text_to_cstring(PG_GETARG_TEXT_P(1));

	initStringInfo(&sinfo);

	while (src)
	{
		char	   *startsep;
		size_t		seplen;

		src = next_token(src, &start, &len, &typ, &startsep, &seplen);
		if (src)
		{
			if (typ == 10)
			{
				appendStringInfo(&sinfo, "%.*s", (int) seplen, startsep);
				appendStringInfo(&sinfo, "%.*s", (int) len, start);
				appendStringInfo(&sinfo, "%.*s", (int) seplen, startsep);
			}
			else if (typ == 5)
			{
				char	   *name = downcase_identifier(start, len, false, true);
				ListCell   *lc;

				varno = -1;

				foreach(lc, variables)
				{
					Variable *var = (Variable *) lfirst(lc);

					if (strcmp(var->name, name) == 0)
					{
						varno = var->varno;
						break;
					}
				}

				if (varno == -1)
				{
					Variable *var = palloc(sizeof(Variable));

					var->name = pstrdup(name);
					var->varno = ++varnos;
					var->valid = false;
					varno = var->varno;

					variables = lappend(variables, var);
				}

				appendStringInfo(&sinfo, "$%d", varno);

				pfree(name);
			}
			else if (typ == 6)
			{
				appendStringInfo(&sinfo, "e\'%.*s\'", (int) len, start);
			}
			else if (typ == 7)
			{
				appendStringInfo(&sinfo, "\'%.*s\'", (int) len, start);
			}
			else if (typ == 8)
			{
				appendStringInfo(&sinfo, "\"%.*s\"", (int) len, start);
			}
			else
				appendStringInfo(&sinfo, "%.*s", (int) len, start);
		}
	}

	elog(NOTICE, "transformed: %s", sinfo.data);

	return (Datum) 0;
}
