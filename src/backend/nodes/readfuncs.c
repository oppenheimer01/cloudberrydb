/*-------------------------------------------------------------------------
 *
 * readfuncs.c
 *	  Reader functions for Postgres tree nodes.
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/nodes/readfuncs.c
 *
 * NOTES
 *	  Path nodes do not have any readfuncs support, because we never
 *	  have occasion to read them in.  (There was once code here that
 *	  claimed to read them, but it was broken as well as unused.)  We
 *	  never read executor state trees, either.
 *
 *    But due to the use of this routine in older version of CDB/MPP/GPDB,
 *    there are routines that do read those types of nodes (unlike PostgreSQL)
 *    Those routines never actually get called.
 *
 *    We could go back and remove them, but they don't hurt anything.
 *
 *    The purpose of these routines is to read serialized trees that were stored
 *    in the catalog, and reconstruct the trees.
 *
 *	  Parse location fields are written out by outfuncs.c, but only for
 *	  debugging use.  When reading a location field, we normally discard
 *	  the stored value and set the location field to -1 (ie, "unknown").
 *	  This is because nodes coming from a stored rule should not be thought
 *	  to have a known location in the current query's text.
 *	  However, if restore_location_fields is true, we do restore location
 *	  fields from the string.  This is currently intended only for use by the
 *	  WRITE_READ_PARSE_PLAN_TREES test code, which doesn't want to cause
 *	  any change in the node contents.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/extensible.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/readfuncs.h"
#include "utils/builtins.h"

#include "cdb/cdbgang.h"
#include "nodes/altertablenodes.h"

/*
 * readfuncs.c implements text-mode node deserialization.
 * Shared read functions are in readfuncs.funcs.c (included below).
 * The binary reader (readfast.c) includes readfuncs.funcs.c directly
 * with its own binary READ_* macros defined.
 */
#ifndef COMPILING_BINARY_FUNCS

/*
 * Macros to simplify reading of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.  Note that these
 * hard-wire conventions about the names of the local variables in a Read
 * routine.
 */

/* Macros for declaring appropriate local variables */

/* A few guys need only local_node */
#define READ_LOCALS_NO_FIELDS(nodeTypeName) \
	nodeTypeName *local_node = makeNode(nodeTypeName)

/* And a few guys need only the pg_strtok support fields */
#define READ_TEMP_LOCALS()	\
	const char *token;		\
	int			length

/* ... but most need both */
#define READ_LOCALS(nodeTypeName)			\
	READ_LOCALS_NO_FIELDS(nodeTypeName);	\
	READ_TEMP_LOCALS()

/* Read an integer field (anything written as ":fldname %d") */
#define READ_INT_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atoi(token)

/* Read an unsigned integer field (anything written as ":fldname %u") */
#define READ_UINT_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atoui(token)

/* Read an unsigned integer field (anything written using UINT64_FORMAT) */
#define READ_UINT64_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = strtou64(token, NULL, 10)

/* Read a long integer field (anything written as ":fldname %ld") */
#define READ_LONG_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atol(token)

/* Read an OID field (don't hard-wire assumption that OID is same as uint) */
#define READ_OID_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atooid(token)

/* Read a char field (ie, one ascii character) */
#define READ_CHAR_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	/* avoid overhead of calling debackslash() for one char */ \
	local_node->fldname = (length == 0) ? '\0' : (token[0] == '\\' ? token[1] : token[0])

/* Read an enumerated-type field that was written as an integer code */
#define READ_ENUM_FIELD(fldname, enumtype) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = (enumtype) atoi(token)

/* Read a float field */
#define READ_FLOAT_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atof(token)

/* Read a boolean field */
#define READ_BOOL_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = strtobool(token)

/* Read a character-string field */
#define READ_STRING_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = nullable_string(token, length)

/* Read a parse location field (and possibly throw away the value) */
#ifdef WRITE_READ_PARSE_PLAN_TREES
#define READ_LOCATION_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = restore_location_fields ? atoi(token) : -1
#else
#define READ_LOCATION_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	(void) token;				/* in case not used elsewhere */ \
	local_node->fldname = -1	/* set field to "unknown" */
#endif

/* Read a Node field */
#define READ_NODE_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	(void) token;				/* in case not used elsewhere */ \
	local_node->fldname = nodeRead(NULL, 0)

/* Read a bytea field */
#define READ_BYTEA_FIELD(fldname) \
	local_node->fldname = (bytea *) DatumGetPointer(readDatum(false))

/* Read a bitmapset field */
#define READ_BITMAPSET_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	(void) token;				/* in case not used elsewhere */ \
	local_node->fldname = _readBitmapset()

/* Read an attribute number array */
#define READ_ATTRNUMBER_ARRAY(fldname, len) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	local_node->fldname = readAttrNumberCols(len)

/* Read an oid array */
#define READ_OID_ARRAY(fldname, len) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	local_node->fldname = readOidCols(len)

/* Read an int array */
#define READ_INT_ARRAY(fldname, len) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	local_node->fldname = readIntCols(len)

/* Read a bool array */
#define READ_BOOL_ARRAY(fldname, len) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	local_node->fldname = readBoolCols(len)

/* Routine exit */
#define READ_DONE() \
	return local_node


/*
 * NOTE: use atoi() to read values written with %d, or atoui() to read
 * values written with %u in outfuncs.c.  An exception is OID values,
 * for which use atooid().  (As of 7.1, outfuncs.c writes OIDs as %u,
 * but this will probably change in the future.)
 */
#define atoui(x)  ((unsigned int) strtoul((x), NULL, 10))

#define strtobool(x)  ((*(x) == 't') ? true : false)

#define nullable_string(token,length)  \
	((length) == 0 ? NULL : debackslash(token, length))


#endif /* COMPILING_BINARY_FUNCS */

#ifndef COMPILING_BINARY_FUNCS
/*
 * _readBitmapset
 */
static Bitmapset *
_readBitmapset(void)
{
	Bitmapset  *result = NULL;

	READ_TEMP_LOCALS();

	token = pg_strtok(&length);
	if (token == NULL)
		elog(ERROR, "incomplete Bitmapset structure");
	if (length != 1 || token[0] != '(')
		elog(ERROR, "unrecognized token: \"%.*s\"", length, token);

	token = pg_strtok(&length);
	if (token == NULL)
		elog(ERROR, "incomplete Bitmapset structure");
	if (length != 1 || token[0] != 'b')
		elog(ERROR, "unrecognized token: \"%.*s\"", length, token);

	for (;;)
	{
		int			val;
		char	   *endptr;

		token = pg_strtok(&length);
		if (token == NULL)
			elog(ERROR, "unterminated Bitmapset structure");
		if (length == 1 && token[0] == ')')
			break;
		val = (int) strtol(token, &endptr, 10);
		if (endptr != token + length)
			elog(ERROR, "unrecognized integer: \"%.*s\"", length, token);
		result = bms_add_member(result, val);
	}

	return result;
}

/*
 * for use by extensions which define extensible nodes
 */
Bitmapset *
readBitmapset(void)
{
	return _readBitmapset();
}

/*
 * _readQuery
 */
static Query *
_readQuery(void)
{
	READ_LOCALS(Query);

	READ_ENUM_FIELD(commandType, CmdType);
	READ_ENUM_FIELD(querySource, QuerySource);
	local_node->queryId = UINT64CONST(0);	/* not saved in output format */
	READ_BOOL_FIELD(canSetTag);
	READ_NODE_FIELD(utilityStmt);
	READ_INT_FIELD(resultRelation);
	READ_BOOL_FIELD(hasAggs);
	READ_BOOL_FIELD(hasWindowFuncs);
	READ_BOOL_FIELD(hasTargetSRFs);
	READ_BOOL_FIELD(hasSubLinks);
	READ_BOOL_FIELD(hasDynamicFunctions);
	READ_BOOL_FIELD(hasFuncsWithExecRestrictions);
	READ_BOOL_FIELD(hasDistinctOn);
	READ_BOOL_FIELD(hasRecursive);
	READ_BOOL_FIELD(hasModifyingCTE);
	READ_BOOL_FIELD(hasForUpdate);
	READ_BOOL_FIELD(hasRowSecurity);
	READ_BOOL_FIELD(canOptSelectLockingClause);
	READ_BOOL_FIELD(isReturn);
	READ_NODE_FIELD(cteList);
	READ_NODE_FIELD(rtable);
	READ_NODE_FIELD(rteperminfos);
	READ_NODE_FIELD(jointree);
	READ_NODE_FIELD(mergeActionList);
	READ_BOOL_FIELD(mergeUseOuterJoin);
	READ_NODE_FIELD(targetList);
	READ_ENUM_FIELD(override, OverridingKind);
	READ_NODE_FIELD(onConflict);
	READ_NODE_FIELD(returningList);
	READ_NODE_FIELD(groupClause);
	READ_BOOL_FIELD(groupDistinct);
	READ_NODE_FIELD(groupingSets);
	READ_NODE_FIELD(havingQual);
	READ_NODE_FIELD(windowClause);
	READ_NODE_FIELD(distinctClause);
	READ_NODE_FIELD(sortClause);
	READ_NODE_FIELD(scatterClause);
	READ_BOOL_FIELD(isTableValueSelect);
	READ_NODE_FIELD(limitOffset);
	READ_NODE_FIELD(limitCount);
	READ_ENUM_FIELD(limitOption, LimitOption);
	READ_NODE_FIELD(rowMarks);
	READ_NODE_FIELD(setOperations);
	READ_NODE_FIELD(constraintDeps);
	READ_NODE_FIELD(withCheckOptions);
    local_node->intoPolicy = NULL;
    READ_BOOL_FIELD(parentStmtType);
	READ_LOCATION_FIELD(stmt_location);
	READ_INT_FIELD(stmt_len);

	READ_DONE();
}
#endif /* COMPILING_BINARY_FUNCS */
















#ifndef COMPILING_BINARY_FUNCS
/*
 * _readConst
 */
static Const *
_readConst(void)
{
	READ_LOCALS(Const);

	READ_OID_FIELD(consttype);
	READ_INT_FIELD(consttypmod);
	READ_OID_FIELD(constcollid);
	READ_INT_FIELD(constlen);
	READ_BOOL_FIELD(constbyval);
	READ_BOOL_FIELD(constisnull);
	READ_LOCATION_FIELD(location);

	token = pg_strtok(&length); /* skip :constvalue */
	if (local_node->constisnull)
		token = pg_strtok(&length); /* skip "<>" */
	else
		local_node->constvalue = readDatum(local_node->constbyval);

	READ_DONE();
}
#endif /* COMPILING_BINARY_FUNCS */








#ifndef COMPILING_BINARY_FUNCS
/*
 * _readOpExpr
 */
static OpExpr *
_readOpExpr(void)
{
	READ_LOCALS(OpExpr);

	READ_OID_FIELD(opno);
	READ_OID_FIELD(opfuncid);
	READ_OID_FIELD(opresulttype);
	READ_BOOL_FIELD(opretset);
	READ_OID_FIELD(opcollid);
	READ_OID_FIELD(inputcollid);
	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}
#endif /* COMPILING_BINARY_FUNCS */




#ifndef COMPILING_BINARY_FUNCS
/*
 * _readBoolExpr
 */
static BoolExpr *
_readBoolExpr(void)
{
	READ_LOCALS(BoolExpr);

	/* do-it-yourself enum representation */
	token = pg_strtok(&length); /* skip :boolop */
	token = pg_strtok(&length); /* get field value */
	if (strncmp(token, "and", 3) == 0)
		local_node->boolop = AND_EXPR;
	else if (strncmp(token, "or", 2) == 0)
		local_node->boolop = OR_EXPR;
	else if (strncmp(token, "not", 3) == 0)
		local_node->boolop = NOT_EXPR;
	else
		elog(ERROR, "unrecognized boolop \"%.*s\"", length, token);

	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}
#endif /* COMPILING_BINARY_FUNCS */



#ifndef COMPILING_BINARY_FUNCS



static A_Const *
_readAConst(void)
{
	READ_LOCALS(A_Const);

	/* We expect either NULL or :val here */
	token = pg_strtok(&length);
	if (length == 4 && strncmp(token, "NULL", 4) == 0)
		local_node->isnull = true;
	else
	{
		union ValUnion *tmp = nodeRead(NULL, 0);

		/* To forestall valgrind complaints, copy only the valid data */
		switch (nodeTag(tmp))
		{
			case T_Integer:
				memcpy(&local_node->val, tmp, sizeof(Integer));
				break;
			case T_Float:
				memcpy(&local_node->val, tmp, sizeof(Float));
				break;
			case T_Boolean:
				memcpy(&local_node->val, tmp, sizeof(Boolean));
				break;
			case T_String:
				memcpy(&local_node->val, tmp, sizeof(String));
				break;
			case T_BitString:
				memcpy(&local_node->val, tmp, sizeof(BitString));
				break;
			default:
				elog(ERROR, "unrecognized node type: %d",
					 (int) nodeTag(tmp));
				break;
		}
	}

	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static A_Expr *
_readAExpr(void)
{
	READ_LOCALS(A_Expr);

	token = pg_strtok(&length);

	if (strncmp(token,"OPER",length)==0)
	{
		local_node->kind = AEXPR_OP;
		READ_NODE_FIELD(name);
	}
	else if (strncmp(token,"ANY",length)==0)
	{
		local_node->kind = AEXPR_OP_ANY;
		READ_NODE_FIELD(name);
	}
	else if (strncmp(token,"ALL",length)==0)
	{
		local_node->kind = AEXPR_OP_ALL;
		READ_NODE_FIELD(name);
	}
	else if (strncmp(token,"DISTINCT",length)==0)
	{
		local_node->kind = AEXPR_DISTINCT;
		READ_NODE_FIELD(name);
	}
	else if (strncmp(token,"NOT_DISTINCT",length)==0)
	{
		local_node->kind = AEXPR_NOT_DISTINCT;
		READ_NODE_FIELD(name);
	}
	else if (strncmp(token,"NULLIF",length)==0)
	{
		local_node->kind = AEXPR_NULLIF;
		READ_NODE_FIELD(name);
	}
	else if (strncmp(token,"IN",length)==0)
	{
		local_node->kind = AEXPR_IN;
		READ_NODE_FIELD(name);
	}
	else if (strncmp(token,"LIKE",length)==0)
	{
		local_node->kind = AEXPR_LIKE;
		READ_NODE_FIELD(name);
	}
	else if (strncmp(token,"ILIKE",length)==0)
	{
		local_node->kind = AEXPR_ILIKE;
		READ_NODE_FIELD(name);
	}
	else if (strncmp(token,"SIMILAR",length)==0)
	{
		local_node->kind = AEXPR_SIMILAR;
		READ_NODE_FIELD(name);
	}
	else if (strncmp(token,"BETWEEN",length)==0)
	{
		local_node->kind = AEXPR_BETWEEN;
		READ_NODE_FIELD(name);
	}
	else if (strncmp(token,"NOT_BETWEEN",length)==0)
	{
		local_node->kind = AEXPR_NOT_BETWEEN;
		READ_NODE_FIELD(name);
	}
	else if (strncmp(token,"BETWEEN_SYM",length)==0)
	{
		local_node->kind = AEXPR_BETWEEN_SYM;
		READ_NODE_FIELD(name);
	}
	else if (strncmp(token,"NOT_BETWEEN_SYM",length)==0)
	{
		local_node->kind = AEXPR_NOT_BETWEEN_SYM;
		READ_NODE_FIELD(name);
	}
	else
	{
		elog(ERROR,"Unable to understand A_Expr node %.30s",token);
	}

	READ_NODE_FIELD(lexpr);
	READ_NODE_FIELD(rexpr);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

#endif

#include "readfuncs.funcs.c"
#ifndef COMPILING_BINARY_FUNCS
/*
 * parseNodeString
 *
 * Given a character string representing a node tree, parseNodeString creates
 * the internal node structure.
 *
 * The string to be read must already have been loaded into pg_strtok().
 */
Node *
parseNodeString(void)
{
	void	   *return_value;

	READ_TEMP_LOCALS();

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	token = pg_strtok(&length);

#define MATCH(tokname, namelen) \
	(length == namelen && memcmp(token, tokname, namelen) == 0)

	/*
	 * Same as MATCH, but we make our life a bit easier by relying on the
	 * compiler to be smart, and evaluate the strlen("<constant>") at
	 * compilation time for us.
	 */
#define MATCHX(tokname) \
	(length == strlen(tokname) && strncmp(token, tokname, strlen(tokname)) == 0)

	if (MATCH("QUERY", 5))
		return_value = _readQuery();
	else if (MATCH("WITHCHECKOPTION", 15))
		return_value = _readWithCheckOption();
	else if (MATCH("SORTGROUPCLAUSE", 15))
		return_value = _readSortGroupClause();
	else if (MATCH("GROUPINGSET", 11))
		return_value = _readGroupingSet();
	else if (MATCH("WINDOWCLAUSE", 12))
		return_value = _readWindowClause();
	else if (MATCH("ROWMARKCLAUSE", 13))
		return_value = _readRowMarkClause();
	else if (MATCH("CTESEARCHCLAUSE", 15))
		return_value = _readCTESearchClause();
	else if (MATCH("CTECYCLECLAUSE", 14))
		return_value = _readCTECycleClause();
	else if (MATCH("COMMONTABLEEXPR", 15))
		return_value = _readCommonTableExpr();
	else if (MATCH("SETOPERATIONSTMT", 16))
		return_value = _readSetOperationStmt();
	else if (MATCH("ALIAS", 5))
		return_value = _readAlias();
	else if (MATCH("RANGEVAR", 8))
		return_value = _readRangeVar();
	else if (MATCH("INTOCLAUSE", 10))
		return_value = _readIntoClause();
	else if (MATCH("COPYINTOCLAUSE", 14))
		return_value = _readCopyIntoClause();
	else if (MATCH("REFRESHCLAUSE", 13))
		return_value = _readRefreshClause();
	else if (MATCH("TABLEFUNC", 9))
		return_value = _readTableFunc();
	else if (MATCH("VAR", 3))
		return_value = _readVar();
	else if (MATCH("CONST", 5))
		return_value = _readConst();
	else if (MATCH("PARAM", 5))
		return_value = _readParam();
	else if (MATCH("AGGREF", 6))
		return_value = _readAggref();
	else if (MATCH("GROUPINGFUNC", 12))
		return_value = _readGroupingFunc();
	else if (MATCH("GROUPID", 7))
		return_value = _readGroupId();
	else if (MATCH("GROUPINGSETID", 13))
		return_value = _readGroupingSetId();
	else if (MATCH("WINDOWFUNC", 10))
		return_value = _readWindowFunc();
	else if (MATCH("SUBSCRIPTINGREF", 15))
		return_value = _readSubscriptingRef();
	else if (MATCH("FUNCEXPR", 8))
		return_value = _readFuncExpr();
	else if (MATCH("NAMEDARGEXPR", 12))
		return_value = _readNamedArgExpr();
	else if (MATCH("OPEXPR", 6))
		return_value = _readOpExpr();
	else if (MATCH("DISTINCTEXPR", 12))
		return_value = _readDistinctExpr();
	else if (MATCH("NULLIFEXPR", 10))
		return_value = _readNullIfExpr();
	else if (MATCH("SCALARARRAYOPEXPR", 17))
		return_value = _readScalarArrayOpExpr();
	else if (MATCH("BOOLEXPR", 8))
		return_value = _readBoolExpr();
	else if (MATCH("SUBLINK", 7))
		return_value = _readSubLink();
	else if (MATCH("FIELDSELECT", 11))
		return_value = _readFieldSelect();
	else if (MATCH("FIELDSTORE", 10))
		return_value = _readFieldStore();
	else if (MATCH("RELABELTYPE", 11))
		return_value = _readRelabelType();
	else if (MATCH("COERCEVIAIO", 11))
		return_value = _readCoerceViaIO();
	else if (MATCH("ARRAYCOERCEEXPR", 15))
		return_value = _readArrayCoerceExpr();
	else if (MATCH("CONVERTROWTYPEEXPR", 18))
		return_value = _readConvertRowtypeExpr();
	else if (MATCH("COLLATE", 7))
		return_value = _readCollateExpr();
	else if (MATCH("CASE", 4))
		return_value = _readCaseExpr();
	else if (MATCH("WHEN", 4))
		return_value = _readCaseWhen();
	else if (MATCH("CASETESTEXPR", 12))
		return_value = _readCaseTestExpr();
	else if (MATCH("ARRAY", 5))
		return_value = _readArrayExpr();
	else if (MATCH("ROW", 3))
		return_value = _readRowExpr();
	else if (MATCH("ROWCOMPARE", 10))
		return_value = _readRowCompareExpr();
	else if (MATCH("COALESCE", 8))
		return_value = _readCoalesceExpr();
	else if (MATCH("MINMAX", 6))
		return_value = _readMinMaxExpr();
	else if (MATCH("SQLVALUEFUNCTION", 16))
		return_value = _readSQLValueFunction();
	else if (MATCH("XMLEXPR", 7))
		return_value = _readXmlExpr();
	else if (MATCH("NULLTEST", 8))
		return_value = _readNullTest();
	else if (MATCH("BOOLEANTEST", 11))
		return_value = _readBooleanTest();
	else if (MATCH("COERCETODOMAIN", 14))
		return_value = _readCoerceToDomain();
	else if (MATCH("COERCETODOMAINVALUE", 19))
		return_value = _readCoerceToDomainValue();
	else if (MATCH("SETTODEFAULT", 12))
		return_value = _readSetToDefault();
	else if (MATCH("CURRENTOFEXPR", 13))
		return_value = _readCurrentOfExpr();
	else if (MATCH("NEXTVALUEEXPR", 13))
		return_value = _readNextValueExpr();
	else if (MATCH("INFERENCEELEM", 13))
		return_value = _readInferenceElem();
	else if (MATCH("TARGETENTRY", 11))
		return_value = _readTargetEntry();
	else if (MATCH("RANGETBLREF", 11))
		return_value = _readRangeTblRef();
	else if (MATCH("JOINEXPR", 8))
		return_value = _readJoinExpr();
	else if (MATCH("FROMEXPR", 8))
		return_value = _readFromExpr();
	else if (MATCH("ONCONFLICTEXPR", 14))
		return_value = _readOnConflictExpr();
	else if (MATCH("APPENDRELINFO", 13))
		return_value = _readAppendRelInfo();
	else if (MATCH("RTE", 3))
		return_value = _readRangeTblEntry();
	else if (MATCH("RANGETBLFUNCTION", 16))
		return_value = _readRangeTblFunction();
	else if (MATCH("TABLESAMPLECLAUSE", 17))
		return_value = _readTableSampleClause();
	else if (MATCH("NOTIFY", 6))
		return_value = _readNotifyStmt();
	else if (MATCH("DEFELEM", 7))
		return_value = _readDefElem();
	else if (MATCH("DECLARECURSOR", 13))
		return_value = _readDeclareCursorStmt();
	else if (MATCH("PLANNEDSTMT", 11))
		return_value = _readPlannedStmt();
	else if (MATCH("PLAN", 4))
		return_value = _readPlan();
	else if (MATCH("RESULT", 6))
		return_value = _readResult();
	else if (MATCH("PROJECTSET", 10))
		return_value = _readProjectSet();
	else if (MATCH("MODIFYTABLE", 11))
		return_value = _readModifyTable();
	else if (MATCH("APPEND", 6))
		return_value = _readAppend();
	else if (MATCH("MERGEAPPEND", 11))
		return_value = _readMergeAppend();
	else if (MATCH("RECURSIVEUNION", 14))
		return_value = _readRecursiveUnion();
	else if (MATCH("BITMAPAND", 9))
		return_value = _readBitmapAnd();
	else if (MATCH("BITMAPOR", 8))
		return_value = _readBitmapOr();
	else if (MATCH("SCAN", 4))
		return_value = _readScan();
	else if (MATCH("SEQSCAN", 7))
		return_value = _readSeqScan();
	else if (MATCH("SAMPLESCAN", 10))
		return_value = _readSampleScan();
	else if (MATCH("INDEXSCAN", 9))
		return_value = _readIndexScan();
	else if (MATCH("DYNAMICINDEXSCAN", 16))
		return_value = _readDynamicIndexScan();
	else if (MATCH("DYNAMICINDEXONLYSCAN", 20))
		return_value = _readDynamicIndexOnlyScan();
	else if (MATCH("INDEXONLYSCAN", 13))
		return_value = _readIndexOnlyScan();
	else if (MATCH("BITMAPINDEXSCAN", 15))
		return_value = _readBitmapIndexScan();
	else if (MATCH("DYNAMICBITMAPINDEXSCAN", 23))
		return_value = _readDynamicBitmapIndexScan();
	else if (MATCH("BITMAPHEAPSCAN", 14))
		return_value = _readBitmapHeapScan();
	else if (MATCH("DYNAMICBITMAPHEAPSCAN", 21))
		return_value = _readDynamicBitmapHeapScan();
	else if (MATCH("TIDSCAN", 7))
		return_value = _readTidScan();
	else if (MATCH("TIDRANGESCAN", 12))
		return_value = _readTidRangeScan();
	else if (MATCH("SUBQUERYSCAN", 12))
		return_value = _readSubqueryScan();
	else if (MATCH("TABLEFUNCTIONSCAN", 17))
		return_value = _readTableFunctionScan();
	else if (MATCH("FUNCTIONSCAN", 12))
		return_value = _readFunctionScan();
	else if (MATCH("VALUESSCAN", 10))
		return_value = _readValuesScan();
	else if (MATCH("TABLEFUNCSCAN", 13))
		return_value = _readTableFuncScan();
	else if (MATCH("CTESCAN", 7))
		return_value = _readCteScan();
	else if (MATCH("NAMEDTUPLESTORESCAN", 19))
		return_value = _readNamedTuplestoreScan();
	else if (MATCH("WORKTABLESCAN", 13))
		return_value = _readWorkTableScan();
	else if (MATCH("FOREIGNSCAN", 11))
		return_value = _readForeignScan();
	else if (MATCH("DYNAMICFOREIGNSCAN", 18))
		return_value = _readDynamicForeignScan();
	else if (MATCH("CUSTOMSCAN", 10))
		return_value = _readCustomScan();
	else if (MATCH("JOIN", 4))
		return_value = _readJoin();
	else if (MATCH("NESTLOOP", 8))
		return_value = _readNestLoop();
	else if (MATCH("MERGEJOIN", 9))
		return_value = _readMergeJoin();
	else if (MATCH("HASHJOIN", 8))
		return_value = _readHashJoin();
	else if (MATCH("MATERIAL", 8))
		return_value = _readMaterial();
	else if (MATCH("MEMOIZE", 7))
		return_value = _readMemoize();
	else if (MATCH("SORT", 4))
		return_value = _readSort();
	else if (MATCH("INCREMENTALSORT", 15))
		return_value = _readIncrementalSort();
	else if (MATCH("GROUP", 5))
		return_value = _readGroup();
	else if (MATCH("AGG", 3))
		return_value = _readAgg();
	else if (MATCH("TupleSplit", 10))
		return_value = _readTupleSplit();
	else if (MATCH("DQAExpr", 7))
		return_value = _readDQAExpr();
	else if (MATCH("WINDOWAGG", 9))
		return_value = _readWindowAgg();
	else if (MATCH("WINDOWHASHAGG", 13))
		return_value = _readWindowHashAgg();
	else if (MATCH("UNIQUE", 6))
		return_value = _readUnique();
	else if (MATCH("GATHER", 6))
		return_value = _readGather();
	else if (MATCH("GATHERMERGE", 11))
		return_value = _readGatherMerge();
	else if (MATCH("HASH", 4))
		return_value = _readHash();
	else if (MATCH("SETOP", 5))
		return_value = _readSetOp();
	else if (MATCH("LOCKROWS", 8))
		return_value = _readLockRows();
	else if (MATCH("RUNTIMEFILTER", 13))
		return_value = _readRuntimeFilter();
	else if (MATCH("LIMIT", 5))
		return_value = _readLimit();
	else if (MATCH("NESTLOOPPARAM", 13))
		return_value = _readNestLoopParam();
	else if (MATCH("PLANROWMARK", 11))
		return_value = _readPlanRowMark();
	else if (MATCH("PARTITIONPRUNEINFO", 18))
		return_value = _readPartitionPruneInfo();
	else if (MATCH("PARTITIONEDRELPRUNEINFO", 23))
		return_value = _readPartitionedRelPruneInfo();
	else if (MATCH("PARTITIONPRUNESTEPOP", 20))
		return_value = _readPartitionPruneStepOp();
	else if (MATCH("PARTITIONPRUNESTEPCOMBINE", 25))
		return_value = _readPartitionPruneStepCombine();
	else if (MATCH("PLANINVALITEM", 13))
		return_value = _readPlanInvalItem();
	else if (MATCH("SUBPLAN", 7))
		return_value = _readSubPlan();
	else if (MATCH("ALTERNATIVESUBPLAN", 18))
		return_value = _readAlternativeSubPlan();
	else if (MATCH("RESTRICTINFO", 12))
		return_value = _readRestrictInfo();
	else if (MATCH("EXTENSIBLENODE", 14))
		return_value = _readExtensibleNode();
	else if (MATCH("PARTITIONBOUNDSPEC", 18))
		return_value = _readPartitionBoundSpec();
	else if (MATCH("PARTITIONRANGEDATUM", 19))
		return_value = _readPartitionRangeDatum();
	else if (MATCH("PARTITIONSPEC", 13))
		return_value = _readPartitionSpec();
	else if (MATCH("PARTITIONELEM", 13))
		return_value = _readPartitionElem();
	else if (MATCHX("PARTITIONCMD"))
		return_value = _readPartitionCmd();

	/* GPDB additions */
	else if (MATCHX("A_ARRAYEXPR"))
		return_value = _readA_ArrayExpr();
	else if (MATCHX("A_CONST"))
		return_value = _readAConst();
	else if (MATCHX("AEXPR"))
		return_value = _readAExpr();
	else if (MATCHX("ALTERDOMAINSTMT"))
		return_value = _readAlterDomainStmt();
	else if (MATCHX("ALTERFUNCTIONSTMT"))
		return_value = _readAlterFunctionStmt();
	else if (MATCHX("ALTEROBJECTSCHEMASTMT"))
		return_value = _readAlterObjectSchemaStmt();
	else if (MATCHX("ALTEROWNERSTMT"))
		return_value = _readAlterOwnerStmt();
	else if (MATCHX("ALTEROPFAMILYSTMT"))
		return_value = _readAlterOpFamilyStmt();
	else if (MATCHX("ALTERPOLICYSTMT"))
		return_value = _readAlterPolicyStmt();
	else if (MATCHX("ALTERROLESETSTMT"))
		return_value = _readAlterRoleSetStmt();
	else if (MATCHX("ALTERSYSTEMSTMT"))
		return_value = _readAlterSystemStmt();
	else if (MATCHX("ALTERROLESTMT"))
		return_value = _readAlterRoleStmt();
	else if (MATCHX("ALTERPROFILESTMT"))
		return_value = _readAlterProfileStmt();
	else if (MATCHX("ALTERSEQSTMT"))
		return_value = _readAlterSeqStmt();
	else if (MATCHX("ALTERTABLECMD"))
		return_value = _readAlterTableCmd();
	else if (MATCHX("ALTEREDTABLEINFO"))
		return_value = _readAlteredTableInfo();
	else if (MATCHX("NEWCONSTRAINT"))
		return_value = _readNewConstraint();
	else if (MATCHX("NEWCOLUMNVALUE"))
		return_value = _readNewColumnValue();
	else if (MATCHX("ALTERTABLESTMT"))
		return_value = _readAlterTableStmt();
	else if (MATCHX("ALTERTYPESTMT"))
		return_value = _readAlterTypeStmt();
	else if (MATCHX("CDBPROCESS"))
		return_value = _readCdbProcess();
	else if (MATCHX("CLUSTERSTMT"))
		return_value = _readClusterStmt();
	else if (MATCHX("COLUMNDEF"))
		return_value = _readColumnDef();
	else if (MATCHX("COLUMNREF"))
		return_value = _readColumnRef();
	else if (MATCHX("PARAMREF"))
		return_value = _readParamRef();
	else if (MATCHX("COMMONTABLEEXPR"))
		return_value = _readCommonTableExpr();
	else if (MATCHX("COMPTYPESTMT"))
		return_value = _readCompositeTypeStmt();
	else if (MATCHX("CONSTRAINT"))
		return_value = _readConstraint();
	else if (MATCHX("CONSTRAINTSSETSTMT"))
		return_value = _readConstraintsSetStmt();
	else if (MATCHX("CREATECAST"))
		return_value = _readCreateCastStmt();
	else if (MATCHX("CREATECONVERSION"))
		return_value = _readCreateConversionStmt();
	else if (MATCHX("CREATEDBSTMT"))
		return_value = _readCreatedbStmt();
	else if (MATCHX("CREATEDOMAINSTMT"))
		return_value = _readCreateDomainStmt();
	else if (MATCHX("CREATEENUMSTMT"))
		return_value = _readCreateEnumStmt();
	else if (MATCHX("CREATEEXTERNALSTMT"))
		return_value = _readCreateExternalStmt();
	else if (MATCHX("CREATEFUNCSTMT"))
		return_value = _readCreateFunctionStmt();
	else if (MATCHX("CREATEOPCLASS"))
		return_value = _readCreateOpClassStmt();
	else if (MATCHX("CREATEOPCLASSITEM"))
		return_value = _readCreateOpClassItem();
	else if (MATCHX("CREATEOPFAMILYSTMT"))
		return_value = _readCreateOpFamilyStmt();
	else if (MATCHX("CREATEPLANGSTMT"))
		return_value = _readCreatePLangStmt();
	else if (MATCHX("CREATEPUBLICATIONSTMT"))
		return_value = _readCreatePublicationStmt();
	else if (MATCHX("ALTERPUBLICATIONSTMT"))
		return_value = _readAlterPublicationStmt();
	else if (MATCHX("CREATESUBSCRIPTIONSTMT"))
		return_value = _readCreateSubscriptionStmt();
	else if (MATCHX("DROPSUBSCRIPTIONSTMT"))
		return_value = _readDropSubscriptionStmt();
	else if (MATCHX("ALTERSUBSCRIPTIONSTMT"))
		return_value = _readAlterSubscriptionStmt();
	else if (MATCHX("CREATEPOLICYSTMT"))
		return_value = _readCreatePolicyStmt();
	else if (MATCHX("CREATEROLESTMT"))
		return_value = _readCreateRoleStmt();
	else if (MATCHX("CREATEPROFILESTMT"))
		return_value = _readCreateProfileStmt();
	else if (MATCHX("CREATESCHEMASTMT"))
		return_value = _readCreateSchemaStmt();
	else if (MATCHX("ALTERSCHEMASTMT"))
		return_value = _readAlterSchemaStmt();
	else if (MATCHX("CREATETAGSTMT"))
		return_value = _readCreateTagStmt();
	else if (MATCHX("ALTERTAGSTMT"))
		return_value = _readAlterTagStmt();
	else if (MATCHX("DROPTAGSTMT"))
		return_value = _readDropTagStmt();
	else if (MATCHX("CREATESEQSTMT"))
		return_value = _readCreateSeqStmt();
	else if (MATCHX("CREATETRANSFORMSTMT"))
		return_value = _readCreateTransformStmt();
	else if (MATCHX("CURSORPOSINFO"))
		return_value = _readCursorPosInfo();
	else if (MATCHX("DEFELEM"))
		return_value = _readDefElem();
	else if (MATCHX("DEFINESTMT"))
		return_value = _readDefineStmt();
	else if (MATCHX("DENYLOGININTERVAL"))
		return_value = _readDenyLoginInterval();
	else if (MATCHX("DENYLOGINPOINT"))
		return_value = _readDenyLoginPoint();
	else if (MATCHX("DROPDBSTMT"))
		return_value = _readDropdbStmt();
	else if (MATCHX("DROPROLESTMT"))
		return_value = _readDropRoleStmt();
	else if (MATCHX("DROPPROFILESTMT"))
		return_value = _readDropProfileStmt();
	else if (MATCHX("DROPSTMT"))
		return_value = _readDropStmt();
	else if (MATCHX("DISTRIBUTIONKEYELEM"))
		return_value = _readDistributionKeyElem();
	else if (MATCHX("EXTTABLETYPEDESC"))
		return_value = _readExtTableTypeDesc();
	else if (MATCHX("FUNCCALL"))
		return_value = _readFuncCall();
	else if (MATCHX("FUNCTIONPARAMETER"))
		return_value = _readFunctionParameter();
	else if (MATCHX("OBJECTWITHARGS"))
		return_value = _readObjectWithArgs();
	else if (MATCHX("GRANTROLESTMT"))
		return_value = _readGrantRoleStmt();
	else if (MATCHX("GRANTSTMT"))
		return_value = _readGrantStmt();
	else if (MATCHX("INDEXELEM"))
		return_value = _readIndexElem();
	else if (MATCHX("INDEXSTMT"))
		return_value = _readIndexStmt();
	else if (MATCHX("LOCKSTMT"))
		return_value = _readLockStmt();
	else if (MATCHX("REINDEXSTMT"))
		return_value = _readReindexStmt();
	else if (MATCHX("REINDEXINDEXINFO"))
		return_value = _readReindexIndexInfo();
	else if (MATCHX("RENAMESTMT"))
		return_value = _readRenameStmt();
	else if (MATCHX("REPLICAIDENTITYSTMT"))
		return_value = _readReplicaIdentityStmt();
	else if (MATCHX("RULESTMT"))
		return_value = _readRuleStmt();
	else if (MATCHX("SEGFILEMAPNODE"))
		return_value = _readSegfileMapNode();
	else if (MATCHX("SINGLEROWERRORDESC"))
		return_value = _readSingleRowErrorDesc();
	else if (MATCHX("SLICETABLE"))
		return_value = _readSliceTable();
	else if (MATCHX("SORTBY"))
		return_value = _readSortBy();
	else if (MATCHX("TABLEVALUEEXPR"))
		return_value = _readTableValueExpr();
	else if (MATCHX("TRUNCATESTMT"))
		return_value = _readTruncateStmt();
	else if (MATCHX("TYPECAST"))
		return_value = _readTypeCast();
	else if (MATCHX("TYPENAME"))
		return_value = _readTypeName();
	else if (MATCHX("VACUUMSTMT"))
		return_value = _readVacuumStmt();
	else if (MATCHX("VACUUMRELATION"))
		return_value = _readVacuumRelation();
	else if (MATCHX("VARIABLESETSTMT"))
		return_value = _readVariableSetStmt();
	else if (MATCHX("VIEWSTMT"))
		return_value = _readViewStmt();
	else if (MATCHX("WITHCLAUSE"))
		return_value = _readWithClause();
	else if (MATCHX("GPPARTITIONDEFINITION"))
		return_value = _readGpPartitionDefinition();
	else if (MATCHX("GPPARTDEFELEM"))
		return_value = _readGpPartDefElem();
	else if (MATCHX("GPPARTITIONRANGEITEM"))
		return_value = _readGpPartitionRangeItem();
	else if (MATCHX("GPPARTITIONRANGESPEC"))
		return_value = _readGpPartitionRangeSpec();
	else if (MATCHX("GPPARTITIONLISTSPEC"))
		return_value = _readGpPartitionListSpec();
	else if (MATCHX("COLUMNREFERENCESTORAGEDIRECTIVE"))
		return_value = _readColumnReferenceStorageDirective();
	else if (MATCHX("RETURN"))
		return_value = _readReturnStmt();
	else if (MATCHX("DROPDIRECTORYTABLESTMT"))
		return_value = _readDropDirectoryTableStmt();
	else if (MATCHX("RTEPERMISSIONINFO"))
		return_value = _readRTEPermissionInfo();
	else if (MATCHX("GPPOLICY"))
		return_value = _readGpPolicy();
	else if (MATCHX("MERGEACTION"))
		return_value = _readMergeAction();
	else if (MATCHX("PUBLICATIONOBJSPEC"))
		return_value = _readPublicationObjSpec();
	else if (MATCHX("PUBLICATIONTABLE"))
		return_value = _readPublicationTable();
	else if (MATCHX("WINDOWDEF"))
		return_value = _readWindowDef();
	else if (MATCHX("JSONCONSTRUCTOREXPR"))
		return_value = _readJsonConstructorExpr();
	else if (MATCHX("JSONISPREDICATE"))
		return_value = _readJsonIsPredicate();
	else if (MATCHX("JSONRETURNING"))
		return_value = _readJsonReturning();
	else if (MATCHX("JSONVALUEEXPR"))
		return_value = _readJsonValueExpr();
	else if (MATCHX("JSONFORMAT"))
		return_value = _readJsonFormat();
	else if (MATCHX("ALTERDATABASESTMT"))
		return_value = _readAlterDatabaseStmt();
	else if (MATCHX("ALTERDIRECTORYTABLESTMT"))
		return_value = _readAlterDirectoryTableStmt();
	else if (MATCHX("ALTEREXTENSIONCONTENTSSTMT"))
		return_value = _readAlterExtensionContentsStmt();
	else if (MATCHX("ALTEREXTENSIONSTMT"))
		return_value = _readAlterExtensionStmt();
	else if (MATCHX("ALTERQUEUESTMT"))
		return_value = _readAlterQueueStmt();
	else if (MATCHX("ALTERRESOURCEGROUPSTMT"))
		return_value = _readAlterResourceGroupStmt();
	else if (MATCHX("ALTERSTATSSTMT"))
		return_value = _readAlterStatsStmt();
	else if (MATCHX("ALTERTASKSTMT"))
		return_value = _readAlterTaskStmt();
	else if (MATCHX("ALTERTSCONFIGURATIONSTMT"))
		return_value = _readAlterTSConfigurationStmt();
	else if (MATCHX("ALTERTSDICTIONARYSTMT"))
		return_value = _readAlterTSDictionaryStmt();
	else if (MATCHX("A_INDICES"))
		return_value = _readA_Indices();
	else if (MATCHX("A_INDIRECTION"))
		return_value = _readA_Indirection();
	else if (MATCHX("A_STAR"))
		return_value = _readA_Star();
	else if (MATCHX("COLLATECLAUSE"))
		return_value = _readCollateClause();
	else if (MATCHX("COMMENTSTMT"))
		return_value = _readCommentStmt();
	else if (MATCHX("COPYSTMT"))
		return_value = _readCopyStmt();
	else if (MATCHX("CREATEDIRECTORYTABLESTMT"))
		return_value = _readCreateDirectoryTableStmt();
	else if (MATCHX("CREATEFOREIGNTABLESTMT"))
		return_value = _readCreateForeignTableStmt();
	else if (MATCHX("CREATEQUEUESTMT"))
		return_value = _readCreateQueueStmt();
	else if (MATCHX("CREATERANGESTMT"))
		return_value = _readCreateRangeStmt();
	else if (MATCHX("CREATERESOURCEGROUPSTMT"))
		return_value = _readCreateResourceGroupStmt();
	else if (MATCHX("CREATESTATSSTMT"))
		return_value = _readCreateStatsStmt();
	else if (MATCHX("CREATESTMT"))
		return_value = _readCreateStmt();
	else if (MATCHX("CREATETABLESPACESTMT"))
		return_value = _readCreateTableSpaceStmt();
	else if (MATCHX("CREATETASKSTMT"))
		return_value = _readCreateTaskStmt();
	else if (MATCHX("CREATETRIGSTMT"))
		return_value = _readCreateTrigStmt();
	else if (MATCHX("DISTRIBUTEDBY"))
		return_value = _readDistributedBy();
	else if (MATCHX("DISTRIBUTIONKEY"))
		return_value = _readDistributionKey();
	else if (MATCHX("DMLACTIONEXPR"))
		return_value = _readDMLActionExpr();
	else if (MATCHX("DROPOWNEDSTMT"))
		return_value = _readDropOwnedStmt();
	else if (MATCHX("DROPQUEUESTMT"))
		return_value = _readDropQueueStmt();
	else if (MATCHX("DROPRESOURCEGROUPSTMT"))
		return_value = _readDropResourceGroupStmt();
	else if (MATCHX("DROPTABLESPACESTMT"))
		return_value = _readDropTableSpaceStmt();
	else if (MATCHX("DROPTASKSTMT"))
		return_value = _readDropTaskStmt();
	else if (MATCHX("EXTERNALSCANINFO"))
		return_value = _readExternalScanInfo();
	else if (MATCHX("GPALTERPARTITIONCMD"))
		return_value = _readGpAlterPartitionCmd();
	else if (MATCHX("GPALTERPARTITIONID"))
		return_value = _readGpAlterPartitionId();
	else if (MATCHX("GPDROPPARTITIONCMD"))
		return_value = _readGpDropPartitionCmd();
	else if (MATCHX("GROUPEDVARINFO"))
		return_value = _readGroupedVarInfo();
	else if (MATCHX("IMPORTFOREIGNSCHEMASTMT"))
		return_value = _readImportForeignSchemaStmt();
	else if (MATCHX("LOCKINGCLAUSE"))
		return_value = _readLockingClause();
	else if (MATCHX("MULTIASSIGNREF"))
		return_value = _readMultiAssignRef();
	else if (MATCHX("PLACEHOLDERVAR"))
		return_value = _readPlaceHolderVar();
	else if (MATCHX("RANGEFUNCTION"))
		return_value = _readRangeFunction();
	else if (MATCHX("RANGESUBSELECT"))
		return_value = _readRangeSubselect();
	else if (MATCHX("RANGETABLEFUNC"))
		return_value = _readRangeTableFunc();
	else if (MATCHX("RANGETABLEFUNCCOL"))
		return_value = _readRangeTableFuncCol();
	else if (MATCHX("RANGETABLESAMPLE"))
		return_value = _readRangeTableSample();
	else if (MATCHX("RAWSTMT"))
		return_value = _readRawStmt();
	else if (MATCHX("REASSIGNOWNEDSTMT"))
		return_value = _readReassignOwnedStmt();
	else if (MATCHX("RELAGGINFO"))
		return_value = _readRelAggInfo();
	else if (MATCHX("RESTARGET"))
		return_value = _readResTarget();
	else if (MATCHX("STATSELEM"))
		return_value = _readStatsElem();
	else if (MATCHX("TABLELIKECLAUSE"))
		return_value = _readTableLikeClause();
	else if (MATCHX("TRANSACTIONSTMT"))
		return_value = _readTransactionStmt();
	else if (MATCHX("TRIGGERTRANSITION"))
		return_value = _readTriggerTransition();
	else if (MATCHX("XMLSERIALIZE"))
		return_value = _readXmlSerialize();
	else if (MATCHX("ACCESSPRIV"))
		return_value = _readAccessPriv();
	else if (MATCHX("ADDFOREIGNSEGSTMT"))
		return_value = _readAddForeignSegStmt();
	else if (MATCHX("AGGEXPRID"))
		return_value = _readAggExprId();
	else if (MATCHX("ALTERCOLLATIONSTMT"))
		return_value = _readAlterCollationStmt();
	else if (MATCHX("ALTERDATABASEREFRESHCOLLSTMT"))
		return_value = _readAlterDatabaseRefreshCollStmt();
	else if (MATCHX("ALTERDATABASESETSTMT"))
		return_value = _readAlterDatabaseSetStmt();
	else if (MATCHX("ALTERDEFAULTPRIVILEGESSTMT"))
		return_value = _readAlterDefaultPrivilegesStmt();
	else if (MATCHX("ALTERENUMSTMT"))
		return_value = _readAlterEnumStmt();
	else if (MATCHX("ALTEREVENTTRIGSTMT"))
		return_value = _readAlterEventTrigStmt();
	else if (MATCHX("ALTERFDWSTMT"))
		return_value = _readAlterFdwStmt();
	else if (MATCHX("ALTERFOREIGNSERVERSTMT"))
		return_value = _readAlterForeignServerStmt();
	else if (MATCHX("ALTEROBJECTDEPENDSSTMT"))
		return_value = _readAlterObjectDependsStmt();
	else if (MATCHX("ALTEROPERATORSTMT"))
		return_value = _readAlterOperatorStmt();
	else if (MATCHX("ALTERSTORAGESERVERSTMT"))
		return_value = _readAlterStorageServerStmt();
	else if (MATCHX("ALTERSTORAGEUSERMAPPINGSTMT"))
		return_value = _readAlterStorageUserMappingStmt();
	else if (MATCHX("ALTERTABLEMOVEALLSTMT"))
		return_value = _readAlterTableMoveAllStmt();
	else if (MATCHX("ALTERTABLESPACEOPTIONSSTMT"))
		return_value = _readAlterTableSpaceOptionsStmt();
	else if (MATCHX("ALTERUSERMAPPINGSTMT"))
		return_value = _readAlterUserMappingStmt();
	else if (MATCHX("CALLSTMT"))
		return_value = _readCallStmt();
	else if (MATCHX("CHECKPOINTSTMT"))
		return_value = _readCheckPointStmt();
	else if (MATCHX("CLOSEPORTALSTMT"))
		return_value = _readClosePortalStmt();
	else if (MATCHX("COMPOUNDUTILITYSTMT"))
		return_value = _readCompoundUtilityStmt();
	else if (MATCHX("CREATEAMSTMT"))
		return_value = _readCreateAmStmt();
	else if (MATCHX("CREATEEVENTTRIGSTMT"))
		return_value = _readCreateEventTrigStmt();
	else if (MATCHX("CREATEEXTENSIONSTMT"))
		return_value = _readCreateExtensionStmt();
	else if (MATCHX("CREATEFDWSTMT"))
		return_value = _readCreateFdwStmt();
	else if (MATCHX("CREATEFOREIGNSERVERSTMT"))
		return_value = _readCreateForeignServerStmt();
	else if (MATCHX("CREATEFOREIGNSTMT"))
		return_value = _readCreateForeignStmt();
	else if (MATCHX("CREATESTORAGESERVERSTMT"))
		return_value = _readCreateStorageServerStmt();
	else if (MATCHX("CREATESTORAGEUSERMAPPINGSTMT"))
		return_value = _readCreateStorageUserMappingStmt();
	else if (MATCHX("CREATETABLEASSTMT"))
		return_value = _readCreateTableAsStmt();
	else if (MATCHX("CREATEUSERMAPPINGSTMT"))
		return_value = _readCreateUserMappingStmt();
	else if (MATCHX("CREATEWAREHOUSESTMT"))
		return_value = _readCreateWarehouseStmt();
	else if (MATCHX("DEALLOCATESTMT"))
		return_value = _readDeallocateStmt();
	else if (MATCHX("DELETESTMT"))
		return_value = _readDeleteStmt();
	else if (MATCHX("DISCARDSTMT"))
		return_value = _readDiscardStmt();
	else if (MATCHX("DOSTMT"))
		return_value = _readDoStmt();
	else if (MATCHX("DROPSTORAGESERVERSTMT"))
		return_value = _readDropStorageServerStmt();
	else if (MATCHX("DROPSTORAGEUSERMAPPINGSTMT"))
		return_value = _readDropStorageUserMappingStmt();
	else if (MATCHX("DROPUSERMAPPINGSTMT"))
		return_value = _readDropUserMappingStmt();
	else if (MATCHX("DROPWAREHOUSESTMT"))
		return_value = _readDropWarehouseStmt();
	else if (MATCHX("EXECUTESTMT"))
		return_value = _readExecuteStmt();
	else if (MATCHX("EXPLAINSTMT"))
		return_value = _readExplainStmt();
	else if (MATCHX("FETCHSTMT"))
		return_value = _readFetchStmt();
	else if (MATCHX("GPSPLITPARTITIONCMD"))
		return_value = _readGpSplitPartitionCmd();
	else if (MATCHX("INFERCLAUSE"))
		return_value = _readInferClause();
	else if (MATCHX("INSERTSTMT"))
		return_value = _readInsertStmt();
	else if (MATCHX("JSONAGGCONSTRUCTOR"))
		return_value = _readJsonAggConstructor();
	else if (MATCHX("JSONARRAYAGG"))
		return_value = _readJsonArrayAgg();
	else if (MATCHX("JSONARRAYCONSTRUCTOR"))
		return_value = _readJsonArrayConstructor();
	else if (MATCHX("JSONARRAYQUERYCONSTRUCTOR"))
		return_value = _readJsonArrayQueryConstructor();
	else if (MATCHX("JSONKEYVALUE"))
		return_value = _readJsonKeyValue();
	else if (MATCHX("JSONOBJECTAGG"))
		return_value = _readJsonObjectAgg();
	else if (MATCHX("JSONOBJECTCONSTRUCTOR"))
		return_value = _readJsonObjectConstructor();
	else if (MATCHX("JSONOUTPUT"))
		return_value = _readJsonOutput();
	else if (MATCHX("LISTENSTMT"))
		return_value = _readListenStmt();
	else if (MATCHX("LOADSTMT"))
		return_value = _readLoadStmt();
	else if (MATCHX("MERGESTMT"))
		return_value = _readMergeStmt();
	else if (MATCHX("MERGEWHENCLAUSE"))
		return_value = _readMergeWhenClause();
	else if (MATCHX("ONCONFLICTCLAUSE"))
		return_value = _readOnConflictClause();
	else if (MATCHX("PLASSIGNSTMT"))
		return_value = _readPLAssignStmt();
	else if (MATCHX("PREPARESTMT"))
		return_value = _readPrepareStmt();
	else if (MATCHX("REFRESHMATVIEWSTMT"))
		return_value = _readRefreshMatViewStmt();
	else if (MATCHX("RETRIEVESTMT"))
		return_value = _readRetrieveStmt();
	else if (MATCHX("ROLESPEC"))
		return_value = _readRoleSpec();
	else if (MATCHX("ROWIDEXPR"))
		return_value = _readRowIdExpr();
	else if (MATCHX("SECLABELSTMT"))
		return_value = _readSecLabelStmt();
	else if (MATCHX("SELECTSTMT"))
		return_value = _readSelectStmt();
	else if (MATCHX("UNLISTENSTMT"))
		return_value = _readUnlistenStmt();
	else if (MATCHX("UPDATESTMT"))
		return_value = _readUpdateStmt();
	else if (MATCHX("VARIABLESHOWSTMT"))
		return_value = _readVariableShowStmt();
	else
	{
        ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("This operation involves an internal data item "
						"of a type called \"%.*s\" which is not "
						"supported in this version of %s.",
						length, token, PACKAGE_NAME)));
		return_value = NULL;	/* keep compiler quiet */
	}

	return (Node *) return_value;
}


/*
 * readDatum
 *
 * Given a string representation of a constant, recreate the appropriate
 * Datum.  The string representation embeds length info, but not byValue,
 * so we must be told that.
 */
Datum
readDatum(bool typbyval)
{
	Size		length,
				i;
	int			tokenLength;
	const char *token;
	Datum		res;
	char	   *s;

	/*
	 * read the actual length of the value
	 */
	token = pg_strtok(&tokenLength);
	length = atoui(token);

	token = pg_strtok(&tokenLength);	/* read the '[' */
	if (token == NULL || token[0] != '[')
		elog(ERROR, "expected \"[\" to start datum, but got \"%s\"; length = %zu",
			 token ? token : "[NULL]", length);

	if (typbyval)
	{
		if (length > (Size) sizeof(Datum))
			elog(ERROR, "byval datum but length = %zu", length);
		res = (Datum) 0;
		s = (char *) (&res);
		for (i = 0; i < (Size) sizeof(Datum); i++)
		{
			token = pg_strtok(&tokenLength);
			s[i] = (char) atoi(token);
		}
	}
	else if (length <= 0)
		res = (Datum) NULL;
	else
	{
		s = (char *) palloc(length);
		for (i = 0; i < length; i++)
		{
			token = pg_strtok(&tokenLength);
			s[i] = (char) atoi(token);
		}
		res = PointerGetDatum(s);
	}

	token = pg_strtok(&tokenLength);	/* read the ']' */
	if (token == NULL || token[0] != ']')
		elog(ERROR, "expected \"]\" to end datum, but got \"%s\"; length = %zu",
			 token ? token : "[NULL]", length);

	return res;
}

/*
 * readAttrNumberCols
 */
AttrNumber *
readAttrNumberCols(int numCols)
{
	int			tokenLength,
				i;
	const char *token;
	AttrNumber *attr_vals;

	if (numCols <= 0)
		return NULL;

	attr_vals = (AttrNumber *) palloc(numCols * sizeof(AttrNumber));
	for (i = 0; i < numCols; i++)
	{
		token = pg_strtok(&tokenLength);
		attr_vals[i] = atoi(token);
	}

	return attr_vals;
}

/*
 * readOidCols
 */
Oid *
readOidCols(int numCols)
{
	int			tokenLength,
				i;
	const char *token;
	Oid		   *oid_vals;

	if (numCols <= 0)
		return NULL;

	oid_vals = (Oid *) palloc(numCols * sizeof(Oid));
	for (i = 0; i < numCols; i++)
	{
		token = pg_strtok(&tokenLength);
		oid_vals[i] = atooid(token);
	}

	return oid_vals;
}

/*
 * readIntCols
 */
int *
readIntCols(int numCols)
{
	int			tokenLength,
				i;
	const char *token;
	int		   *int_vals;

	if (numCols <= 0)
		return NULL;

	int_vals = (int *) palloc(numCols * sizeof(int));
	for (i = 0; i < numCols; i++)
	{
		token = pg_strtok(&tokenLength);
		int_vals[i] = atoi(token);
	}

	return int_vals;
}

/*
 * readBoolCols
 */
bool *
readBoolCols(int numCols)
{
	int			tokenLength,
				i;
	const char *token;
	bool	   *bool_vals;

	if (numCols <= 0)
		return NULL;

	bool_vals = (bool *) palloc(numCols * sizeof(bool));
	for (i = 0; i < numCols; i++)
	{
		token = pg_strtok(&tokenLength);
		bool_vals[i] = strtobool(token);
	}

	return bool_vals;
}
#endif
