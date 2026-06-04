/*-------------------------------------------------------------------------
 *
 * outfast.c
 *	  Fast serialization functions for Postgres tree nodes.
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * NOTES
 *	  Every node type that can appear in an Greenplum Database serialized query or plan
 *    tree must have an output function defined here.
 *
 * 	  There *MUST* be a one-to-one correspondence between this routine
 *    and readfast.c.  If not, you will likely crash the system.
 *
 *     By design, the only user of these routines is the function
 *     serializeNode in cdbsrlz.c.  Other callers beware.
 *
 *    Most _out* functions are auto-generated and shared with outfuncs.c
 *    via the included outfuncs.funcs.c file. Only custom/hand-written
 *    functions for plan nodes, GPDB-specific nodes, and nodes requiring
 *    special binary serialization are defined directly in this file.
 *
 * 	  Rather than serialize to a (somewhat human-readable) string, these
 *    routines create a binary serialization via a simple depth-first walk
 *    of the tree.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>

#include "lib/stringinfo.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "utils/datum.h"
#include "utils/expandeddatum.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "cdb/cdbgang.h"
#include "utils/workfile_mgr.h"
#include "parser/parsetree.h"

/*
 * Macros to simplify output of different kinds of fields.	Use these
 * wherever possible to reduce the chance for silly typos.	Note that these
 * hard-wire conventions about the names of the local variables in an Out
 * routine.
 */

/*
 * Write the label for the node type.  nodelabel is accepted for
 * compatibility with outfuncs.c, but is ignored
 */
#define WRITE_NODE_TYPE(nodelabel) \
	{ int16 nt =nodeTag(node); appendBinaryStringInfo(str, (const char *)&nt, sizeof(int16)); }

/* Write an integer field  */
#define WRITE_INT_FIELD(fldname) \
	{ appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(int)); }

/* Write an integer field  */
#define WRITE_INT8_FIELD(fldname) \
	{ appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(int8)); }

/* Write an integer field  */
#define WRITE_INT16_FIELD(fldname) \
	{ appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(int16)); }

/* Write an unsigned integer field */
#define WRITE_UINT_FIELD(fldname) \
	appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(int))

/* Write an uint64 field */
#define WRITE_UINT64_FIELD(fldname) \
	appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(uint64))

/* Write an OID field (don't hard-wire assumption that OID is same as uint) */
#define WRITE_OID_FIELD(fldname) \
	appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(Oid))

/* Write a long-integer field */
#define WRITE_LONG_FIELD(fldname) \
	appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(long))

/* Write a char field (ie, one ascii character) */
#define WRITE_CHAR_FIELD(fldname) \
	appendBinaryStringInfo(str, &node->fldname, 1)

/* Write an enumerated-type field as an integer code */
#define WRITE_ENUM_FIELD(fldname, enumtype) \
	{ int16 en=node->fldname; appendBinaryStringInfo(str, (const char *)&en, sizeof(int16)); }

/* Write a float field --- the format is accepted but ignored (for compat with outfuncs.c)  */
#define WRITE_FLOAT_FIELD(fldname) \
	appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(double))

/* Write a boolean field */
#define WRITE_BOOL_FIELD(fldname) \
	{ \
		char b = node->fldname ? 1 : 0; \
		appendBinaryStringInfo(str, (const char *)&b, 1); }

/* Write a character-string (possibly NULL) varable */
#define WRITE_STRING_VAR(var) \
	{ int slen = var != NULL ? strlen(var) : 0; \
		appendBinaryStringInfo(str, (const char *)&slen, sizeof(int)); \
		if (slen>0) appendBinaryStringInfo(str, var, slen);}

/* Write a character-string (possibly NULL) field */
#define WRITE_STRING_FIELD(fldname)  WRITE_STRING_VAR(node->fldname)

/* Write a parse location field (actually same as INT case) */
#define WRITE_LOCATION_FIELD(fldname) \
	{ appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(int)); }

/*
 * Write a Node field
 *
 * If compiled with GP_SERIALIZATION_DEBUG, write the field name in the
 * serialized form, and check that it matches in the read function
 * (READ_NODE_FIELD in readfast.c). That makes it much easier to debug bugs
 * where the out and read functions are not in sync, as you get an error
 * much earlier, and it can print the field name where the mismatch occurred.
 * It makes the serialized plans much larger, though, so we don't want to do
 * it production.
 */
#ifdef GP_SERIALIZATION_DEBUG
#define WRITE_NODE_FIELD(fldname) \
	do { \
		const char *xx = CppAsString(fldname); \
		appendBinaryStringInfo(str, xx, strlen(xx) + 1); \
		(_outNode(str, node->fldname)); \
	} while (0)
#else
#define WRITE_NODE_FIELD(fldname) \
	(_outNode(str, node->fldname))
#endif


/* Write a bitmapset field */
#define WRITE_BITMAPSET_FIELD(fldname) \
	 _outBitmapset(str, node->fldname)

/* Write a binary field */
#define WRITE_BINARY_FIELD(fldname, sz) \
{ appendBinaryStringInfo(str, (const char *) &node->fldname, (sz)); }

/* Write a bytea field */
#define WRITE_BYTEA_FIELD(fldname) \
	(_outDatum(str, PointerGetDatum(node->fldname), -1, false))

/* Write a dummy field -- value not displayable or copyable */
#define WRITE_DUMMY_FIELD(fldname) \
	{ /*int * dummy = 0; appendBinaryStringInfo(str,(const char *)&dummy, sizeof(int *)) ;*/ }

	/* Read an integer array */
#define WRITE_INT_ARRAY(fldname, count) \
	StaticAssertStmt(sizeof(node->fldname[0]) == sizeof(int32), \
					 "WRITE_INT_ARRAY() used on wrong array type"); \
	if ( (count) > 0 ) \
	{ \
		int i; \
		for(i = 0; i < (count); i++) \
		{ \
			appendBinaryStringInfo(str, (const char *)&node->fldname[i], sizeof(int32)); \
		} \
	}

/* Write a boolean array  */
#define WRITE_BOOL_ARRAY(fldname, count) \
	if ( (count) > 0 ) \
	{ \
		int i; \
		for(i = 0; i < (count); i++) \
		{ \
			char b = node->fldname[i] ? 1 : 0;								\
			appendBinaryStringInfo(str, (const char *)&b, 1); \
		} \
	}

/* Write an Trasnaction ID array  */
#define WRITE_XID_ARRAY(fldname, count) \
	if ( (count) > 0 ) \
	{ \
		int i; \
		for(i = 0; i < (count); i++) \
		{ \
			appendBinaryStringInfo(str, (const char *)&node->fldname[i], sizeof(TransactionId)); \
		} \
	}

/* Write an AttrNumber array  */
#define WRITE_ATTRNUMBER_ARRAY(fldname, count) \
	if ( (count) > 0 ) \
	{ \
		int i; \
		for(i = 0; i < (count); i++) \
		{ \
			appendBinaryStringInfo(str, (const char *)&node->fldname[i], sizeof(AttrNumber)); \
		} \
	}

/* Write an Oid array  */
#define WRITE_OID_ARRAY(fldname, count) \
	if ( (count) > 0 ) \
	{ \
		int i; \
		for(i = 0; i < (count); i++) \
		{ \
			appendBinaryStringInfo(str, (const char *)&node->fldname[i], sizeof(Oid)); \
		} \
	}

static void _outNode(StringInfo str, void *obj);

#define outDatum(str, value, typlen, typbyval) _outDatum(str, value, typlen, typbyval)

static void
_outList(StringInfo str, List *node)
{
	ListCell   *lc;

	if (node == NULL)
	{
		int16 tg = 0;
		appendBinaryStringInfo(str, (const char *)&tg, sizeof(int16));
		return;
	}

	WRITE_NODE_TYPE("");
    WRITE_INT_FIELD(length);

	foreach(lc, node)
	{

		if (IsA(node, List))
		{
			_outNode(str, lfirst(lc));
		}
		else if (IsA(node, IntList))
		{
			int n = lfirst_int(lc);
			appendBinaryStringInfo(str, (const char *)&n, sizeof(int));
		}
		else if (IsA(node, OidList))
		{
			Oid n = lfirst_oid(lc);
			appendBinaryStringInfo(str, (const char *)&n, sizeof(Oid));
		}
	}
}

/*
 * _outBitmapset -
 *	   converts a bitmap set of integers
 *
 * Currently bitmapsets do not appear in any node type that is stored in
 * rules, so there is no support in readfast.c for reading this format.
 */
static void
_outBitmapset(StringInfo str, const Bitmapset *bms)
{
	int			i;
	int			nwords = 0;

	if (bms)
		nwords = bms->nwords;

	appendBinaryStringInfo(str, (char *) &nwords, sizeof(int));
	for (i = 0; i < nwords; i++)
	{
		appendBinaryStringInfo(str, (char *) &bms->words[i], sizeof(bitmapword));
	}

}

/*
 * Print the value of a Datum given its type.
 */
static void
_outDatum(StringInfo str, Datum value, int typlen, bool typbyval)
{
	Size		length;
	char	   *s;

	if (typbyval)
	{
		s = (char *) (&value);
		appendBinaryStringInfo(str, s, sizeof(Datum));
	}
	else
	{
		s = (char *) DatumGetPointer(value);
		if (!PointerIsValid(s))
		{
			length = 0;
			appendBinaryStringInfo(str, (char *)&length, sizeof(Size));
		}
		else
		{
			if (typlen == -1 && VARATT_IS_EXTERNAL_EXPANDED(s))
			{
				ExpandedObjectHeader *eoh = DatumGetEOHP(value);
				Size		resultsize;
				char		*resultptr;

				resultsize = EOH_get_flat_size(eoh);
				resultptr = (char *) palloc(resultsize);
				EOH_flatten_into(eoh, (void *) resultptr, resultsize);
				appendBinaryStringInfo(str, (char *)&resultsize, sizeof(Size));
				appendBinaryStringInfo(str, resultptr, resultsize);
			}
			else
			{
				length = datumGetSize(value, typbyval, typlen);
				appendBinaryStringInfo(str, (char *)&length, sizeof(Size));
				appendBinaryStringInfo(str, s, length);
			}
		}
	}
}

/* Additional includes needed for functions inlined from outfuncs.c */
#include "miscadmin.h"
#include "nodes/extensible.h"
#include "nodes/pathnodes.h"
#include "utils/rel.h"
#include "nodes/altertablenodes.h"

/*
 * Binary write functions for all node types, inlined from outfuncs.c
 * with binary WRITE_* macros active. outfast.c is fully self-contained.
 */
#define booltostr(x)  ((x) ? "true" : "false")



/*
 *	Stuff from plannodes.h
 */

static void
_outPlannedStmt(StringInfo str, const PlannedStmt *node)
{
	WRITE_NODE_TYPE("PLANNEDSTMT");

	WRITE_ENUM_FIELD(commandType, CmdType);
	WRITE_ENUM_FIELD(planGen, PlanGenerator);
	WRITE_UINT64_FIELD(queryId);
	WRITE_BOOL_FIELD(hasReturning);
	WRITE_BOOL_FIELD(hasModifyingCTE);
	WRITE_BOOL_FIELD(canSetTag);
	WRITE_BOOL_FIELD(transientPlan);
	WRITE_BOOL_FIELD(oneoffPlan);
	WRITE_OID_FIELD(simplyUpdatableRel);
	WRITE_BOOL_FIELD(dependsOnRole);
	WRITE_BOOL_FIELD(parallelModeNeeded);
	WRITE_INT_FIELD(jitFlags);
	WRITE_NODE_FIELD(planTree);
	WRITE_NODE_FIELD(rtable);
	WRITE_NODE_FIELD(permInfos);
	WRITE_NODE_FIELD(resultRelations);
	WRITE_NODE_FIELD(appendRelations);
	WRITE_NODE_FIELD(subplans);
	WRITE_BITMAPSET_FIELD(rewindPlanIDs);
	WRITE_NODE_FIELD(rowMarks);
	WRITE_NODE_FIELD(relationOids);
	/*
	 * Don't serialize invalItems when dispatching. The TIDs of the invalidated items wouldn't
	 * make sense in segments.
	 */
	WRITE_NODE_FIELD(paramExecTypes);
	WRITE_NODE_FIELD(utilityStmt);
	WRITE_LOCATION_FIELD(stmt_location);
	WRITE_INT_FIELD(stmt_len);

	WRITE_INT_ARRAY(subplan_sliceIds, list_length(node->subplans));

	WRITE_INT_FIELD(numSlices);
	for (int i = 0; i < node->numSlices; i++)
	{
		WRITE_INT_FIELD(slices[i].sliceIndex);
		WRITE_INT_FIELD(slices[i].parentIndex);
		WRITE_INT_FIELD(slices[i].gangType);
		WRITE_INT_FIELD(slices[i].numsegments);
		WRITE_INT_FIELD(slices[i].parallel_workers);
		WRITE_INT_FIELD(slices[i].segindex);
		WRITE_BOOL_FIELD(slices[i].directDispatch.isDirectDispatch);
		WRITE_NODE_FIELD(slices[i].directDispatch.contentIds);
	}

	WRITE_BITMAPSET_FIELD(rewindPlanIDs);

	WRITE_NODE_FIELD(intoPolicy);

	WRITE_UINT64_FIELD(query_mem);

	WRITE_NODE_FIELD(intoClause);
	WRITE_NODE_FIELD(copyIntoClause);
	WRITE_NODE_FIELD(refreshClause);
	WRITE_INT_FIELD(metricsQueryType);
	WRITE_NODE_FIELD(extensionContext);

}

/*
 * print the basic stuff of all nodes that inherit from Plan
 */
static void
_outPlanInfo(StringInfo str, const Plan *node)
{
	WRITE_FLOAT_FIELD(startup_cost);
	WRITE_FLOAT_FIELD(total_cost);
	WRITE_FLOAT_FIELD(plan_rows);
	WRITE_INT_FIELD(plan_width);
	WRITE_BOOL_FIELD(parallel_aware);
	WRITE_BOOL_FIELD(parallel_safe);
	WRITE_BOOL_FIELD(async_capable);
	WRITE_INT_FIELD(plan_node_id);
	WRITE_NODE_FIELD(targetlist);
	WRITE_NODE_FIELD(qual);
	WRITE_NODE_FIELD(lefttree);
	WRITE_NODE_FIELD(righttree);
	WRITE_NODE_FIELD(initPlan);
	WRITE_BITMAPSET_FIELD(extParam);
	WRITE_BITMAPSET_FIELD(allParam);
	/* 'flow' is only needed during planning. */

	WRITE_UINT64_FIELD(operatorMemKB);
}

/*
 * print the basic stuff of all nodes that inherit from Scan
 */
static void
_outScanInfo(StringInfo str, const Scan *node)
{
	_outPlanInfo(str, (const Plan *) node);

	WRITE_UINT_FIELD(scanrelid);
}

/*
 * print the basic stuff of all nodes that inherit from Join
 */
static void
_outJoinPlanInfo(StringInfo str, const Join *node)
{
	_outPlanInfo(str, (const Plan *) node);

	WRITE_BOOL_FIELD(prefetch_inner);
	WRITE_BOOL_FIELD(prefetch_joinqual);
	WRITE_BOOL_FIELD(prefetch_qual);

	WRITE_ENUM_FIELD(jointype, JoinType);
	WRITE_BOOL_FIELD(inner_unique);
	WRITE_NODE_FIELD(joinqual);
}


static void
_outPlan(StringInfo str, const Plan *node)
{
	WRITE_NODE_TYPE("PLAN");

	_outPlanInfo(str, (const Plan *) node);
}

static void
_outResult(StringInfo str, const Result *node)
{
	WRITE_NODE_TYPE("RESULT");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_NODE_FIELD(resconstantqual);

	WRITE_INT_FIELD(numHashFilterCols);
	WRITE_ATTRNUMBER_ARRAY(hashFilterColIdx, node->numHashFilterCols);
	WRITE_OID_ARRAY(hashFilterFuncs, node->numHashFilterCols);
}

static void
_outProjectSet(StringInfo str, const ProjectSet *node)
{
	WRITE_NODE_TYPE("PROJECTSET");

	_outPlanInfo(str, (const Plan *) node);
}

static void
_outModifyTable(StringInfo str, const ModifyTable *node)
{
	WRITE_NODE_TYPE("MODIFYTABLE");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_ENUM_FIELD(operation, CmdType);
	WRITE_BOOL_FIELD(canSetTag);
	WRITE_UINT_FIELD(nominalRelation);
	WRITE_UINT_FIELD(rootRelation);
	WRITE_BOOL_FIELD(partColsUpdated);
	WRITE_BOOL_FIELD(splitUpdate);
	WRITE_NODE_FIELD(resultRelations);
	WRITE_NODE_FIELD(updateColnosLists);
	WRITE_NODE_FIELD(withCheckOptionLists);
	WRITE_NODE_FIELD(returningLists);
	WRITE_NODE_FIELD(fdwPrivLists);
	WRITE_BITMAPSET_FIELD(fdwDirectModifyPlans);
	WRITE_NODE_FIELD(rowMarks);
	WRITE_INT_FIELD(epqParam);
	WRITE_ENUM_FIELD(onConflictAction, OnConflictAction);
	WRITE_NODE_FIELD(arbiterIndexes);
	WRITE_NODE_FIELD(onConflictSet);
	WRITE_NODE_FIELD(onConflictCols);
	WRITE_NODE_FIELD(onConflictWhere);
	WRITE_UINT_FIELD(exclRelRTI);
	WRITE_NODE_FIELD(exclRelTlist);
	WRITE_BOOL_FIELD(forceTupleRouting);
	WRITE_NODE_FIELD(mergeActionLists);
}

static void
_outAppend(StringInfo str, const Append *node)
{
	WRITE_NODE_TYPE("APPEND");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_BITMAPSET_FIELD(apprelids);
	WRITE_NODE_FIELD(appendplans);
	WRITE_INT_FIELD(nasyncplans);
	WRITE_INT_FIELD(first_partial_plan);
	WRITE_NODE_FIELD(part_prune_info);
	WRITE_NODE_FIELD(join_prune_paramids);
}

static void
_outMergeAppend(StringInfo str, const MergeAppend *node)
{
	WRITE_NODE_TYPE("MERGEAPPEND");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_BITMAPSET_FIELD(apprelids);
	WRITE_NODE_FIELD(mergeplans);
	WRITE_INT_FIELD(numCols);
	WRITE_ATTRNUMBER_ARRAY(sortColIdx, node->numCols);
	WRITE_OID_ARRAY(sortOperators, node->numCols);
	WRITE_OID_ARRAY(collations, node->numCols);
	WRITE_BOOL_ARRAY(nullsFirst, node->numCols);
	WRITE_NODE_FIELD(part_prune_info);
	WRITE_NODE_FIELD(join_prune_paramids);
}

static void
_outRecursiveUnion(StringInfo str, const RecursiveUnion *node)
{
	WRITE_NODE_TYPE("RECURSIVEUNION");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_INT_FIELD(wtParam);
	WRITE_INT_FIELD(numCols);
	WRITE_ATTRNUMBER_ARRAY(dupColIdx, node->numCols);
	WRITE_OID_ARRAY(dupOperators, node->numCols);
	WRITE_OID_ARRAY(dupCollations, node->numCols);
	WRITE_LONG_FIELD(numGroups);
}

static void
_outBitmapAnd(StringInfo str, const BitmapAnd *node)
{
	WRITE_NODE_TYPE("BITMAPAND");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_NODE_FIELD(bitmapplans);
}

static void
_outBitmapOr(StringInfo str, const BitmapOr *node)
{
	WRITE_NODE_TYPE("BITMAPOR");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_BOOL_FIELD(isshared);
	WRITE_NODE_FIELD(bitmapplans);
}

static void
_outGather(StringInfo str, const Gather *node)
{
	WRITE_NODE_TYPE("GATHER");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_INT_FIELD(num_workers);
	WRITE_INT_FIELD(rescan_param);
	WRITE_BOOL_FIELD(single_copy);
	WRITE_BOOL_FIELD(invisible);
	WRITE_BITMAPSET_FIELD(initParam);
}

static void
_outGatherMerge(StringInfo str, const GatherMerge *node)
{
	WRITE_NODE_TYPE("GATHERMERGE");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_INT_FIELD(num_workers);
	WRITE_INT_FIELD(rescan_param);
	WRITE_INT_FIELD(numCols);
	WRITE_ATTRNUMBER_ARRAY(sortColIdx, node->numCols);
	WRITE_OID_ARRAY(sortOperators, node->numCols);
	WRITE_OID_ARRAY(collations, node->numCols);
	WRITE_BOOL_ARRAY(nullsFirst, node->numCols);
	WRITE_BITMAPSET_FIELD(initParam);
}

static void
_outScan(StringInfo str, const Scan *node)
{
	WRITE_NODE_TYPE("SCAN");

	_outScanInfo(str, node);
}

static void
_outSeqScan(StringInfo str, const SeqScan *node)
{
	WRITE_NODE_TYPE("SEQSCAN");

	_outScanInfo(str, (const Scan *) node);
}

static void
_outDynamicSeqScan(StringInfo str, const DynamicSeqScan *node)
{
	WRITE_NODE_TYPE("DYNAMICSEQSCAN");

	_outScanInfo(str, (Scan *)node);
	WRITE_NODE_FIELD(partOids);
	WRITE_NODE_FIELD(part_prune_info);
	WRITE_NODE_FIELD(join_prune_paramids);
}

static void
_outSampleScan(StringInfo str, const SampleScan *node)
{
	WRITE_NODE_TYPE("SAMPLESCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_NODE_FIELD(tablesample);
}

static void
outIndexScanFields(StringInfo str, const IndexScan *node)
{
	_outScanInfo(str, (const Scan *) node);

	WRITE_OID_FIELD(indexid);
	WRITE_NODE_FIELD(indexqual);
	WRITE_NODE_FIELD(indexqualorig);
	WRITE_NODE_FIELD(indexorderby);
	WRITE_NODE_FIELD(indexorderbyorig);
	WRITE_NODE_FIELD(indexorderbyops);
	WRITE_ENUM_FIELD(indexorderdir, ScanDirection);
}

static void
_outIndexScan(StringInfo str, const IndexScan *node)
{
	WRITE_NODE_TYPE("INDEXSCAN");

	outIndexScanFields(str, node);
}

static void
outIndexOnlyScanFields(StringInfo str, const IndexOnlyScan *node)
{
	_outScanInfo(str, (const Scan *) node);

	WRITE_OID_FIELD(indexid);
	WRITE_NODE_FIELD(indexqual);
	WRITE_NODE_FIELD(recheckqual);
	WRITE_NODE_FIELD(indexorderby);
	WRITE_NODE_FIELD(indextlist);
	WRITE_ENUM_FIELD(indexorderdir, ScanDirection);
}

static void
_outIndexOnlyScan(StringInfo str, const IndexOnlyScan *node)
{
	WRITE_NODE_TYPE("INDEXONLYSCAN");

	outIndexOnlyScanFields(str, node);
}

static void
_outDynamicIndexScan(StringInfo str, const DynamicIndexScan *node)
{
	WRITE_NODE_TYPE("DYNAMICINDEXSCAN");

	outIndexScanFields(str, &node->indexscan);
	WRITE_NODE_FIELD(partOids);
	WRITE_NODE_FIELD(part_prune_info);
	WRITE_NODE_FIELD(join_prune_paramids);
}

static void
_outDynamicIndexOnlyScan(StringInfo str, const DynamicIndexOnlyScan *node)
{
	WRITE_NODE_TYPE("DYNAMICINDEXONLYSCAN");

	outIndexOnlyScanFields(str, &node->indexscan);
	WRITE_NODE_FIELD(partOids);
	WRITE_NODE_FIELD(part_prune_info);
	WRITE_NODE_FIELD(join_prune_paramids);
}

static void
_outBitmapIndexScanFields(StringInfo str, const BitmapIndexScan *node)
{
	_outScanInfo(str, (Scan *) node);

	WRITE_OID_FIELD(indexid);
	WRITE_BOOL_FIELD(isshared);
	WRITE_NODE_FIELD(indexqual);
	WRITE_NODE_FIELD(indexqualorig);
}

static void
_outBitmapIndexScan(StringInfo str, const BitmapIndexScan *node)
{
	WRITE_NODE_TYPE("BITMAPINDEXSCAN");

	_outBitmapIndexScanFields(str, node);
}

static void
_outDynamicBitmapIndexScan(StringInfo str, const DynamicBitmapIndexScan *node)
{
	WRITE_NODE_TYPE("DYNAMICBITMAPINDEXSCAN");

	_outBitmapIndexScanFields(str, &node->biscan);
}

static void
outBitmapHeapScanFields(StringInfo str, const BitmapHeapScan *node)
{
	_outScanInfo(str, (const Scan *) node);

	WRITE_NODE_FIELD(bitmapqualorig);
}

static void
_outBitmapHeapScan(StringInfo str, const BitmapHeapScan *node)
{
	WRITE_NODE_TYPE("BITMAPHEAPSCAN");

	outBitmapHeapScanFields(str, node);
}

static void
_outDynamicBitmapHeapScan(StringInfo str, const DynamicBitmapHeapScan *node)
{
	WRITE_NODE_TYPE("DYNAMICBITMAPHEAPSCAN");

	outBitmapHeapScanFields(str, &node->bitmapheapscan);
	WRITE_NODE_FIELD(partOids);
	WRITE_NODE_FIELD(part_prune_info);
	WRITE_NODE_FIELD(join_prune_paramids);
}

static void
_outTidScan(StringInfo str, const TidScan *node)
{
	WRITE_NODE_TYPE("TIDSCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_NODE_FIELD(tidquals);
}

static void
_outSubqueryScan(StringInfo str, const SubqueryScan *node)
{
	WRITE_NODE_TYPE("SUBQUERYSCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_NODE_FIELD(subplan);
}

static void
_outFunctionScan(StringInfo str, const FunctionScan *node)
{
	WRITE_NODE_TYPE("FUNCTIONSCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_NODE_FIELD(functions);
	WRITE_BOOL_FIELD(funcordinality);
	WRITE_NODE_FIELD(param);
	WRITE_BOOL_FIELD(resultInTupleStore);
	WRITE_INT_FIELD(initplanId);
}

static void
_outTableFuncScan(StringInfo str, const TableFuncScan *node)
{
	WRITE_NODE_TYPE("TABLEFUNCSCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_NODE_FIELD(tablefunc);
}

static void
_outValuesScan(StringInfo str, const ValuesScan *node)
{
	WRITE_NODE_TYPE("VALUESSCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_NODE_FIELD(values_lists);
}

static void
_outCteScan(StringInfo str, const CteScan *node)
{
	WRITE_NODE_TYPE("CTESCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_INT_FIELD(ctePlanId);
	WRITE_INT_FIELD(cteParam);
}

static void
_outNamedTuplestoreScan(StringInfo str, const NamedTuplestoreScan *node)
{
	WRITE_NODE_TYPE("NAMEDTUPLESTORESCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_STRING_FIELD(enrname);
}

static void
_outWorkTableScan(StringInfo str, const WorkTableScan *node)
{
	WRITE_NODE_TYPE("WORKTABLESCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_INT_FIELD(wtParam);
}

static void
outForeignScanFields(StringInfo str, const ForeignScan *node)
{
	_outScanInfo(str, (const Scan *) node);

	WRITE_ENUM_FIELD(operation, CmdType);
	WRITE_UINT_FIELD(resultRelation);
	WRITE_OID_FIELD(fs_server);
	WRITE_NODE_FIELD(fdw_exprs);
	WRITE_NODE_FIELD(fdw_private);
	WRITE_NODE_FIELD(fdw_scan_tlist);
	WRITE_NODE_FIELD(fdw_recheck_quals);
	WRITE_BITMAPSET_FIELD(fs_relids);
	WRITE_BOOL_FIELD(fsSystemCol);
}

static void
_outForeignScan(StringInfo str, const ForeignScan *node)
{
	WRITE_NODE_TYPE("FOREIGNSCAN");

	outForeignScanFields(str, node);
}

static void
_outDynamicForeignScan(StringInfo str, const DynamicForeignScan *node)
{
	WRITE_NODE_TYPE("DYNAMICFOREIGNSCAN");

	outForeignScanFields(str, &node->foreignscan);
	WRITE_NODE_FIELD(partOids);
	WRITE_NODE_FIELD(part_prune_info);
	WRITE_NODE_FIELD(join_prune_paramids);
	WRITE_NODE_FIELD(fdw_private_list);
}


static void
_outJoin(StringInfo str, const Join *node)
{
	WRITE_NODE_TYPE("JOIN");

	_outJoinPlanInfo(str, (const Join *) node);
}

static void
_outNestLoop(StringInfo str, const NestLoop *node)
{
	WRITE_NODE_TYPE("NESTLOOP");

	_outJoinPlanInfo(str, (const Join *) node);

	WRITE_NODE_FIELD(nestParams);

	WRITE_BOOL_FIELD(shared_outer);
	WRITE_BOOL_FIELD(singleton_outer); /*CDB-OLAP*/
}

static void
_outMergeJoin(StringInfo str, const MergeJoin *node)
{
	int			numCols;

	WRITE_NODE_TYPE("MERGEJOIN");

	_outJoinPlanInfo(str, (const Join *) node);

	WRITE_BOOL_FIELD(skip_mark_restore);
	WRITE_NODE_FIELD(mergeclauses);

	numCols = list_length(node->mergeclauses);

	WRITE_OID_ARRAY(mergeFamilies, numCols);
	WRITE_OID_ARRAY(mergeCollations, numCols);
	WRITE_INT_ARRAY(mergeStrategies, numCols);
	WRITE_BOOL_ARRAY(mergeNullsFirst, numCols);
    WRITE_BOOL_FIELD(unique_outer);
}

static void
_outHashJoin(StringInfo str, const HashJoin *node)
{
	WRITE_NODE_TYPE("HASHJOIN");

	_outJoinPlanInfo(str, (const Join *) node);

	WRITE_NODE_FIELD(hashclauses);
	WRITE_NODE_FIELD(hashoperators);
	WRITE_NODE_FIELD(hashcollations);
	WRITE_NODE_FIELD(hashkeys);
	WRITE_NODE_FIELD(hashqualclauses);
	WRITE_BOOL_FIELD(batch0_barrier);
	WRITE_BOOL_FIELD(outer_motionhazard);
}

static void
_outAgg(StringInfo str, const Agg *node)
{
	WRITE_NODE_TYPE("AGG");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_ENUM_FIELD(aggstrategy, AggStrategy);
	WRITE_ENUM_FIELD(aggsplit, AggSplit);
	WRITE_INT_FIELD(numCols);
	WRITE_ATTRNUMBER_ARRAY(grpColIdx, node->numCols);
	WRITE_OID_ARRAY(grpOperators, node->numCols);
	WRITE_OID_ARRAY(grpCollations, node->numCols);
	WRITE_LONG_FIELD(numGroups);
	WRITE_UINT64_FIELD(transitionSpace);
	WRITE_BITMAPSET_FIELD(aggParams);
	WRITE_NODE_FIELD(groupingSets);
	WRITE_NODE_FIELD(chain);
	WRITE_BOOL_FIELD(streaming);
	WRITE_UINT_FIELD(agg_expr_id);
}

static void
_outWindowAgg(StringInfo str, const WindowAgg *node)
{
	WRITE_NODE_TYPE("WINDOWAGG");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_UINT_FIELD(winref);
	WRITE_INT_FIELD(partNumCols);
	WRITE_ATTRNUMBER_ARRAY(partColIdx, node->partNumCols);
	WRITE_OID_ARRAY(partOperators, node->partNumCols);
	WRITE_OID_ARRAY(partCollations, node->partNumCols);
	WRITE_INT_FIELD(ordNumCols);
	WRITE_ATTRNUMBER_ARRAY(ordColIdx, node->ordNumCols);
	WRITE_OID_ARRAY(ordOperators, node->ordNumCols);
	WRITE_OID_ARRAY(ordCollations, node->ordNumCols);
	WRITE_INT_FIELD(firstOrderCol);
	WRITE_OID_FIELD(firstOrderCmpOperator);
	WRITE_BOOL_FIELD(firstOrderNullsFirst);
	WRITE_INT_FIELD(frameOptions);
	WRITE_NODE_FIELD(startOffset);
	WRITE_NODE_FIELD(endOffset);
	WRITE_NODE_FIELD(runCondition);
	WRITE_NODE_FIELD(runConditionOrig);
	WRITE_OID_FIELD(startInRangeFunc);
	WRITE_OID_FIELD(endInRangeFunc);
	WRITE_OID_FIELD(inRangeColl);
	WRITE_BOOL_FIELD(inRangeAsc);
	WRITE_BOOL_FIELD(inRangeNullsFirst);
	WRITE_BOOL_FIELD(topWindow);
}

static void
_outWindowHashAgg(StringInfo str, const WindowHashAgg *node)
{
	WRITE_NODE_TYPE("WINDOWHASHAGG");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_UINT_FIELD(winref);
	WRITE_INT_FIELD(partNumCols);
	WRITE_ATTRNUMBER_ARRAY(partColIdx, node->partNumCols);
	WRITE_OID_ARRAY(partOperators, node->partNumCols);
	WRITE_OID_ARRAY(partCollations, node->partNumCols);
	WRITE_INT_FIELD(ordNumCols);
	WRITE_ATTRNUMBER_ARRAY(ordColIdx, node->ordNumCols);
	WRITE_OID_ARRAY(ordOperators, node->ordNumCols);
	WRITE_OID_ARRAY(ordCollations, node->ordNumCols);
	WRITE_BOOL_ARRAY(ordNullsFirst, node->ordNumCols);
	WRITE_INT_FIELD(frameOptions);
	WRITE_NODE_FIELD(startOffset);
	WRITE_NODE_FIELD(endOffset);
	WRITE_OID_FIELD(startInRangeFunc);
	WRITE_OID_FIELD(endInRangeFunc);
	WRITE_OID_FIELD(inRangeColl);
	WRITE_BOOL_FIELD(inRangeAsc);
	WRITE_BOOL_FIELD(inRangeNullsFirst);
}

static void
_outGroup(StringInfo str, const Group *node)
{
	WRITE_NODE_TYPE("GROUP");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_INT_FIELD(numCols);
	WRITE_ATTRNUMBER_ARRAY(grpColIdx, node->numCols);
	WRITE_OID_ARRAY(grpOperators, node->numCols);
	WRITE_OID_ARRAY(grpCollations, node->numCols);
}

static void
_outMaterial(StringInfo str, const Material *node)
{
	WRITE_NODE_TYPE("MATERIAL");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_BOOL_FIELD(cdb_strict);
	WRITE_BOOL_FIELD(cdb_shield_child_from_rescans);
}

static void
_outSortInfo(StringInfo str, const Sort *node)
{
	_outPlanInfo(str, (const Plan *) node);

	WRITE_INT_FIELD(numCols);
	WRITE_ATTRNUMBER_ARRAY(sortColIdx, node->numCols);
	WRITE_OID_ARRAY(sortOperators, node->numCols);
	WRITE_OID_ARRAY(collations, node->numCols);
	WRITE_BOOL_ARRAY(nullsFirst, node->numCols);
}

static void
_outSort(StringInfo str, const Sort *node)
{
	WRITE_NODE_TYPE("SORT");

	_outSortInfo(str, node);
}

static void
_outIncrementalSort(StringInfo str, const IncrementalSort *node)
{
	WRITE_NODE_TYPE("INCREMENTALSORT");

	_outSortInfo(str, (const Sort *) node);

	WRITE_INT_FIELD(nPresortedCols);
}

static void
_outUnique(StringInfo str, const Unique *node)
{
	WRITE_NODE_TYPE("UNIQUE");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_INT_FIELD(numCols);
	WRITE_ATTRNUMBER_ARRAY(uniqColIdx, node->numCols);
	WRITE_OID_ARRAY(uniqOperators, node->numCols);
	WRITE_OID_ARRAY(uniqCollations, node->numCols);
}

static void
_outHash(StringInfo str, const Hash *node)
{
	WRITE_NODE_TYPE("HASH");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_NODE_FIELD(hashkeys);
	WRITE_OID_FIELD(skewTable);
	WRITE_INT_FIELD(skewColumn);
	WRITE_BOOL_FIELD(skewInherit);
	WRITE_FLOAT_FIELD(rows_total);
	WRITE_BOOL_FIELD(rescannable);          /*CDB*/
	WRITE_BOOL_FIELD(sync_barrier);
}

static void
_outSetOp(StringInfo str, const SetOp *node)
{
	WRITE_NODE_TYPE("SETOP");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_ENUM_FIELD(cmd, SetOpCmd);
	WRITE_ENUM_FIELD(strategy, SetOpStrategy);
	WRITE_INT_FIELD(numCols);
	WRITE_ATTRNUMBER_ARRAY(dupColIdx, node->numCols);
	WRITE_OID_ARRAY(dupOperators, node->numCols);
	WRITE_OID_ARRAY(dupCollations, node->numCols);
	WRITE_INT_FIELD(flagColIdx);
	WRITE_INT_FIELD(firstFlag);
	WRITE_LONG_FIELD(numGroups);
}

static void
_outLockRows(StringInfo str, const LockRows *node)
{
	WRITE_NODE_TYPE("LOCKROWS");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_NODE_FIELD(rowMarks);
	WRITE_INT_FIELD(epqParam);
}

static void
_outRuntimeFilter(StringInfo str, const RuntimeFilter *node)
{
	WRITE_NODE_TYPE("RUNTIME_FILTER");

	_outPlanInfo(str, (const Plan *) node);
}

static void
_outLimit(StringInfo str, const Limit *node)
{
	WRITE_NODE_TYPE("LIMIT");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_NODE_FIELD(limitOffset);
	WRITE_NODE_FIELD(limitCount);
	WRITE_ENUM_FIELD(limitOption, LimitOption);
	WRITE_INT_FIELD(uniqNumCols);
	WRITE_ATTRNUMBER_ARRAY(uniqColIdx, node->uniqNumCols);
	WRITE_OID_ARRAY(uniqOperators, node->uniqNumCols);
	WRITE_OID_ARRAY(uniqCollations, node->uniqNumCols);
}

static void
_outOpExpr(StringInfo str, const OpExpr *node)
{
	WRITE_NODE_TYPE("OPEXPR");

	WRITE_OID_FIELD(opno);
	WRITE_OID_FIELD(opfuncid);
	WRITE_OID_FIELD(opresulttype);
	WRITE_BOOL_FIELD(opretset);
	WRITE_OID_FIELD(opcollid);
	WRITE_OID_FIELD(inputcollid);
	WRITE_NODE_FIELD(args);
	WRITE_LOCATION_FIELD(location);
}

static void
_outCreateStmtInfo(StringInfo str, const CreateStmt *node)
{
	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(tableElts);
	WRITE_NODE_FIELD(inhRelations);
	WRITE_NODE_FIELD(partspec);
	WRITE_NODE_FIELD(partbound);
	WRITE_NODE_FIELD(ofTypename);
	WRITE_NODE_FIELD(constraints);
	WRITE_NODE_FIELD(options);
	WRITE_ENUM_FIELD(oncommit, OnCommitAction);
	WRITE_STRING_FIELD(tablespacename);
	WRITE_STRING_FIELD(accessMethod);
	WRITE_BOOL_FIELD(if_not_exists);
	WRITE_ENUM_FIELD(origin, CreateStmtOrigin);

	WRITE_NODE_FIELD(distributedBy);
	WRITE_NODE_FIELD(partitionBy);
	WRITE_CHAR_FIELD(relKind);
	WRITE_OID_FIELD(ownerid);
	WRITE_BOOL_FIELD(buildAoBlkdir);
	WRITE_NODE_FIELD(attr_encodings);
	WRITE_BOOL_FIELD(isCtas);
	WRITE_NODE_FIELD(intoQuery);
	WRITE_NODE_FIELD(intoPolicy);

	WRITE_NODE_FIELD(part_idx_oids);
	WRITE_NODE_FIELD(part_idx_names);
	WRITE_NODE_FIELD(tags);

	/*
	 * Some extra checks to make sure we didn't get lost
	 * during serialization/deserialization
	 */
	Assert(node->relKind != 0);
	Assert(node->oncommit <= ONCOMMIT_DROP);
}

static void
_outIndexStmt(StringInfo str, const IndexStmt *node)
{
	WRITE_NODE_TYPE("INDEXSTMT");

	WRITE_STRING_FIELD(idxname);
	WRITE_NODE_FIELD(relation);
	WRITE_OID_FIELD(relationOid);
	WRITE_STRING_FIELD(accessMethod);
	WRITE_STRING_FIELD(tableSpace);
	WRITE_NODE_FIELD(indexParams);
	WRITE_NODE_FIELD(indexIncludingParams);
	WRITE_NODE_FIELD(options);
	WRITE_NODE_FIELD(whereClause);
	WRITE_NODE_FIELD(excludeOpNames);
	WRITE_STRING_FIELD(idxcomment);
	WRITE_OID_FIELD(indexOid);
	WRITE_OID_FIELD(oldNumber);
	WRITE_UINT_FIELD(oldCreateSubid);
	WRITE_UINT_FIELD(oldFirstRelfilelocatorSubid);
	WRITE_BOOL_FIELD(unique);
	WRITE_BOOL_FIELD(nulls_not_distinct);
	WRITE_BOOL_FIELD(primary);
	WRITE_BOOL_FIELD(isconstraint);
	WRITE_BOOL_FIELD(deferrable);
	WRITE_BOOL_FIELD(initdeferred);
	WRITE_BOOL_FIELD(transformed);
	WRITE_BOOL_FIELD(concurrent);
	WRITE_BOOL_FIELD(if_not_exists);
	WRITE_BOOL_FIELD(reset_default_tblspc);
	WRITE_ENUM_FIELD(concurrentlyPhase, IndexConcurrentlyPhase);
	WRITE_OID_FIELD(indexRelationOid);
	WRITE_NODE_FIELD(tags);
}

static void
_outColumnDef(StringInfo str, const ColumnDef *node)
{
	WRITE_NODE_TYPE("COLUMNDEF");

	WRITE_STRING_FIELD(colname);
	WRITE_NODE_FIELD(typeName);
	WRITE_STRING_FIELD(compression);
	WRITE_INT_FIELD(inhcount);
	WRITE_BOOL_FIELD(is_local);
	WRITE_BOOL_FIELD(is_not_null);
	WRITE_BOOL_FIELD(is_from_type);
	WRITE_INT_FIELD(attnum);
	WRITE_INT_FIELD(storage);
	WRITE_STRING_FIELD(storage_name);
	WRITE_NODE_FIELD(raw_default);
	WRITE_NODE_FIELD(cooked_default);

	WRITE_BOOL_FIELD(hasCookedMissingVal);
	WRITE_BOOL_FIELD(missingIsNull);
	if (node->hasCookedMissingVal && !node->missingIsNull)
		outDatum(str, node->missingVal, -1, false);

	WRITE_CHAR_FIELD(identity);
	WRITE_NODE_FIELD(identitySequence);
	WRITE_CHAR_FIELD(generated);
	WRITE_NODE_FIELD(collClause);
	WRITE_OID_FIELD(collOid);
	WRITE_NODE_FIELD(constraints);
	WRITE_NODE_FIELD(encoding);
	WRITE_NODE_FIELD(fdwoptions);
	WRITE_LOCATION_FIELD(location);
}

static void
_outRangeTblEntry(StringInfo str, const RangeTblEntry *node)
{
	WRITE_NODE_TYPE("RTE");

	/* put alias + eref first to make dump more legible */
	WRITE_NODE_FIELD(alias);
	WRITE_NODE_FIELD(eref);
	WRITE_ENUM_FIELD(rtekind, RTEKind);
	WRITE_BOOL_FIELD(relisivm);

	switch (node->rtekind)
	{
		case RTE_RELATION:
			WRITE_OID_FIELD(relid);
			WRITE_CHAR_FIELD(relkind);
			WRITE_INT_FIELD(rellockmode);
			WRITE_NODE_FIELD(tablesample);
			WRITE_UINT_FIELD(perminfoindex);
			break;
		case RTE_SUBQUERY:
			WRITE_NODE_FIELD(subquery);
			WRITE_BOOL_FIELD(security_barrier);
			WRITE_OID_FIELD(relid);
			WRITE_CHAR_FIELD(relkind);
			WRITE_INT_FIELD(rellockmode);
			WRITE_UINT_FIELD(perminfoindex);
			break;
		case RTE_JOIN:
			WRITE_ENUM_FIELD(jointype, JoinType);
			WRITE_INT_FIELD(joinmergedcols);
			WRITE_NODE_FIELD(joinaliasvars);
			WRITE_NODE_FIELD(joinleftcols);
			WRITE_NODE_FIELD(joinrightcols);
			WRITE_NODE_FIELD(join_using_alias);
			break;
		case RTE_FUNCTION:
			WRITE_NODE_FIELD(functions);
			WRITE_BOOL_FIELD(funcordinality);
			break;
		case RTE_TABLEFUNCTION:
			WRITE_NODE_FIELD(subquery);
			WRITE_NODE_FIELD(functions);
			WRITE_BOOL_FIELD(funcordinality);
			break;
		case RTE_TABLEFUNC:
			WRITE_NODE_FIELD(tablefunc);
			break;
		case RTE_VALUES:
			WRITE_NODE_FIELD(values_lists);
			WRITE_NODE_FIELD(coltypes);
			WRITE_NODE_FIELD(coltypmods);
			WRITE_NODE_FIELD(colcollations);
			break;
		case RTE_CTE:
			WRITE_STRING_FIELD(ctename);
			WRITE_UINT_FIELD(ctelevelsup);
			WRITE_BOOL_FIELD(self_reference);
			WRITE_NODE_FIELD(coltypes);
			WRITE_NODE_FIELD(coltypmods);
			WRITE_NODE_FIELD(colcollations);
			break;
		case RTE_NAMEDTUPLESTORE:
			WRITE_STRING_FIELD(enrname);
			WRITE_FLOAT_FIELD(enrtuples);
			WRITE_OID_FIELD(relid);
			WRITE_NODE_FIELD(coltypes);
			WRITE_NODE_FIELD(coltypmods);
			WRITE_NODE_FIELD(colcollations);
			break;
		case RTE_RESULT:
			/* no extra fields */
			break;
        case RTE_VOID:                                                  /*CDB*/
            break;
		default:
			elog(ERROR, "unrecognized RTE kind: %d", (int) node->rtekind);
			break;
	}

	WRITE_BOOL_FIELD(lateral);
	WRITE_BOOL_FIELD(inh);
	WRITE_BOOL_FIELD(inFromCl);
	WRITE_NODE_FIELD(securityQuals);

	WRITE_BOOL_FIELD(forceDistRandom);
}

static void
_outRangeTblFunction(StringInfo str, const RangeTblFunction *node)
{
	WRITE_NODE_TYPE("RANGETBLFUNCTION");

	WRITE_NODE_FIELD(funcexpr);
	WRITE_INT_FIELD(funccolcount);
	WRITE_NODE_FIELD(funccolnames);
	WRITE_NODE_FIELD(funccoltypes);
	WRITE_NODE_FIELD(funccoltypmods);
	WRITE_NODE_FIELD(funccolcollations);
	/* funcuserdata is only serialized in binary out/read functions */
	WRITE_BYTEA_FIELD(funcuserdata);
	WRITE_BITMAPSET_FIELD(funcparams);
}

static void
_outConstraint(StringInfo str, const Constraint *node)
{
	WRITE_NODE_TYPE("CONSTRAINT");

	WRITE_ENUM_FIELD(contype, ConstrType);
	/* name, or NULL if unnamed */
	WRITE_STRING_FIELD(conname);
	WRITE_BOOL_FIELD(deferrable);
	WRITE_BOOL_FIELD(initdeferred);
	WRITE_LOCATION_FIELD(location);

	WRITE_BOOL_FIELD(is_no_inherit);
	WRITE_NODE_FIELD(raw_expr);
	WRITE_STRING_FIELD(cooked_expr);
	WRITE_CHAR_FIELD(generated_when);
	WRITE_BOOL_FIELD(nulls_not_distinct);

	WRITE_NODE_FIELD(keys);
	WRITE_NODE_FIELD(including);

	WRITE_NODE_FIELD(exclusions);

	WRITE_NODE_FIELD(options);
	WRITE_STRING_FIELD(indexname);
	WRITE_STRING_FIELD(indexspace);
	WRITE_BOOL_FIELD(reset_default_tblspc);

	WRITE_STRING_FIELD(access_method);
	WRITE_NODE_FIELD(where_clause);

	WRITE_NODE_FIELD(pktable);
	WRITE_NODE_FIELD(fk_attrs);
	WRITE_NODE_FIELD(pk_attrs);
	WRITE_CHAR_FIELD(fk_matchtype);
	WRITE_CHAR_FIELD(fk_upd_action);
	WRITE_CHAR_FIELD(fk_del_action);
	WRITE_NODE_FIELD(old_conpfeqop);
	WRITE_OID_FIELD(old_pktable_oid);

	WRITE_BOOL_FIELD(skip_validation);
	WRITE_BOOL_FIELD(initially_valid);
}


static void
_outCreateDirectoryTableStmt(StringInfo str, const CreateDirectoryTableStmt *node)
{
	WRITE_NODE_TYPE("CREATEDIRECTORYTABLESTMT");

	_outCreateStmtInfo(str, (const CreateStmt *) node);
	WRITE_STRING_FIELD(tablespacename);
	WRITE_STRING_FIELD(location);
}

static void
_outQueryDispatchDesc(StringInfo str, const QueryDispatchDesc *node)
{
	WRITE_NODE_TYPE("QUERYDISPATCHDESC");

	WRITE_NODE_FIELD(intoCreateStmt);
	WRITE_NODE_FIELD(paramInfo);
	WRITE_NODE_FIELD(oidAssignments);
	WRITE_NODE_FIELD(sliceTable);
	WRITE_NODE_FIELD(cursorPositions);
	WRITE_STRING_FIELD(parallelCursorName);
	WRITE_BOOL_FIELD(useChangedAOOpts);
	WRITE_INT_FIELD(secContext);
	WRITE_NODE_FIELD(namedRelList);
	WRITE_OID_FIELD(matviewOid);
	WRITE_OID_FIELD(tableid);
	WRITE_INT_FIELD(snaplen);
	WRITE_STRING_FIELD(snapname);
}

static void
_outTupleDescNode(StringInfo str, const TupleDescNode *node)
{
	int			i;

	Assert(node->tuple->tdtypeid == RECORDOID);

	WRITE_NODE_TYPE("TUPLEDESCNODE");
	WRITE_INT_FIELD(natts);
	WRITE_INT_FIELD(tuple->natts);

	for (i = 0; i < node->tuple->natts; i++)
		appendBinaryStringInfo(str, (char *) &node->tuple->attrs[i], ATTRIBUTE_FIXED_PART_SIZE);

	Assert(node->tuple->constr == NULL);

	WRITE_OID_FIELD(tuple->tdtypeid);
	WRITE_INT_FIELD(tuple->tdtypmod);
	WRITE_INT_FIELD(tuple->tdrefcount);
}

static void
_outSerializedParams(StringInfo str, const SerializedParams *node)
{
	WRITE_NODE_TYPE("SERIALIZEDPARAMS");

	WRITE_INT_FIELD(nExternParams);
	for (int i = 0; i < node->nExternParams; i++)
	{
		WRITE_BOOL_FIELD(externParams[i].isnull);
		WRITE_INT_FIELD(externParams[i].pflags);
		WRITE_OID_FIELD(externParams[i].ptype);
		WRITE_INT_FIELD(externParams[i].plen);
		WRITE_BOOL_FIELD(externParams[i].pbyval);

		if (!node->externParams[i].isnull)
			outDatum(str,
					 node->externParams[i].value,
					 node->externParams[i].plen,
					 node->externParams[i].pbyval);
	}

	WRITE_INT_FIELD(nExecParams);
	for (int i = 0; i < node->nExecParams; i++)
	{
		WRITE_BOOL_FIELD(execParams[i].isnull);
		WRITE_BOOL_FIELD(execParams[i].isvalid);
		WRITE_INT_FIELD(execParams[i].plen);
		WRITE_BOOL_FIELD(execParams[i].pbyval);

		if (node->execParams[i].isvalid && !node->execParams[i].isnull)
			outDatum(str,
					 node->execParams[i].value,
					 node->execParams[i].plen,
					 node->execParams[i].pbyval);
		WRITE_BOOL_FIELD(execParams[i].pbyval);
	}

	/*
	 * No text output function for TupleDescNodes. But that's OK, we
	 * only support text output for debugging purposes.
	 */
	WRITE_NODE_FIELD(transientTypes);
}

static void
_outOidAssignment(StringInfo str, const OidAssignment *node)
{
	WRITE_NODE_TYPE("OIDASSIGNMENT");

	WRITE_OID_FIELD(catalog);
	WRITE_STRING_FIELD(objname);
	WRITE_OID_FIELD(namespaceOid);
	WRITE_OID_FIELD(keyOid1);
	WRITE_OID_FIELD(keyOid2);
	WRITE_OID_FIELD(oid);
}

static void
_outSequence(StringInfo str, const Sequence *node)
{
	WRITE_NODE_TYPE("SEQUENCE");
	_outPlanInfo(str, (Plan *)node);
	WRITE_NODE_FIELD(subplans);
}

static void
_outDQAExpr(StringInfo str, const DQAExpr *node)
{
    WRITE_NODE_TYPE("DQAExpr");

    WRITE_INT_FIELD(agg_expr_id);
    WRITE_BITMAPSET_FIELD(agg_args_id_bms);
    WRITE_NODE_FIELD(agg_filter);
}

static void
_outTupleSplit(StringInfo str, const TupleSplit *node)
{
	WRITE_NODE_TYPE("TupleSplit");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_INT_FIELD(numCols);
	WRITE_ATTRNUMBER_ARRAY(grpColIdx, node->numCols);
	WRITE_NODE_FIELD(dqa_expr_lst);
}

static void
_outTableFunctionScan(StringInfo str, const TableFunctionScan *node)
{
	WRITE_NODE_TYPE("TABLEFUNCTIONSCAN");

	_outScanInfo(str, (Scan *) node);

	WRITE_NODE_FIELD(function);
}

static void
_outShareInputScan(StringInfo str, const ShareInputScan *node)
{
	WRITE_NODE_TYPE("SHAREINPUTSCAN");

	WRITE_BOOL_FIELD(cross_slice);
	WRITE_INT_FIELD(share_id);
	WRITE_INT_FIELD(producer_slice_id);
	WRITE_INT_FIELD(this_slice_id);
	WRITE_INT_FIELD(nconsumers);
	WRITE_BOOL_FIELD(discard_output);
	WRITE_BOOL_FIELD(ref_set);

	_outPlanInfo(str, (Plan *) node);
}

static void
_outMotion(StringInfo str, const Motion *node)
{
	WRITE_NODE_TYPE("MOTION");

	WRITE_INT_FIELD(motionID);
	WRITE_ENUM_FIELD(motionType, MotionType);

	WRITE_BOOL_FIELD(sendSorted);

	WRITE_NODE_FIELD(hashExprs);
	WRITE_OID_ARRAY(hashFuncs, list_length(node->hashExprs));
	WRITE_INT_FIELD(numSortCols);
	WRITE_ATTRNUMBER_ARRAY(sortColIdx, node->numSortCols);
	WRITE_INT_ARRAY(sortOperators, node->numSortCols);
	WRITE_INT_ARRAY(collations, node->numSortCols);
	WRITE_BOOL_ARRAY(nullsFirst, node->numSortCols);
	WRITE_INT_FIELD(segidColIdx);

	WRITE_INT_FIELD(numHashSegments);

	/* senderSliceInfo is intentionally omitted. It's only used during planning */

	_outPlanInfo(str, (Plan *) node);
}

/*
 * _outSplitUpdate
 */
static void
_outSplitUpdate(StringInfo str, const SplitUpdate *node)
{
	WRITE_NODE_TYPE("SplitUpdate");

	WRITE_INT_FIELD(actionColIdx);
	WRITE_NODE_FIELD(insertColIdx);
	WRITE_NODE_FIELD(deleteColIdx);

	WRITE_INT_FIELD(numHashSegments);
	WRITE_INT_FIELD(numHashAttrs);
	WRITE_ATTRNUMBER_ARRAY(hashAttnos, node->numHashAttrs);
	WRITE_OID_ARRAY(hashFuncs, node->numHashAttrs);

	_outPlanInfo(str, (Plan *) node);
}

/*
 * _outSplitMerge
 */
static void
_outSplitMerge(StringInfo str, const SplitMerge *node)
{
	WRITE_NODE_TYPE("SplitMerge");

	WRITE_INT_FIELD(numHashSegments);
	WRITE_INT_FIELD(numHashAttrs);
	WRITE_ATTRNUMBER_ARRAY(hashAttnos, node->numHashAttrs);
	WRITE_OID_ARRAY(hashFuncs, node->numHashAttrs);
	WRITE_NODE_FIELD(resultRelations);
	WRITE_NODE_FIELD(mergeActionLists);

	_outPlanInfo(str, (Plan *) node);
}

/*
 * _outAssertOp
 */
static void
_outAssertOp(StringInfo str, const AssertOp *node)
{
	WRITE_NODE_TYPE("AssertOp");

	WRITE_NODE_FIELD(errmessage);
	WRITE_INT_FIELD(errcode);

	_outPlanInfo(str, (Plan *) node);
}

/*
 * _outPartitionSelector
 */
static void
_outPartitionSelector(StringInfo str, const PartitionSelector *node)
{
	WRITE_NODE_TYPE("PartitionSelector");

	WRITE_INT_FIELD(paramid);
	WRITE_NODE_FIELD(part_prune_info);

	_outPlanInfo(str, (Plan *) node);
}

/*****************************************************************************
 *
 *	Stuff from primnodes.h.
 *
 *****************************************************************************/

static void
_outExtTableTypeDesc(StringInfo str, const ExtTableTypeDesc *node)
{
	WRITE_NODE_TYPE("EXTTABLETYPEDESC");

	WRITE_ENUM_FIELD(exttabletype, ExtTableType);
	WRITE_NODE_FIELD(location_list);
	WRITE_NODE_FIELD(on_clause);
	WRITE_STRING_FIELD(command_string);
}

static void
_outCreateExternalStmt(StringInfo str, const CreateExternalStmt *node)
{
	WRITE_NODE_TYPE("CREATEEXTERNALSTMT");

	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(tableElts);
	WRITE_NODE_FIELD(exttypedesc);
	WRITE_STRING_FIELD(format);
	WRITE_NODE_FIELD(formatOpts);
	WRITE_BOOL_FIELD(isweb);
	WRITE_BOOL_FIELD(iswritable);
	WRITE_NODE_FIELD(sreh);
	WRITE_NODE_FIELD(extOptions);
	WRITE_NODE_FIELD(encoding);
	WRITE_NODE_FIELD(distributedBy);
	WRITE_NODE_FIELD(tags);
}

static void
wrapStringList(List *list)
{
	ListCell *lc;

	foreach(lc, list)
	{
		char	   *str = (char *) lfirst(lc);

		lfirst(lc) = makeString(str);
	}
}

static void
unwrapStringList(List *list)
{
	ListCell *lc;

	foreach(lc, list)
	{
		String	   *val = lfirst(lc);

		lfirst(lc) = strVal(val);
		pfree(val);
	}
}

static void
_outAlteredTableInfo(StringInfo str, const AlteredTableInfo *node)
{
	ListCell   *lc;

	WRITE_NODE_TYPE("ALTEREDTABLEINFO");

	WRITE_OID_FIELD(relid);
	WRITE_CHAR_FIELD(relkind);
	/* oldDesc is omitted */

	for (int i = 0; i < AT_NUM_PASSES; i++)
    {
		WRITE_NODE_FIELD(subcmds[i]);
    }

	/*
	 * These aren't Nodes in upstream, so make sure the node tags
	 * are set correctly before trying to serialize them.
	 */
	foreach(lc, node->constraints)
	{
		NewConstraint *e = (NewConstraint *) lfirst(lc);
		e->type = T_NewConstraint;
	}
	foreach(lc, node->newvals)
	{
		NewColumnValue *e = (NewColumnValue *) lfirst(lc);
		e->type = T_NewColumnValue;
	}

	WRITE_NODE_FIELD(constraints);
	WRITE_NODE_FIELD(newvals);
	WRITE_NODE_FIELD(afterStmts);
	WRITE_BOOL_FIELD(verify_new_notnull);
	WRITE_INT_FIELD(rewrite);
	WRITE_OID_FIELD(newAccessMethod);
	WRITE_BOOL_FIELD(dist_opfamily_changed);
	WRITE_OID_FIELD(new_opclass);
	/*
	 * NB: newTableSpace is excluded, it will be assigned in phase 1 of AlterTable.
	 * If newTableSpace is required, refer to the name in its corresponding cmd.
	 * If newTableSpace is strongly required in serialization, please add it
	 * and update `ATPrepSetTableSpace()` to avoid error.
	 */
	WRITE_BOOL_FIELD(chgPersistence);
	WRITE_CHAR_FIELD(newrelpersistence);
	WRITE_NODE_FIELD(partition_constraint);
	WRITE_BOOL_FIELD(validate_default);
	WRITE_NODE_FIELD(changedConstraintOids);

	/* node->changedConstraintDefs is a list of naked strings, so
	 * we can't use WRITE_NODE_FIELD on it. Temporarily wrap them in Values.
	 */
	wrapStringList(node->changedConstraintDefs);
	WRITE_NODE_FIELD(changedConstraintDefs);
	/* unwrap them again */
	unwrapStringList(node->changedConstraintDefs);

	WRITE_NODE_FIELD(changedIndexOids);
	wrapStringList(node->changedIndexDefs);
	WRITE_NODE_FIELD(changedIndexDefs);
	unwrapStringList(node->changedIndexDefs);
	WRITE_NODE_FIELD(beforeStmtLists);
	WRITE_NODE_FIELD(constraintLists);
}

static void
_outNewConstraint(StringInfo str, const NewConstraint *node)
{
	WRITE_NODE_TYPE("NEWCONSTRAINT");

	WRITE_STRING_FIELD(name);
	WRITE_ENUM_FIELD(contype, ConstrType);
	WRITE_OID_FIELD(refrelid);
	WRITE_OID_FIELD(refindid);
	WRITE_OID_FIELD(conid);
	WRITE_NODE_FIELD(qual);
	/* can't serialize qualstate */
}

static void
_outNewColumnValue(StringInfo str, const NewColumnValue *node)
{
	WRITE_NODE_TYPE("NEWCOLUMNVALUE");

	WRITE_INT_FIELD(attnum);
	WRITE_NODE_FIELD(expr);
	/* can't serialize exprstate */
	WRITE_BOOL_FIELD(is_generated);
}

static void
_outCdbProcess(StringInfo str, const CdbProcess *node)
{
	WRITE_NODE_TYPE("CDBPROCESS");
	WRITE_STRING_FIELD(listenerAddr);
	WRITE_INT_FIELD(listenerPort);
	WRITE_INT_FIELD(pid);
	WRITE_INT_FIELD(contentid);
	WRITE_INT_FIELD(dbid);
}

static void
_outSliceTable(StringInfo str, const SliceTable *node)
{
	WRITE_NODE_TYPE("SLICETABLE");

	WRITE_INT_FIELD(localSlice);
	WRITE_INT_FIELD(numSlices);
	for (int i = 0; i < node->numSlices; i++)
	{
		WRITE_INT_FIELD(slices[i].sliceIndex);
		WRITE_INT_FIELD(slices[i].rootIndex);
		WRITE_INT_FIELD(slices[i].parentIndex);
		WRITE_INT_FIELD(slices[i].planNumSegments);
		WRITE_NODE_FIELD(slices[i].children); /* List of int index */
		WRITE_ENUM_FIELD(slices[i].gangType, GangType);
		WRITE_NODE_FIELD(slices[i].segments); /* List of int */
		WRITE_BOOL_FIELD(slices[i].useMppParallelMode);
		WRITE_INT_FIELD(slices[i].parallel_workers);
		WRITE_DUMMY_FIELD(slices[i].primaryGang);
		WRITE_NODE_FIELD(slices[i].primaryProcesses); /* List of (CDBProcess *) */
		WRITE_BITMAPSET_FIELD(slices[i].processesMap);
	}
	WRITE_BOOL_FIELD(hasMotions);
	WRITE_INT_FIELD(instrument_options);
	WRITE_INT_FIELD(ic_instance_id);
}

static void
_outCursorPosInfo(StringInfo str, const CursorPosInfo *node)
{
	WRITE_NODE_TYPE("CURSORPOSINFO");

	WRITE_STRING_FIELD(cursor_name);
	WRITE_INT_FIELD(gp_segment_id);
	WRITE_UINT_FIELD(ctid.ip_blkid.bi_hi);
	WRITE_UINT_FIELD(ctid.ip_blkid.bi_lo);
	WRITE_UINT_FIELD(ctid.ip_posid);
	WRITE_OID_FIELD(table_oid);
}

static void
_outMemoize(StringInfo str, const Memoize *node)
{
	WRITE_NODE_TYPE("MEMOIZE");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_INT_FIELD(numKeys);
	WRITE_OID_ARRAY(hashOperators, node->numKeys);
	WRITE_OID_ARRAY(collations, node->numKeys);
	WRITE_NODE_FIELD(param_exprs);
	WRITE_BOOL_FIELD(singlerow);
	WRITE_BOOL_FIELD(binary_mode);
	WRITE_UINT_FIELD(est_entries);
	WRITE_BITMAPSET_FIELD(keyparamids);
}

static void
_outTidRangeScan(StringInfo str, const TidRangeScan *node)
{
	WRITE_NODE_TYPE("TIDRANGESCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_NODE_FIELD(tidrangequals);
}

static void
_outEphemeralNamedRelationInfo(StringInfo str, const EphemeralNamedRelationInfo *node)
{
	int			i;

	WRITE_NODE_TYPE("EphemeralNamedRelationInfo");
	WRITE_STRING_FIELD(name);
	WRITE_OID_FIELD(reliddesc);
	WRITE_INT_FIELD(natts);
	WRITE_INT_FIELD(tuple->natts);

	for (i = 0; i < node->tuple->natts; i++)
		appendBinaryStringInfo(str, (char *) &node->tuple->attrs[i], ATTRIBUTE_FIXED_PART_SIZE);

	WRITE_OID_FIELD(tuple->tdtypeid);
	WRITE_INT_FIELD(tuple->tdtypmod);
	WRITE_INT_FIELD(tuple->tdrefcount);
	WRITE_ENUM_FIELD(enrtype, EphemeralNameRelationType);
	WRITE_FLOAT_FIELD(enrtuples);
}

static void
_outDropStmtInfo(StringInfo str, const DropStmt *node)
{
	WRITE_NODE_FIELD(objects);
	WRITE_ENUM_FIELD(removeType, ObjectType);
	WRITE_ENUM_FIELD(behavior, DropBehavior);
	WRITE_BOOL_FIELD(missing_ok);
	WRITE_BOOL_FIELD(concurrent);
	WRITE_BOOL_FIELD(isdynamic);
}

static void
_outDropDirectoryTableStmt(StringInfo str, const DropDirectoryTableStmt *node)
{
	WRITE_NODE_TYPE("DROPDIRECTORYTABLESTMT");

	_outDropStmtInfo(str, (const DropStmt *) node);
	WRITE_BOOL_FIELD(with_content);
}

/*****************************************************************************
 *
 *	Stuff from primnodes.h.
 *
 *****************************************************************************/

static void
_outConst(StringInfo str, Const *node)
{
	WRITE_NODE_TYPE("CONST");

	WRITE_OID_FIELD(consttype);
	WRITE_INT_FIELD(consttypmod);
	WRITE_OID_FIELD(constcollid);
	WRITE_INT_FIELD(constlen);
	WRITE_BOOL_FIELD(constbyval);
	WRITE_BOOL_FIELD(constisnull);
	WRITE_LOCATION_FIELD(location);

	if (!node->constisnull)
		_outDatum(str, node->constvalue, node->constlen, node->constbyval);
}

static void
_outBoolExpr(StringInfo str, BoolExpr *node)
{
	WRITE_NODE_TYPE("BOOLEXPR");
	WRITE_ENUM_FIELD(boolop, BoolExprType);

	WRITE_NODE_FIELD(args);
	WRITE_LOCATION_FIELD(location);
}

/*****************************************************************************
 *
 *	Stuff from extensible.h
 *
 *****************************************************************************/

static void
_outExtensibleNode(StringInfo str, const ExtensibleNode *node)
{
	const ExtensibleNodeMethods *methods;
	StringInfoData buf;
	initStringInfo(&buf);

	methods = GetExtensibleNodeMethods(node->extnodename, false);

	WRITE_NODE_TYPE("EXTENSIBLENODE");

	WRITE_STRING_FIELD(extnodename);

	/* serialize the private fields */
	methods->nodeOut(&buf, node);

	WRITE_STRING_VAR(buf.data);
}

/*****************************************************************************
 *
 *	Stuff from parsenodes.h.
 *
 *****************************************************************************/

static void
_outQuery(StringInfo str, Query *node)
{
	WRITE_NODE_TYPE("QUERY");

	WRITE_ENUM_FIELD(commandType, CmdType);
	WRITE_ENUM_FIELD(querySource, QuerySource);
	WRITE_BOOL_FIELD(canSetTag);
	WRITE_NODE_FIELD(utilityStmt);
	WRITE_INT_FIELD(resultRelation);
	WRITE_BOOL_FIELD(hasAggs);
	WRITE_BOOL_FIELD(hasWindowFuncs);
	WRITE_BOOL_FIELD(hasSubLinks);
	WRITE_BOOL_FIELD(hasDynamicFunctions);
	WRITE_BOOL_FIELD(hasFuncsWithExecRestrictions);
	WRITE_BOOL_FIELD(hasDistinctOn);
	WRITE_BOOL_FIELD(hasRecursive);
	WRITE_BOOL_FIELD(hasModifyingCTE);
	WRITE_BOOL_FIELD(hasForUpdate);
	WRITE_BOOL_FIELD(hasRowSecurity);
	WRITE_BOOL_FIELD(canOptSelectLockingClause);
	WRITE_NODE_FIELD(cteList);
	WRITE_NODE_FIELD(rtable);
	WRITE_NODE_FIELD(rteperminfos);
	WRITE_NODE_FIELD(jointree);
	WRITE_NODE_FIELD(mergeActionList);
	WRITE_BOOL_FIELD(mergeUseOuterJoin);
	WRITE_NODE_FIELD(targetList);
	WRITE_NODE_FIELD(withCheckOptions);
	WRITE_NODE_FIELD(onConflict);
	WRITE_NODE_FIELD(returningList);
	WRITE_NODE_FIELD(groupClause);
	WRITE_NODE_FIELD(groupingSets);
	WRITE_NODE_FIELD(havingQual);
	WRITE_NODE_FIELD(windowClause);
	WRITE_NODE_FIELD(distinctClause);
	WRITE_NODE_FIELD(sortClause);
	WRITE_NODE_FIELD(scatterClause);
	WRITE_BOOL_FIELD(isTableValueSelect);
	WRITE_NODE_FIELD(limitOffset);
	WRITE_NODE_FIELD(limitCount);
	WRITE_NODE_FIELD(rowMarks);
	WRITE_NODE_FIELD(setOperations);
	WRITE_NODE_FIELD(constraintDeps);
	WRITE_BOOL_FIELD(parentStmtType);

	/* Don't serialize policy */
}

static void
_outAExpr(StringInfo str, A_Expr *node)
{
	WRITE_NODE_TYPE("AEXPR");
	WRITE_ENUM_FIELD(kind, A_Expr_Kind);

	switch (node->kind)
	{
		case AEXPR_OP:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_OP_ANY:

			WRITE_NODE_FIELD(name);

			break;
		case AEXPR_OP_ALL:

			WRITE_NODE_FIELD(name);

			break;
		case AEXPR_DISTINCT:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_NOT_DISTINCT:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_NULLIF:

			WRITE_NODE_FIELD(name);
			break;

		case AEXPR_IN:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_LIKE:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_ILIKE:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_SIMILAR:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_BETWEEN:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_NOT_BETWEEN:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_BETWEEN_SYM:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_NOT_BETWEEN_SYM:

			WRITE_NODE_FIELD(name);
			break;

		default:

			break;
	}

	WRITE_NODE_FIELD(lexpr);
	WRITE_NODE_FIELD(rexpr);
	WRITE_LOCATION_FIELD(location);
}

static void
_outInteger(StringInfo str, const Integer *node)
{
	int16 vt = T_Integer;
	appendBinaryStringInfo(str, (const char *)&vt, sizeof(int16));
	appendBinaryStringInfo(str, (const char *)&node->ival, sizeof(int));
}


static void
_outFloat(StringInfo str, const Float *node)
{
	int16 vt = T_Float;
	int slen;

	appendBinaryStringInfo(str, (const char *) &vt, sizeof(int16));
	slen = (node->fval != NULL ? strlen(node->fval) : 0);
	appendBinaryStringInfo(str, (const char *)&slen, sizeof(int));
	if (slen > 0)
		appendBinaryStringInfo(str, node->fval, slen);
}

static void
_outBoolean(StringInfo str, const Boolean *node)
{
	int16 vt = T_Boolean;
	appendBinaryStringInfo(str, (const char *)&vt, sizeof(int16));
	appendBinaryStringInfo(str, (const char *)&node->boolval, sizeof(bool));
}


static void
_outString(StringInfo str, const String *node)
{
	int16 vt = T_String;
	int slen;

	appendBinaryStringInfo(str, (const char *) &vt, sizeof(int16));
	slen = (node->sval != NULL ? strlen(node->sval) : 0);
	appendBinaryStringInfo(str, (const char *)&slen, sizeof(int));
	if (slen > 0)
		appendBinaryStringInfo(str, node->sval, slen);
}

static void
_outBitString(StringInfo str, const BitString *node)
{
	int16 vt = T_BitString;
	int slen;

	appendBinaryStringInfo(str, (const char *) &vt, sizeof(int16));
	slen = (node->bsval != NULL ? strlen(node->bsval) : 0);
	appendBinaryStringInfo(str, (const char *)&slen, sizeof(int));
	if (slen > 0)
		appendBinaryStringInfo(str, node->bsval, slen);
}


static void
_outAConst(StringInfo str, A_Const *node)
{
	WRITE_NODE_TYPE("A_CONST");
	WRITE_BOOL_FIELD(isnull);
	if (!node->isnull)
		_outNode(str, &node->val);
	WRITE_LOCATION_FIELD(location);
}

static void
_outCookedConstraint(StringInfo str, CookedConstraint *node)
{
	WRITE_NODE_TYPE("COOKEDCONSTRAINT");

	WRITE_ENUM_FIELD(contype,ConstrType);
	WRITE_STRING_FIELD(name);
	WRITE_INT_FIELD(attnum);
	WRITE_NODE_FIELD(expr);
	WRITE_BOOL_FIELD(is_local);
	WRITE_INT_FIELD(inhcount);
	WRITE_BOOL_FIELD(is_no_inherit);
}

static void
_outAddForeignSegstmt(StringInfo str, AddForeignSegStmt *node)
{
	WRITE_NODE_TYPE("ADDFOREIGNSEGSTMT");

	WRITE_STRING_FIELD(servername);
	WRITE_STRING_FIELD(tablename);
	WRITE_NODE_FIELD(options);
}

static void
_outCustomScan(StringInfo str, const CustomScan *node)
{
	WRITE_NODE_TYPE("CUSTOMSCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_UINT_FIELD(flags);
	WRITE_NODE_FIELD(custom_plans);
	WRITE_NODE_FIELD(custom_exprs);
	WRITE_NODE_FIELD(custom_private);
	WRITE_NODE_FIELD(custom_scan_tlist);
	WRITE_BITMAPSET_FIELD(custom_relids);
	WRITE_STRING_FIELD(methods->CustomName);	
}


/*
 * _outNode -
 *	  converts a Node into binary string and append it to 'str'
 *
 * The following include provides binary _out* functions for all
 * auto-generated node types using the binary WRITE_* macros.
 */

#include "outfuncs.funcs.c"

static void
_outNode(StringInfo str, void *obj)
{
	if (obj == NULL)
	{
		int16 tg = 0;
		appendBinaryStringInfo(str, (const char *)&tg, sizeof(int16));
	}
	else if (IsA(obj, List) ||IsA(obj, IntList) || IsA(obj, OidList))
		_outList(str, obj);
	else if (IsA(obj, Integer))
		_outInteger(str, (Integer *) obj);
	else if (IsA(obj, Float))
		_outFloat(str, (Float *) obj);
	else if (IsA(obj, Boolean))
		_outBoolean(str, (Boolean *) obj);
	else if (IsA(obj, String))
		_outString(str, (String *) obj);
	else if (IsA(obj, BitString))
		_outBitString(str, (BitString *) obj);
	else if (IsA(obj, Bitmapset))
		outBitmapset(str, (Bitmapset *) obj);
	else
	{
		switch (nodeTag(obj))
		{
			#include "outfuncs.switch.c"

			/* Abstract plan node types (marked abstract, so not in generated switch) */
			case T_Plan:
				_outPlan(str, obj);
				break;
			case T_Scan:
				_outScan(str, obj);
				break;
			case T_Join:
				_outJoin(str, obj);
				break;

			/* GPDB-specific node types not covered by the generated switch */
			case T_QueryDispatchDesc:
				_outQueryDispatchDesc(str, obj);
				break;
			case T_OidAssignment:
				_outOidAssignment(str, obj);
				break;
			case T_SerializedParams:
				_outSerializedParams(str, obj);
				break;
			case T_SliceTable:
				_outSliceTable(str, obj);
				break;
			case T_CursorPosInfo:
				_outCursorPosInfo(str, obj);
				break;
			case T_TupleDescNode:
				_outTupleDescNode(str, obj);
				break;
			case T_EphemeralNamedRelationInfo:
				_outEphemeralNamedRelationInfo(str, obj);
				break;
			case T_AlteredTableInfo:
				_outAlteredTableInfo(str, obj);
				break;
			case T_NewConstraint:
				_outNewConstraint(str, obj);
				break;
			case T_NewColumnValue:
				_outNewColumnValue(str, obj);
				break;
			case T_DQAExpr:
				_outDQAExpr(str, obj);
				break;
			case T_ExtTableTypeDesc:
				_outExtTableTypeDesc(str, obj);
				break;
			case T_CdbProcess:
				_outCdbProcess(str, obj);
				break;
			case T_CookedConstraint:
				_outCookedConstraint(str, obj);
				break;

			default:
				elog(ERROR, "could not serialize unrecognized node type: %d",
						 (int) nodeTag(obj));
				break;
		}
	}
}

/*
 * nodeToBinaryStringFast -
 *	   returns a binary representation of the Node as a palloc'd string
 */
char *
nodeToBinaryStringFast(void *obj, int *length)
{
	StringInfoData str;
	int16 tg = (int16) 0xDEAD;

	/* see stringinfo.h for an explanation of this maneuver */
	initStringInfoOfSize(&str, 4096);

	_outNode(&str, obj);

	/* Add something special at the end that we can check in readfast.c */
	appendBinaryStringInfo(&str, (const char *)&tg, sizeof(int16));

	*length = str.len;
	return str.data;
}
