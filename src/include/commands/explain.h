/*-------------------------------------------------------------------------
 *
 * explain.h
 *	  prototypes for explain.c
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 * src/include/commands/explain.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXPLAIN_H
#define EXPLAIN_H

#include "executor/executor.h"
#include "parser/parse_node.h"

<<<<<<< HEAD
typedef enum ExplainFormat
{
	EXPLAIN_FORMAT_TEXT,
	EXPLAIN_FORMAT_XML,
	EXPLAIN_FORMAT_JSON,
	EXPLAIN_FORMAT_YAML
} ExplainFormat;

typedef struct ExplainWorkersState
{
	int			num_workers;	/* # of worker processes the plan used */
	bool	   *worker_inited;	/* per-worker state-initialized flags */
	StringInfoData *worker_str; /* per-worker transient output buffers */
	int		   *worker_state_save;	/* per-worker grouping state save areas */
	StringInfo	prev_str;		/* saved output buffer while redirecting */
} ExplainWorkersState;

typedef struct ExplainState
{
	StringInfo	str;			/* output buffer */
	/* options */
	bool		verbose;		/* be verbose */
	bool		analyze;		/* print actual times */
	bool		costs;			/* print estimated costs */
	bool		locus;			/* print path locus */
	bool		buffers;		/* print buffer usage */
	bool		dxl;			/* CDB: print DXL */
	bool		slicetable;		/* CDB: print slice table */
	bool		memory_detail;	/* CDB: print per-node memory usage */
	bool		wal;			/* print WAL usage */
	bool		timing;			/* print detailed node timing */
	bool		summary;		/* print total planning and execution timing */
	bool		settings;		/* print modified settings */
	bool		generic;		/* generate a generic plan */
	ExplainFormat format;		/* output format */
	/* state for output formatting --- not reset for each new plan tree */
	int			indent;			/* current indentation level */
	List	   *grouping_stack; /* format-specific grouping state */
	/* state related to the current plan tree (filled by ExplainPrintPlan) */
	PlannedStmt *pstmt;			/* top of plan */
	List	   *rtable;			/* range table */
	List	   *rtable_names;	/* alias names for RTEs */
	List	   *deparse_cxt;	/* context list for deparsing expressions */
	Bitmapset  *printed_subplans;	/* ids of SubPlans we've printed */

    /* CDB */
    struct CdbExplain_ShowStatCtx  *showstatctx;    /* EXPLAIN ANALYZE info */
	ExecSlice  *currentSlice;	/* slice whose nodes we are visiting */
	bool		subplanDispatchedSeparately;

	PlanState  *parentPlanState;
	bool		hide_workers;	/* set if we find an invisible Gather */
	/* state related to the current plan node */
	ExplainWorkersState *workers_state; /* needed if parallel plan */
} ExplainState;
=======
struct ExplainState;			/* defined in explain_state.h */
>>>>>>> REL_18_BETA1_branch

/* Hook for plugins to get control in ExplainOneQuery() */
typedef void (*ExplainOneQuery_hook_type) (Query *query,
										   int cursorOptions,
										   IntoClause *into,
										   struct ExplainState *es,
										   const char *queryString,
										   ParamListInfo params,
										   QueryEnvironment *queryEnv);
extern PGDLLIMPORT ExplainOneQuery_hook_type ExplainOneQuery_hook;

/* Hook for EXPLAIN plugins to print extra information for each plan */
typedef void (*explain_per_plan_hook_type) (PlannedStmt *plannedstmt,
											IntoClause *into,
											struct ExplainState *es,
											const char *queryString,
											ParamListInfo params,
											QueryEnvironment *queryEnv);
extern PGDLLIMPORT explain_per_plan_hook_type explain_per_plan_hook;

/* Hook for EXPLAIN plugins to print extra fields on individual plan nodes */
typedef void (*explain_per_node_hook_type) (PlanState *planstate,
											List *ancestors,
											const char *relationship,
											const char *plan_name,
											struct ExplainState *es);
extern PGDLLIMPORT explain_per_node_hook_type explain_per_node_hook;

/* Hook for plugins to get control in explain_get_index_name() */
typedef const char *(*explain_get_index_name_hook_type) (Oid indexId);
extern PGDLLIMPORT explain_get_index_name_hook_type explain_get_index_name_hook;


extern void ExplainQuery(ParseState *pstate, ExplainStmt *stmt,
						 ParamListInfo params, DestReceiver *dest);
extern void standard_ExplainOneQuery(Query *query, int cursorOptions,
									 IntoClause *into, struct ExplainState *es,
									 const char *queryString, ParamListInfo params,
									 QueryEnvironment *queryEnv);

extern TupleDesc ExplainResultDesc(ExplainStmt *stmt);

extern void ExplainOneUtility(Node *utilityStmt, IntoClause *into,
							  struct ExplainState *es, ParseState *pstate,
							  ParamListInfo params);

extern void ExplainOnePlan(PlannedStmt *plannedstmt, CachedPlan *cplan,
						   CachedPlanSource *plansource, int query_index,
						   IntoClause *into, struct ExplainState *es,
						   const char *queryString,
						   ParamListInfo params, QueryEnvironment *queryEnv,
						   const instr_time *planduration,
						   const BufferUsage *bufusage,
<<<<<<< HEAD
						   int cursorOptions);

extern void ExplainPrintPlan(ExplainState *es, QueryDesc *queryDesc);
extern void ExplainPrintTriggers(ExplainState *es, QueryDesc *queryDesc);
extern void ExplainParallelRetrieveCursor(ExplainState *es, QueryDesc* queryDesc);
extern void ExplainPrintSliceTable(ExplainState *es, QueryDesc *queryDesc);
=======
						   const MemoryContextCounters *mem_counters);

extern void ExplainPrintPlan(struct ExplainState *es, QueryDesc *queryDesc);
extern void ExplainPrintTriggers(struct ExplainState *es,
								 QueryDesc *queryDesc);
>>>>>>> REL_18_BETA1_branch

extern void ExplainPrintJITSummary(struct ExplainState *es,
								   QueryDesc *queryDesc);

extern void ExplainQueryText(struct ExplainState *es, QueryDesc *queryDesc);
extern void ExplainQueryParameters(struct ExplainState *es,
								   ParamListInfo params, int maxlen);

extern void ExplainPrintExecStatsEnd(ExplainState *es, QueryDesc *queryDesc);

#endif							/* EXPLAIN_H */
