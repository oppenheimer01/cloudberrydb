/*-------------------------------------------------------------------------
 *
 * cdbgroupingpaths.c
 *	  Routines to aid in planning grouping queries for parallel
 *    execution.  This is, essentially, an extension of the file
 *    optimizer/plan/planner.c, although some functions are not
 *    externalized.
 *
 *
 * The general shape of the generated plan is similar to the parallel
 * aggregation plans in upstream:
 *
 * Finalize Aggregate [3]
 *    Motion             [2]
 *       Partial Aggregate  [1]
 *
 * but there are many different variants of this basic shape:
 *
 * [1] The Partial stage can be sorted or hashed. Furthermore,
 *     the sorted Agg can be construct from sorting the cheapest input Path,
 *     or from pre-sorted Paths.
 *
 * [2] The partial results need to be gathered for the second stage.
 *     For plain aggregation, with no GROUP BY, the results need to be
 *     gathered to a single node. With GROUP BY, they can be redistributed
 *     according to the GROUP BY columns.
 *
 * [3] Like the first tage, the second stage can likewise be sorted or hashed.
 *
 *
 * Things get more complicated if any of the aggregates have DISTINCT
 * arguments, also known as DQAs or Distinct-Qualified Aggregates. If there
 * is only one DQA, and the input path happens to be collocated with the
 * DISTINCT argument, then we can proceed with a two-stage path like above.
 * But otherwise, three stages and possibly TupleSplit node is needed. See
 * add_single_dqa_hash_agg_path() and add_multi_dqas_hash_agg_path() for
 * details.
 *
 * Portions Copyright (c) 2019-Present VMware, Inc. or its affiliates.
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdbgroupingpaths.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "cdb/cdbgroup.h"
#include "cdb/cdbgroupingpaths.h"
#include "cdb/cdbhash.h"
#include "cdb/cdbpath.h"
#include "cdb/cdbpathlocus.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "foreign/fdwapi.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/prep.h"
#include "optimizer/tlist.h"
#include "parser/parse_clause.h"
#include "parser/parse_oper.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"

#include "access/genam.h"
#include "access/table.h"
#include "catalog/pg_rewrite.h"
#include "nodes/pathnodes.h"
#include "parser/parsetree.h"
#include "utils/guc.h"
#include "commands/matview.h"

typedef enum
{
	INVALID_DQA = -1,
	SINGLE_DQA, /* only one unique DQA expr */
	MULTI_DQAS, /* multiple DQA exprs */
	SINGLE_DQA_WITHAGG, /* only one unique DQA expr with agg */
	MULTI_DQAS_WITHAGG/* mixed DQA and normal aggregate */
} DQAType;

/*
 * For convenience, we collect various inputs and intermediate planning results
 * in this struct, instead of passing a dozen arguments to all subroutines.
 */
typedef struct
{
	/* From the Query */
	bool		hasAggs;
	List	   *groupClause;	/* a list of SortGroupClause's */
	List	   *groupingSets;	/* a list of GroupingSet's if present */
	List	   *group_tles;

	/* Inputs from the caller */
	List	   *havingQual;		/* qualifications applied to groups */
	PathTarget *target;			/* targetlist of final aggregated result */
	bool		can_sort;
	bool		can_hash;
	double		dNumGroupsTotal;		/* total number of groups in the result, across all QEs */
	const AggClauseCosts *agg_costs;
	const AggClauseCosts *agg_partial_costs;
	const AggClauseCosts *agg_final_costs;
	List	   *rollups;
	List       *new_rollups;
	AggStrategy strat;

	PathTarget *partial_grouping_target;	/* targetlist of partially aggregated result */
	List	   *final_groupClause;			/* SortGroupClause for final grouping */
	List	   *final_group_tles;
	Index		gsetid_sortref;

	/*
	 * Pathkeys representing GROUP BY.
	 *
	 * 'partial_needed_pathkeys' represents a sort order that's needed for
	 * doing a sorted GroupAggregate in the first
	 * stage. 'partial_sort_pathkey' is normally the same, but in case of
	 * DISTINCT ON and ORDER BY it can include extra columns that are presentt
	 * in the ORDER BY but not in DISTINCT ON. The idea is the needed_pathkeys
	 * are sufficient to perform the grouping, but if we have to sort the
	 * input, we sort using sort_pathkeys. By including the extra columns in
	 * the Sort we can avoid sorting the data again later to satisfy the ORDER
	 * BY.
	 *
	 * 'final_needed_pathkeys' is the sort order needed to perform the 2nd
	 * stage by sorted GroupAggregate.  In normal GROUP BY it is the same as
	 * 'partial_needed_pathkeys', but if there are GROUPING SETS,
	 * 'final_needed_pathkeys' includes the internal GROUPINGSET_ID()
	 * expression, used to distinguish the rolled up rows. And
	 * 'final_sort_pathkeys' is the same, but might include extra ORDER BY
	 * columns.
	 *
	 */
	List	   *partial_needed_pathkeys;
	List	   *partial_sort_pathkeys;
	List	   *final_needed_pathkeys;
	List	   *final_sort_pathkeys;

	DQAType     type;

	/*
	 * partial_rel holds the partially aggregated results from the first stage.
	 */
	RelOptInfo *partial_rel;
} cdb_agg_planning_context;

typedef struct
{
	DQAType     type;

	PathTarget  *final_target;          /* finalize agg tlist */
	PathTarget  *partial_target;        /* partial agg tlist */
	PathTarget  *tup_split_target;      /* AggExprId + subpath_proj_target */
	PathTarget  *input_proj_target;     /* input tuple tlist + DQA expr */

	List        *dqa_group_clause;      /* DQA exprs + group by clause for remove duplication */

	List        *dqa_expr_lst;          /* DQAExpr list */
	double		 dNumDistinctGroups;	/* # of distinct combinations of GROUP BY and DISTINCT exprs */

} cdb_multi_dqas_info;

static void create_two_stage_paths(PlannerInfo *root, cdb_agg_planning_context *ctx,
								   RelOptInfo *input_rel, RelOptInfo *output_rel, List *partial_pathlist);

static List *get_all_rollup_groupclauses(List *rollups);

static Index add_gsetid_tlist(List *tlist);

static SortGroupClause *create_gsetid_groupclause(Index groupref);
static List *strip_gsetid_from_pathkeys(Index gsetid_sortref, List *pathkeys);

static void add_first_stage_group_agg_path(PlannerInfo *root,
										   Path *path,
										   bool is_sorted,
										   cdb_agg_planning_context *ctx);
static void add_first_stage_hash_agg_path(PlannerInfo *root,
										  Path *path,
										  cdb_agg_planning_context *ctx);
static void add_second_stage_group_agg_path(PlannerInfo *root,
											Path *path,
											bool is_sorted,
											cdb_agg_planning_context *ctx,
											RelOptInfo *output_rel,
											bool is_partial);
static void add_second_stage_hash_agg_path(PlannerInfo *root,
										   Path *path,
										   cdb_agg_planning_context *ctx,
										   RelOptInfo *output_rel,
										   bool is_partial);
static void add_single_dqa_hash_agg_path(PlannerInfo *root,
										 Path *path,
										 cdb_agg_planning_context *ctx,
										 RelOptInfo *output_rel,
										 PathTarget *input_target,
										 List       *dqa_group_clause,
										 double dNumDistinctGroups);
static void add_single_mixed_dqa_hash_agg_path(PlannerInfo *root,
                                               Path *path,
                                               cdb_agg_planning_context *ctx,
                                               RelOptInfo *output_rel,
                                               PathTarget *input_target,
                                               List       *dqa_group_clause);
static void
add_multi_dqas_hash_agg_path(PlannerInfo *root,
							 Path *path,
							 cdb_agg_planning_context *ctx,
							 RelOptInfo *output_rel,
							 cdb_multi_dqas_info *info);

static void
fetch_single_dqa_info(PlannerInfo *root,
					  Path *path,
					  cdb_agg_planning_context *ctx,
					  cdb_multi_dqas_info *info);

static void
fetch_multi_dqas_info(PlannerInfo *root,
					  Path *path,
					  cdb_agg_planning_context *ctx,
					  cdb_multi_dqas_info *info);

static DQAType
recognize_dqa_type(cdb_agg_planning_context *ctx);

static PathTarget *
strip_aggdistinct(PathTarget *target);

#ifdef SERVERLESS
static bool
try_append_agg(Query *parse);

static bool
expand_append_agg(PlannerInfo *root, cdb_agg_planning_context *ctx);

static Relation
simple_view_matching(Query *parse);

static void
expand_append_agg_guts(PlannerInfo *root, cdb_agg_planning_context *ctx, Relation matviewRel);

static Path*
build_view_seqscan_path(PlannerInfo *root, cdb_agg_planning_context *ctx, Relation matviewRel);

static void
rewrite_to_append_agg_path(PlannerInfo *root, cdb_agg_planning_context *ctx, Relation matviewRel, Path *mv_seqscan_path);

static PathTarget *
make_pathtarget_from_tupledesc(TupleDesc tupdes);
typedef struct
{
	int 	varno;
} aqumv_adjust_varno_context;

static void aqumv_adjust_simple_parse(Query *parse);
static void aqumv_adjust_varno(Query *parse, int delta);
static Node *aqumv_adjust_varno_mutator(Node *node, aqumv_adjust_varno_context *context);
#endif

/*
 * cdb_create_multistage_grouping_paths
 *
 * This is basically an extension of the function create_grouping_paths() from
 * planner.c.  It creates two- and three-stage Paths to implement aggregates
 * and/or GROUP BY.
 *
 * The caller already constructed Paths for one-stage plans, we are only
 * concerned about more complicated multi-stage plans here.
 */
void
cdb_create_multistage_grouping_paths(PlannerInfo *root,
								   RelOptInfo *input_rel,
								   RelOptInfo *output_rel,
								   PathTarget *target,
								   PathTarget *partial_grouping_target,
								   List *havingQual,
								   double dNumGroupsTotal,
								   const AggClauseCosts *agg_costs,
								   const AggClauseCosts *agg_partial_costs,
								   const AggClauseCosts *agg_final_costs,
								   List *rollups,
								   List *new_rollups,
								   AggStrategy strat,
								   List *partial_pathlist)
{
	Query	   *parse = root->parse;
	Path	   *cheapest_path = input_rel->cheapest_total_path;
	bool		has_ordered_aggs = root->numPureOrderedAggs > 0;
	cdb_agg_planning_context ctx;
	bool		can_sort;
	bool		can_hash;

	/* The caller should've checked these already */
	Assert(parse->hasAggs || parse->groupClause);

	/*
	 * This prohibition could be relaxed if we tracked missing combine
	 * functions per DQA and were willing to plan some DQAs as single and
	 * some as multiple phases.  Not currently, however.
	 */
	Assert(!root->hasNonCombine && !root->hasNonSerialAggs);
	Assert(root->config->gp_enable_multiphase_agg);

	/*
	 * Ordered aggregates need to run the transition function on the
	 * values in sorted order, which in turn translates into single phase
	 * aggregation.
	 */
	if (has_ordered_aggs)
		return;

	/*
	 * We are currently unwilling to redistribute a gathered intermediate
	 * across the cluster.  This might change one day.
	 */
	if (!CdbPathLocus_IsPartitioned(cheapest_path->locus))
		return;

	/*
	 * Is the input hashable / sortable? This is largely the same logic as in
	 * upstream create_grouping_paths(), but we can do hashing in limited ways
	 * even if there are DISTINCT aggs or grouping sets.
	 */
	can_sort = grouping_is_sortable(parse->groupClause);
	can_hash = (parse->groupClause != NIL &&
				root->numPureOrderedAggs == 0 &&
				grouping_is_hashable(parse->groupClause));

	/*
	 * Prepare a struct to hold the arguments and intermediate results
	 * across subroutines.
	 */
	memset(&ctx, 0, sizeof(ctx));
	ctx.can_sort = can_sort;
	ctx.can_hash = can_hash;
	ctx.target = target;
	ctx.dNumGroupsTotal = dNumGroupsTotal;
	ctx.agg_costs = agg_costs;
	ctx.agg_partial_costs = agg_partial_costs;
	ctx.agg_final_costs = agg_final_costs;
	ctx.rollups = rollups;
	ctx.new_rollups = new_rollups;
	ctx.strat = strat;

	ctx.hasAggs = parse->hasAggs;
	ctx.groupClause = parse->groupClause;
	ctx.groupingSets = parse->groupingSets;
	ctx.havingQual = havingQual;
	ctx.partial_rel = fetch_upper_rel(root, UPPERREL_CDB_FIRST_STAGE_GROUP_AGG, NULL);
	ctx.partial_rel->fdwroutine = input_rel->fdwroutine;
	ctx.partial_rel->serverid = input_rel->serverid;
	ctx.partial_rel->segSeverids = input_rel->segSeverids;
	ctx.partial_rel->userid = input_rel->userid;
	ctx.partial_rel->exec_location = input_rel->exec_location;
	ctx.partial_rel->num_segments = input_rel->num_segments;

	/* create a partial rel similar to make_grouping_rel() */
	if (IS_OTHER_REL(input_rel))
	{
		ctx.partial_rel = fetch_upper_rel(root, UPPERREL_CDB_FIRST_STAGE_GROUP_AGG,
										  input_rel->relids);
		ctx.partial_rel->reloptkind = RELOPT_OTHER_UPPER_REL;
	}
	else
	{
		ctx.partial_rel = fetch_upper_rel(root, UPPERREL_CDB_FIRST_STAGE_GROUP_AGG,
										  NULL);
	}
	ctx.partial_needed_pathkeys = root->group_pathkeys;
	ctx.partial_sort_pathkeys = root->group_pathkeys;
	/*
	 * CBDB parallel: Set consider_parallel for costs comparison.
	 * Else 2-stage agg with lower costs may lose to 1-stage agg.
	 */
	ctx.partial_rel->consider_parallel = output_rel->consider_parallel;

	ctx.group_tles = get_common_group_tles(target,
										   parse->groupClause,
										   ctx.rollups);

	/*
	 * For twostage grouping sets, we perform grouping sets aggregation in
	 * partial stage and normal aggregation in final stage.
	 *
	 * With this method, there is a problem, i.e., in the final stage of
	 * aggregation, we don't have a way to distinguish which tuple comes from
	 * which grouping set, which is needed for merging the partial results.
	 *
	 * For instance, suppose we have a table t(c1, c2, c3) containing one row
	 * (1, NULL, 3), and we are selecting agg(c3) group by grouping sets
	 * ((c1,c2), (c1)). Then there would be two tuples as partial results for
	 * that row, both are (1, NULL, agg(3)), one is from group by (c1,c2) and
	 * one is from group by (c1). If we cannot tell that the two tuples are
	 * from two different grouping sets, we will merge them incorrectly.
	 *
	 * So we add a hidden column 'GROUPINGSET_ID', representing grouping set
	 * id, to the targetlist of Partial Aggregate node, as well as to the sort
	 * keys and group keys for Finalize Aggregate node. So only tuples coming
	 * from the same grouping set can get merged in the final stage of
	 * aggregation. Note that we need to keep 'GROUPINGSET_ID' at the head of
	 * sort keys in final stage to ensure correctness.
	 *
	 * Below is a plan to illustrate this idea:
	 *
	 * # explain (costs off, verbose)
	 * select c1, c2, c3, avg(c3) from gstest group by grouping sets((c1,c2),(c1),(c2,c3));
	 *                                 QUERY PLAN
	 * ---------------------------------------------------------------------------
	 *  Finalize GroupAggregate
	 *    Output: c1, c2, c3, avg(c3)
	 *    Group Key: (GROUPINGSET_ID()), gstest.c1, gstest.c2, gstest.c3
	 *    ->  Sort
	 *          Output: c1, c2, c3, (PARTIAL avg(c3)), (GROUPINGSET_ID())
	 *          Sort Key: (GROUPINGSET_ID()), gstest.c1, gstest.c2, gstest.c3
	 *          ->  Gather Motion 3:1  (slice1; segments: 3)
	 *                Output: c1, c2, c3, (PARTIAL avg(c3)), (GROUPINGSET_ID())
	 *                ->  Partial GroupAggregate
	 *                      Output: c1, c2, c3, PARTIAL avg(c3), GROUPINGSET_ID()
	 *                      Group Key: gstest.c1, gstest.c2
	 *                      Group Key: gstest.c1
	 *                      Sort Key: gstest.c2, gstest.c3
	 *                        Group Key: gstest.c2, gstest.c3
	 *                      ->  Sort
	 *                            Output: c1, c2, c3
	 *                            Sort Key: gstest.c1, gstest.c2
	 *                            ->  Seq Scan on public.gstest
	 *                                  Output: c1, c2, c3
	 *  Optimizer: Postgres query optimizer
	 * (20 rows)
	 *
	 * Here, we prepare a target list and a corresponding list of SortGroupClauses
	 * for the result of the Partial Aggregate stage.
	 */
	if (parse->groupingSets)
	{
		GroupingSetId *gsetid;
		List	   *grouping_sets_tlist;
		SortGroupClause *gsetcl;
		List	   *gcls;
		List	   *tlist;

		gsetid = makeNode(GroupingSetId);
		grouping_sets_tlist = copyObject(root->processed_tlist);
		ctx.gsetid_sortref = add_gsetid_tlist(grouping_sets_tlist);

		gsetcl = create_gsetid_groupclause(ctx.gsetid_sortref);

		ctx.final_groupClause = lappend(copyObject(parse->groupClause), gsetcl);

		ctx.partial_grouping_target = copyObject(partial_grouping_target);
		if (!list_member(ctx.partial_grouping_target->exprs, gsetid))
			add_column_to_pathtarget(ctx.partial_grouping_target,
									 (Expr *) gsetid, ctx.gsetid_sortref);

		gcls = get_all_rollup_groupclauses(rollups);
		gcls = lappend(gcls, gsetcl);
		tlist = make_tlist_from_pathtarget(ctx.partial_grouping_target);

		/*
		 * The input to the final stage will be sorted by this. It includes the
		 * GROUPINGSET_ID() column.
		 */
		ctx.final_needed_pathkeys = make_pathkeys_for_sortclauses(root, gcls, tlist);
	}
	else
	{
		ctx.partial_grouping_target = partial_grouping_target;
		ctx.final_groupClause = parse->groupClause;
		ctx.final_needed_pathkeys = root->group_pathkeys;
		ctx.gsetid_sortref = 0;
	}
	ctx.final_sort_pathkeys = ctx.final_needed_pathkeys;
	ctx.final_group_tles = get_common_group_tles(ctx.partial_grouping_target,
												 ctx.final_groupClause,
												 NIL);
	ctx.partial_rel->reltarget = ctx.partial_grouping_target;

	/*
	 * All set, generate the two-stage paths.
	 */
	create_two_stage_paths(root, &ctx, input_rel, output_rel, partial_pathlist);

	/*
	 * Aggregates with DISTINCT arguments are more complicated, and are not
	 * handled by create_two_stage_paths() (except for the case of a single
	 * DQA that happens to be collocated with the input, see
	 * add_first_stage_group_agg_path()). Consider ways to implement them,
	 * too.
	 */
	if ((can_hash || parse->groupClause == NIL) &&
		!parse->groupingSets &&
		list_length(agg_costs->distinctAggrefs) > 0)
	{
		/*
		 * Try possible plans for DISTINCT-qualified aggregate.
		 */
		cdb_multi_dqas_info info = {};
		DQAType type = recognize_dqa_type(&ctx);
		switch (type)
		{
		case SINGLE_DQA:
			{
				fetch_single_dqa_info(root, cheapest_path, &ctx, &info);

				add_single_dqa_hash_agg_path(root,
											 cheapest_path,
											 &ctx,
											 output_rel,
											 info.input_proj_target,
											 info.dqa_group_clause,
											 info.dNumDistinctGroups);
			}
			break;
		case SINGLE_DQA_WITHAGG:
			{
				fetch_single_dqa_info(root, cheapest_path, &ctx, &info);

				add_single_mixed_dqa_hash_agg_path(root,
				                                   cheapest_path,
				                                   &ctx,
				                                   output_rel,
				                                   info.input_proj_target,
				                                   info.dqa_group_clause);
			}
			break;
		case MULTI_DQAS:
			{
				fetch_multi_dqas_info(root, cheapest_path, &ctx, &info);
				/*
				 * GPDB_14_MERGE_FIXME: We have done some copy job in
				 * make_partial_grouping_target, so that the agg references
				 * in plan is actually different from
				 * agg_partial_costs->distinctAggrefs. And it has to be
				 * different since we need to compute and set agg_expr_id for
				 * tuple split cases.
				 * However, we need to push multi dqa's filter to tuplesplit
				 * to get the correct result. And thus we need to remove the
				 * filter in aggref referenced by the plan.
				 *
				 * It's not that trivial to fix it perfectly. By manually
				 * removing the origin plan's aggfilter can work around
				 * this problem. We'll look at it again later.
				 */
				ListCell   *lc;
				foreach(lc, root->agginfos)
				{
					AggInfo    *agginfo = (AggInfo *) lfirst(lc);
					Aggref	   *aggref = agginfo->representative_aggref;
					if (aggref->aggdistinct != NIL)
						aggref->aggfilter = NULL;
				}
				add_multi_dqas_hash_agg_path(root,
											 cheapest_path,
											 &ctx,
											 output_rel,
											 &info);
			}
			break;
		case MULTI_DQAS_WITHAGG:
			break;
		default:
			break;
		}
	}
}

/*
 * cdb_create_twostage_distinct_paths
 *
 * Alternative entry point for DISTINCT planning.
 *
 * This is basically an extension of the function create_distinct_paths() in
 * planner.c.  It creates two-stage Aggregate Paths to implement DISTINCT.
 * The caller already constructed a Paths for one-stage plans.
 *
 * 'input_rel' is usually the result of query_planner(), but it can also be
 * the result of windowing and/or GROUP BY planning, if the query contains
 * both DISTINCT and GROUP BY/windowing.
 */
void
cdb_create_twostage_distinct_paths(PlannerInfo *root,
								   RelOptInfo *input_rel,
								   RelOptInfo *output_rel,
								   PathTarget *target,
								   double dNumGroupsTotal)
{
	Query	   *parse = root->parse;
	Path	   *cheapest_path = input_rel->cheapest_total_path;
	AggClauseCosts zero_agg_costs;
	cdb_agg_planning_context ctx;
	bool		allow_sort;
	bool		allow_hash;

	/*
	 * We are currently unwilling to redistribute a gathered intermediate
	 * across the cluster.  This might change one day.
	 */
	if (!CdbPathLocus_IsPartitioned(cheapest_path->locus))
		return;

	/*
	 * Is the input hashable / sortable?
	 */
	allow_sort = grouping_is_sortable(parse->distinctClause);
	if (parse->hasDistinctOn || !enable_hashagg)
		allow_hash = false;		/* policy-based decision not to hash */
	else if (!grouping_is_hashable(parse->distinctClause))
		allow_hash = false;
	else
		allow_hash = true;

	/* Set up a dummy AggClauseCosts struct. There are no aggregates. */
	memset(&zero_agg_costs, 0, sizeof(zero_agg_costs));

	memset(&ctx, 0, sizeof(ctx));
	ctx.can_sort = allow_sort;
	ctx.can_hash = allow_hash;
	ctx.target = target;
	ctx.partial_grouping_target = target;
	ctx.dNumGroupsTotal = dNumGroupsTotal;
	ctx.agg_costs = &zero_agg_costs;
	ctx.agg_partial_costs = &zero_agg_costs;
	ctx.agg_final_costs = &zero_agg_costs;
	ctx.rollups = NIL;
	ctx.partial_rel = fetch_upper_rel(root, UPPERREL_CDB_FIRST_STAGE_DISTINCT, NULL);
	/*
	 * CBDB parallel: Set consider_parallel for costs comparison.
	 * Else 2-stage agg with lower costs may lose to 1-stage agg.
	 */
	ctx.partial_rel->consider_parallel = output_rel->consider_parallel;

	/*
	 * Set up these fields to look like a query with a GROUP BY on all the
	 * DISTINCT columns. No HAVING or aggregates; the DISTINCT processing happens
	 * logically after grouping and aggregation, so those have already been
	 * handled in the grouping stage.
	 */
	ctx.hasAggs = false;
	ctx.groupingSets = NIL;
	ctx.havingQual = NULL;
	ctx.groupClause = parse->distinctClause;
	ctx.group_tles = get_common_group_tles(target, parse->distinctClause, NIL);
	ctx.final_groupClause = ctx.groupClause;
	ctx.final_group_tles = ctx.group_tles;
	ctx.gsetid_sortref = 0;

	if (ctx.can_sort)
	{
		/*
		 * First, if we have any adequately-presorted paths, just stick a
		 * Unique node on those.  Then consider doing an explicit sort of the
		 * cheapest input path and Unique'ing that.
		 *
		 * When we have DISTINCT ON, we must sort by the more rigorous of
		 * DISTINCT and ORDER BY, else it won't have the desired behavior.
		 * Also, if we do have to do an explicit sort, we might as well use
		 * the more rigorous ordering to avoid a second sort later.  (Note
		 * that the parser will have ensured that one clause is a prefix of
		 * the other.)
		 */
		if (parse->hasDistinctOn &&
			list_length(root->distinct_pathkeys) <
			list_length(root->sort_pathkeys))
			ctx.partial_needed_pathkeys = root->sort_pathkeys;
		else
			ctx.partial_needed_pathkeys = root->distinct_pathkeys;

		/* For explicit-sort case, always use the more rigorous clause */
		if (list_length(root->distinct_pathkeys) <
			list_length(root->sort_pathkeys))
		{
			ctx.partial_sort_pathkeys = root->sort_pathkeys;
			/* Assert checks that parser didn't mess up... */
			Assert(pathkeys_contained_in(root->distinct_pathkeys,
										 ctx.partial_sort_pathkeys));
		}
		else
			ctx.partial_sort_pathkeys = root->distinct_pathkeys;
		ctx.final_needed_pathkeys = ctx.partial_needed_pathkeys;
		ctx.final_sort_pathkeys = ctx.partial_sort_pathkeys;
	}

	/*
	 * All set, generate the two-stage paths.
	 */
	create_two_stage_paths(root, &ctx, input_rel, output_rel, NIL);
}

/*
 * Guts of GROUP BY and DISTINCT planning.
 */
static void
create_two_stage_paths(PlannerInfo *root, cdb_agg_planning_context *ctx,
					   RelOptInfo *input_rel, RelOptInfo *output_rel, List *partial_pathlist)
{
	Path	   *cheapest_path = input_rel->cheapest_total_path;

	/*
	 * Consider ways to do the first Aggregate stage.
	 *
	 * The first stage's output is Partially Aggregated. The paths are
	 * collected to the ctx->partial_rel, by calling add_path().
	 * These partially aggregated paths are considered
	 * more like MPP paths in Greenplum in general.
	 *
	 * First consider sorted Aggregate paths.
	 */
	if (ctx->can_sort)
	{
		ListCell   *lc;

		foreach(lc, input_rel->pathlist)
		{
			Path	   *path = (Path *) lfirst(lc);
			bool		is_sorted;

			/*
			 * If the input is neatly distributed along the GROUP BY columns,
			 * there's no point in a two-stage plan. The code in planner.c
			 * already created the straightforward one-stage plan.
			 */
			if (cdbpathlocus_collocates_tlist(root, path->locus, ctx->group_tles))
				continue;

			/*
			 * Consider input paths that are already sorted, and the one with
			 * the lowest total cost.
			 */
			is_sorted = pathkeys_contained_in(ctx->partial_needed_pathkeys,
											  path->pathkeys);
			if (path == cheapest_path || is_sorted)
				add_first_stage_group_agg_path(root, path, is_sorted, ctx);
		}
	}

	/*
	 * Consider Hash Aggregate over the cheapest input path.
	 *
	 * Hashing is not possible with DQAs.
	 */
	if (ctx->can_hash &&
		list_length(ctx->agg_costs->distinctAggrefs) == 0)
	{
		/*
		 * If the input is neatly distributed along the GROUP BY columns,
		 * there's no point in a two-stage plan. The code in planner.c already
		 * created the straightforward one-stage plan.
		 */
		if (!cdbpathlocus_collocates_tlist(root, cheapest_path->locus, ctx->group_tles))
			add_first_stage_hash_agg_path(root, cheapest_path, ctx);
	}

	if (partial_pathlist)
	{
		ListCell   *lc;

		foreach(lc, partial_pathlist)
		{
			Path       *path = (Path *) lfirst(lc);

			if (cdbpathlocus_collocates_tlist(root, path->locus, ctx->group_tles))
				continue;
			add_partial_path(ctx->partial_rel, path);
		}
	}

	if (ctx->partial_rel->fdwroutine &&
		ctx->partial_rel->fdwroutine->GetForeignUpperPaths &&
		ctx->partial_rel->segSeverids)
	{
		GroupPathExtraData extra;
		FdwRoutine *fdwroutine = ctx->partial_rel->fdwroutine;
		ListCell *lc;

		extra.patype = PARTITIONWISE_AGGREGATE_NONE;
		extra.havingQual = NULL;

		foreach(lc, ctx->partial_rel->reltarget->exprs)
		{
			Expr *expr;

			expr = lfirst(lc);

			if (IsA(expr, Aggref))
				((Aggref*)expr)->aggsplit = AGGSPLIT_SIMPLE;
		}

		fdwroutine->GetForeignUpperPaths(root, UPPERREL_GROUP_AGG, input_rel,
										 ctx->partial_rel, &extra);
	}

#ifdef SERVERLESS
	/*
	 * Incremental AGG based on materialized views.	
	 */
	bool	path_changed = false;

	if (enable_answer_query_using_materialized_views && /* Use this GUC for now. */
		try_append_agg(root->parse))
	{
		path_changed = expand_append_agg(root, ctx);
	}
	
#endif

	/*
	 * We now have partially aggregated paths in ctx->partial_rel. Consider
	 * different ways of performing the Finalize Aggregate stage.
	 */
	if (ctx->partial_rel->pathlist)
	{
		Path	   *cheapest_first_stage_path;

#ifdef SERVERLESS
		/* Do not change cheapest if we assign, trust Append with agg always win. */
		if (!path_changed)
			set_cheapest(ctx->partial_rel);
#else
		set_cheapest(ctx->partial_rel);
#endif
		cheapest_first_stage_path = ctx->partial_rel->cheapest_total_path;

		if (!IsA(cheapest_first_stage_path, ForeignPath))
		{
			ListCell *lc;

			foreach(lc, ctx->partial_rel->reltarget->exprs)
			{
				Expr *expr;

				expr = lfirst(lc);

				if (IsA(expr, Aggref))
					((Aggref*)expr)->aggsplit = AGGSPLIT_INITIAL_SERIAL;
			}
		}

		if (ctx->can_sort)
		{
			ListCell   *lc;

			foreach(lc, ctx->partial_rel->pathlist)
			{
				Path	   *path = (Path *) lfirst(lc);
				bool		is_sorted;

				/*
				 * In two-stage GROUPING SETS paths, the second stage's grouping
				 * will include GROUPINGSET_ID(), which is not included in
				 * root->pathkeys. The first stage's sort order does not include
				 * that, so we know it's not sorted.
				 */
				if (!root->parse->groupingSets)
					is_sorted = pathkeys_contained_in(ctx->final_needed_pathkeys,
													  path->pathkeys);
				else
					is_sorted = false;
#ifdef SERVERLESS
				/*
				 * We eager append agg if there was, the previous normal agg with sort may break
				 * that case. Though the cost is same but different order matters.
				 * Bypass others to make sure that.
				 */
				if (path_changed && (path != cheapest_first_stage_path))
					continue;
#endif
				if (path == cheapest_first_stage_path || is_sorted)
				{
					add_second_stage_group_agg_path(root, path, is_sorted,
													ctx, output_rel, false);
				}
			}
		}

		if (ctx->can_hash && list_length(ctx->agg_costs->distinctAggrefs) == 0)
		{
			add_second_stage_hash_agg_path(root, cheapest_first_stage_path,
										   ctx, output_rel, false);
		}
	}

	/*
	 * Same like above, but for partial paths in partital_rel,
	 * that's parallel agg with multiple workers.
	 */
	if (ctx->partial_rel->partial_pathlist)
	{
		Path	   *cheapest_first_stage_path;

		cheapest_first_stage_path = linitial(ctx->partial_rel->partial_pathlist);

		if (ctx->can_sort)
		{
			ListCell   *lc;

			foreach(lc, ctx->partial_rel->partial_pathlist)
			{
				Path	   *path = (Path *) lfirst(lc);
				bool		is_sorted;

				/*
				 * In two-stage GROUPING SETS paths, the second stage's grouping
				 * will include GROUPINGSET_ID(), which is not included in
				 * root->pathkeys. The first stage's sort order does not include
				 * that, so we know it's not sorted.
				 */
				if (!root->parse->groupingSets)
					is_sorted = pathkeys_contained_in(ctx->final_needed_pathkeys,
													  path->pathkeys);
				else
					is_sorted = false;
				if (path == cheapest_first_stage_path || is_sorted)
				{
					add_second_stage_group_agg_path(root, path, is_sorted,
													ctx, output_rel, true);
				}
			}
		}

		if (ctx->can_hash && list_length(ctx->agg_costs->distinctAggrefs) == 0)
		{
			add_second_stage_hash_agg_path(root, cheapest_first_stage_path,
										   ctx, output_rel, true);
		}
	}
}


/*
 * Add a TargetEntry node of type GroupingSetId to the tlist.
 * Return its ressortgroupref.
 */
static Index
add_gsetid_tlist(List *tlist)
{
	TargetEntry *tle;
	GroupingSetId *gsetid;
	ListCell *lc;

	foreach(lc, tlist)
	{
		tle = lfirst_node(TargetEntry, lc);
		if (IsA(tle->expr, GroupingSetId))
			elog(ERROR, "GROUPINGSET_ID already exists in tlist");
	}

	gsetid = makeNode(GroupingSetId);
	tle = makeTargetEntry((Expr *)gsetid, list_length(tlist) + 1,
			"GROUPINGSET_ID", true);
	assignSortGroupRef(tle, tlist);
	tlist = lappend(tlist, tle);

	return tle->ressortgroupref;
}

/*
 * Add a SortGroupClause node to the groupClause representing the GroupingSetId.
 * Note we insert the new node to the head of groupClause.
 */
static SortGroupClause *
create_gsetid_groupclause(Index groupref)
{
	SortGroupClause *gc;
	Oid         sortop;
	Oid         eqop;
	bool        hashable;

	get_sort_group_operators(INT4OID,
			false, true, false,
			&sortop, &eqop, NULL,
			&hashable);

	gc = makeNode(SortGroupClause);
	gc->tleSortGroupRef = groupref;
	gc->eqop = eqop;
	gc->sortop = sortop;
	gc->nulls_first = false;
	gc->hashable = hashable;

	return gc;
}

static List *
strip_gsetid_from_pathkeys(Index gsetid_sortref, List *pathkeys)
{
	ListCell   *lc;
	List	   *new_pathkeys;

	if (gsetid_sortref == 0)
		return pathkeys;

	new_pathkeys = NIL;
	foreach(lc, pathkeys)
	{
		PathKey	   *pathkey = lfirst(lc);
		EquivalenceClass *eclass = pathkey->pk_eclass;

		if (eclass->ec_sortref == gsetid_sortref)
		{
			/*
			 * The GROUPINGSETID_EXPR() should be the last pathkey. But just in
			 * case it's not, any columns after it won't be in right order i
			 * we remove it from the middle.
			 */
			break;
		}

		new_pathkeys = lappend(new_pathkeys, pathkey);
	}
	return new_pathkeys;
}

/*
 * Create a partially aggregated path from given input 'path' by sorting (if
 * input isn't sorted already).
 */
static void
add_first_stage_group_agg_path(PlannerInfo *root,
							   Path *path,
							   bool is_sorted,
							   cdb_agg_planning_context *ctx)
{
	DQAType     dqa_type;

	/*
	 * DISTINCT-qualified aggregates are accepted only in the special
	 * case that the input happens to be collocated with the DISTINCT
	 * argument.
	 */
	if (ctx->agg_costs->distinctAggrefs)
	{
		cdb_multi_dqas_info info = {};
		List	   *dqa_group_tles;

		dqa_type = recognize_dqa_type(ctx);

		/* For the query:
		 *     select count(distinct a), sum(b), sum(c) from t;
		 * If t is distributed by (a), we can also use multi stage
		 * agg because two same a cannot be in different segments.
		 * So we should also consider SINGLE_DQA_WITHAGG here.
		 */
		if (dqa_type != SINGLE_DQA && dqa_type != SINGLE_DQA_WITHAGG)
			return;

		fetch_single_dqa_info(root, path, ctx, &info);

		/*
		 * If subpath is projection capable, we do not want to generate a
		 * projection plan. The reason is that the projection plan does not
		 * constrain a child tlist when it creates subplan. Thus, GROUP BY expr
		 * may not be found in the scan targetlist.
		 */
		path = apply_projection_to_path(root, path->parent, path, info.input_proj_target);

		/* If the input distribution matches the distinct, we can proceed */
		dqa_group_tles = get_common_group_tles(info.input_proj_target,
											   info.dqa_group_clause,
											   ctx->rollups);
		if (!cdbpathlocus_collocates_tlist(root, path->locus, dqa_group_tles))
			return;
	}

	if (!is_sorted)
	{
		path = (Path *) create_sort_path(root,
										 ctx->partial_rel,
										 path,
										 ctx->partial_sort_pathkeys,
										 -1.0);
	}

	if (ctx->groupingSets)
	{
		/*
		 * We have grouping sets, possibly with aggregation.  Make
		 * a GroupingSetsPath.
		 *
		 * NOTE: We don't pass the HAVING quals here. HAVING quals can
		 * only be evaluated in the Finalize stage, after computing the
		 * final aggregate values.
		 */
		Path	   *first_stage_agg_path;

		first_stage_agg_path =
			(Path *) create_groupingsets_path(root,
											  ctx->partial_rel,
											  path,
											  AGGSPLIT_INITIAL_SERIAL,
											  NIL,
											  AGG_SORTED,
											  ctx->rollups,
											  ctx->agg_partial_costs);
		add_path(ctx->partial_rel, first_stage_agg_path, root);
	}
	else if (ctx->hasAggs || ctx->groupClause)
	{
		add_path(ctx->partial_rel,
			(Path *) create_agg_path(root,
									 ctx->partial_rel,
									 path,
									 ctx->partial_grouping_target,
									 ctx->groupClause ? AGG_SORTED : AGG_PLAIN,
									 ctx->hasAggs ? AGGSPLIT_INITIAL_SERIAL : AGGSPLIT_SIMPLE,
									 false, /* streaming */
									 ctx->groupClause,
									 NIL,
									 ctx->agg_partial_costs,
									 estimate_num_groups_on_segment(ctx->dNumGroupsTotal,
																	path->rows, path->locus)),
				 root);
	}
	else
	{
		Assert(false);
	}
}

/*
 * Create Finalize Aggregate path, from a partially aggregated input.
 * If is_partial is true, add path to partital_pathlist.
 */
static void
add_second_stage_group_agg_path(PlannerInfo *root,
								Path *initial_agg_path,
								bool is_sorted,
								cdb_agg_planning_context *ctx,
								RelOptInfo *output_rel,
								bool is_partial)
{
	Path	   *path;
	CdbPathLocus singleQE_locus;
	CdbPathLocus group_locus;
	bool		need_redistribute;

	/* The input should be distributed, otherwise no point in a two-stage Agg. */
	Assert(CdbPathLocus_IsPartitioned(initial_agg_path->locus));

	group_locus = choose_grouping_locus(root,
										initial_agg_path,
										ctx->final_group_tles,
										&need_redistribute);
	Assert(need_redistribute);

	/*
	 * We consider two different loci for the final result:
	 *
	 * 1. Redistribute the partial result according to GROUP BY columns,
	 *    Sort, Aggregate.
	 *
	 * 2. Gather the partial result to a single process, Sort if needed,
	 *    Aggregate.
	 *
	 * Redistributing the partial result has the advantage that the Finalize
	 * stage can run in parallel. The downside is that a Redistribute Motion
	 * loses any possible input order, so we'll need an extra Sort step even
	 * if the input was already ordered. Also, gathering the partial result
	 * directly to the QD will avoid one Motion, if the final result is needed
	 * in the QD anyway.
	 *
	 * We generate a Path for both, and let add_path() decide which ones
	 * to keep.
	 */
	/* Alternative 1: Redistribute -> Sort -> Agg */
	if (CdbPathLocus_IsHashed(group_locus))
	{
		path = initial_agg_path;

		path = cdbpath_create_motion_path(root, path, NIL,
											 false, group_locus);

		if (ctx->final_sort_pathkeys)
			path = (Path *) create_sort_path(root,
											 output_rel,
											 path,
											 ctx->final_sort_pathkeys,
											 -1.0);

		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										ctx->target,
										(ctx->final_groupClause ? AGG_SORTED : AGG_PLAIN),
										ctx->hasAggs ? AGGSPLIT_FINAL_DESERIAL : AGGSPLIT_SIMPLE,
										false, /* streaming */
										ctx->final_groupClause,
										ctx->havingQual,
										ctx->agg_final_costs,
										ctx->dNumGroupsTotal);
		path->pathkeys = strip_gsetid_from_pathkeys(ctx->gsetid_sortref, path->pathkeys);

		if (!is_partial)
			add_path(output_rel, path, root);
		else
			add_partial_path(output_rel, path);
	}

	/*
	 * Alternative 2: [Sort if needed] -> Gather -> Agg
	 */
	CdbPathLocus_MakeSingleQE(&singleQE_locus, getgpsegmentCount());

	path = initial_agg_path;
	if (!is_sorted)
	{
		path = (Path *) create_sort_path(root,
										 output_rel,
										 path,
										 ctx->final_sort_pathkeys,
										 -1.0);
	}

	path = cdbpath_create_motion_path(root, path,
									  path->pathkeys,
									  false, singleQE_locus);

	path = (Path *) create_agg_path(root,
								output_rel,
								path,
								ctx->target,
								(ctx->final_groupClause ? AGG_SORTED : AGG_PLAIN),
								ctx->hasAggs ? AGGSPLIT_FINAL_DESERIAL : AGGSPLIT_SIMPLE,
								false, /* streaming */
								ctx->final_groupClause,
								ctx->havingQual,
								ctx->agg_final_costs,
								ctx->dNumGroupsTotal);
	path->pathkeys = strip_gsetid_from_pathkeys(ctx->gsetid_sortref, path->pathkeys);

	if (!is_partial)
		add_path(output_rel, path, root);
	else
		add_partial_path(output_rel, path);
}

/*
 * Create a partially aggregated path from given input 'path' by hashing.
 */
static void
add_first_stage_hash_agg_path(PlannerInfo *root,
							  Path *path,
							  cdb_agg_planning_context *ctx)
{
	Query	   *parse = root->parse;
	Path       *first_stage_agg_path = NULL;
	double		dNumGroups;

	dNumGroups = estimate_num_groups_on_segment(ctx->dNumGroupsTotal,
												path->rows, path->locus);


	if (parse->groupingSets && ctx->new_rollups)
	{
		first_stage_agg_path =
			(Path *) create_groupingsets_path(root,
											  ctx->partial_rel,
											  path,
											  AGGSPLIT_INITIAL_SERIAL,
											  NIL,
											  ctx->strat,
											  ctx->new_rollups,
											  ctx->agg_partial_costs);
		CdbPathLocus_MakeStrewn(&(first_stage_agg_path->locus),
								CdbPathLocus_NumSegments(first_stage_agg_path->locus),
								path->parallel_workers);
		add_path(ctx->partial_rel, first_stage_agg_path, root);
	}
	else
	{
		add_path(ctx->partial_rel,
				 (Path *) create_agg_path(root,
										  ctx->partial_rel,
										  path,
										  ctx->partial_grouping_target,
										  AGG_HASHED,
										  ctx->hasAggs ? AGGSPLIT_INITIAL_SERIAL : AGGSPLIT_SIMPLE,
										  false, /* streaming */
										  ctx->groupClause,
										  NIL,
										  ctx->agg_partial_costs,
										  dNumGroups),
				 root);
	}
}

/*
 * Create Finalize Aggregate path from a partially aggregated input by hashing.
 * If is_partial is true, add path to partital_pathlist.
 */
static void
add_second_stage_hash_agg_path(PlannerInfo *root,
							   Path *initial_agg_path,
							   cdb_agg_planning_context *ctx,
							   RelOptInfo *output_rel,
							   bool is_partial)
{
	CdbPathLocus group_locus;
	bool		needs_redistribute;
	double		dNumGroups;
	Size		hashentrysize;

	group_locus = choose_grouping_locus(root,
										initial_agg_path,
										ctx->final_group_tles,
										&needs_redistribute);
	/* if no redistribution is needed, why are we here? */
	Assert(needs_redistribute);

	/*
	 * Calculate the number of groups in the second stage, per segment.
	 */
	if (CdbPathLocus_IsPartitioned(group_locus))
		dNumGroups = clamp_row_est(ctx->dNumGroupsTotal /
								   CdbPathLocus_NumSegments(group_locus));
	else
		dNumGroups = ctx->dNumGroupsTotal;

	/* Would the hash table fit in memory? */
	hashentrysize = MAXALIGN(initial_agg_path->pathtarget->width) + MAXALIGN(SizeofMinimalTupleHeader);

	if (enable_hashagg_disk ||
		hashentrysize * dNumGroups < work_mem * 1024L)
	{
		Path	   *path = initial_agg_path;

		if (needs_redistribute)
			path = cdbpath_create_motion_path(root, path, NIL, false,
											  group_locus);

		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										ctx->target,
										AGG_HASHED,
										ctx->hasAggs ? AGGSPLIT_FINAL_DESERIAL : AGGSPLIT_SIMPLE,
										false, /* streaming */
										ctx->final_groupClause,
										ctx->havingQual,
										ctx->agg_final_costs,
										dNumGroups);
		if (!is_partial)
			add_path(output_rel, path, root);
		else
			add_partial_path(output_rel, path);
	}

	/*
	 * Like in the Group Agg case, if the final result needs to be brough to
	 * the QD, we consider doing the Finalize Aggregate in the QD directly to
	 * avoid another Gather Motion above the Finalize Aggregate. It's less
	 * likely to be a win than with sorted Aggs, because a hashed agg won't
	 * benefit from preserving the input order, but it can still be cheaper if
	 * there are only a few groups.
	 */
	if (!CdbPathLocus_IsBottleneck(group_locus) &&
		CdbPathLocus_IsBottleneck(root->final_locus))
	{
		CdbPathLocus singleQE_locus;
		CdbPathLocus_MakeSingleQE(&singleQE_locus, getgpsegmentCount());

		hashentrysize = MAXALIGN(initial_agg_path->pathtarget->width) + MAXALIGN(SizeofMinimalTupleHeader);
		if (hashentrysize * ctx->dNumGroupsTotal <= work_mem * 1024L)
		{
			Path	   *path = initial_agg_path;

			path = cdbpath_create_motion_path(root, path,
											  NIL, false,
											  singleQE_locus);

			path = (Path *) create_agg_path(root,
											output_rel,
											path,
											ctx->target,
											AGG_HASHED,
											ctx->hasAggs ? AGGSPLIT_FINAL_DESERIAL : AGGSPLIT_SIMPLE,
											false, /* streaming */
											ctx->final_groupClause,
											ctx->havingQual,
											ctx->agg_final_costs,
											ctx->dNumGroupsTotal);

			if (!is_partial)
				add_path(output_rel, path, root);
			else
				add_partial_path(output_rel, path);
		}
	}
}

static Node *
strip_aggdistinct_mutator(Node *node, void *context)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, Aggref))
	{
		Aggref	   *newAggref = (Aggref *) copyObject(node);

		newAggref->aggdistinct = NIL;

		node = (Node *) newAggref;
	}
	return expression_tree_mutator(node, strip_aggdistinct_mutator, context);
}

static PathTarget *
strip_aggdistinct(PathTarget *target)
{
	PathTarget *result;

	result = copy_pathtarget(target);
	result->exprs = (List *) strip_aggdistinct_mutator((Node *) result->exprs, NULL);

	return result;
}

/*
 * Create Paths for an Aggregate with one DISTINCT-qualified aggregate and
 * multi normal aggregate.
 */
static void add_single_mixed_dqa_hash_agg_path(PlannerInfo *root,
                                               Path *path,
                                               cdb_agg_planning_context *ctx,
                                               RelOptInfo *output_rel,
                                               PathTarget *input_target,
                                               List       *dqa_group_clause)
{
	List	   *dqa_group_tles;
	CdbPathLocus distinct_locus;
	bool		distinct_need_redistribute;
	CdbPathLocus singleQE_locus;

	if (!gp_enable_agg_distinct)
		return;

	if (ctx->groupClause)
		return;

	/*
	 * If subpath is projection capable, we do not want to generate a
	 * projection plan. The reason is that the projection plan does not
	 * constrain a child tlist when it creates subplan. Thus, GROUP BY expr
	 * may not be found in the scan targetlist.
	 */
	path = apply_projection_to_path(root, path->parent, path, input_target);

	dqa_group_tles = get_common_group_tles(input_target, dqa_group_clause, NIL  );
	distinct_locus = choose_grouping_locus(root, path,
										   dqa_group_tles,
										   &distinct_need_redistribute);

	if (!CdbPathLocus_IsPartitioned(distinct_locus))
		return;

	if (distinct_need_redistribute)
		path = cdbpath_create_motion_path(root, path, NIL, false,
										  distinct_locus);

	path = (Path *) create_agg_path(root,
									output_rel,
									path,
									ctx->partial_grouping_target,
									AGG_PLAIN,
									AGGSPLIT_INITIAL_SERIAL,
									false, /* streaming */
									ctx->groupClause,
									NIL,
									ctx->agg_partial_costs, /* FIXME */
									estimate_num_groups_on_segment(ctx->dNumGroupsTotal,
																   path->rows, path->locus));

	CdbPathLocus_MakeSingleQE(&singleQE_locus, getgpsegmentCount());
	path = cdbpath_create_motion_path(root, path, NIL, false, singleQE_locus);

	path = (Path *) create_agg_path(root,
									output_rel,
									path,
									ctx->target,
									AGG_PLAIN,
									AGGSPLIT_FINAL_DESERIAL,
									false, /* streaming */
									ctx->groupClause,
									ctx->havingQual,
									ctx->agg_final_costs,
									ctx->dNumGroupsTotal);
	add_path(output_rel, path, root);
}

/*
 * Create Paths for an Aggregate with one DISTINCT-qualified aggregate.
 */
static void
add_single_dqa_hash_agg_path(PlannerInfo *root,
							 Path *path,
							 cdb_agg_planning_context *ctx,
							 RelOptInfo *output_rel,
							 PathTarget *input_target,
							 List       *dqa_group_clause,
							 double dNumDistinctGroups)
{
	List	   *dqa_group_tles;
	int			num_input_segments;
	List	   *group_tles;
	CdbPathLocus group_locus;
	double		dNumGroups;
	bool		group_need_redistribute;
	CdbPathLocus distinct_locus;
	bool		distinct_need_redistribute;

	if (!gp_enable_agg_distinct)
		return;

	/*
	 * If subpath is projection capable, we do not want to generate a
	 * projection plan. The reason is that the projection plan does not
	 * constrain a child tlist when it creates subplan. Thus, GROUP BY expr
	 * may not be found in the scan targetlist.
	 */
	path = apply_projection_to_path(root, path->parent, path, input_target);

	if (CdbPathLocus_IsPartitioned(path->locus))
		num_input_segments = CdbPathLocus_NumSegments(path->locus);
	else
		num_input_segments = 1;

	dqa_group_tles = get_common_group_tles(input_target, dqa_group_clause, NIL);
	distinct_locus = choose_grouping_locus(root, path,
										   dqa_group_tles,
										   &distinct_need_redistribute);

	/*
	 * Calculate the number of groups in the final stage, per segment.
	 * group_locus is the corresponding locus for the final stage.
	 */
	group_tles = get_common_group_tles(input_target, ctx->groupClause, NIL);
	group_locus = choose_grouping_locus(root, path,
										group_tles,
										&group_need_redistribute);
	if (CdbPathLocus_IsPartitioned(group_locus))
		dNumGroups = clamp_row_est(ctx->dNumGroupsTotal /
								   CdbPathLocus_NumSegments(path->locus));
	else
		dNumGroups = ctx->dNumGroupsTotal;

	if (!distinct_need_redistribute || !group_need_redistribute)
	{
		Path *orig_path = path;
		double		input_rows = path->rows;

		/*
		 * 1. If the input's locus matches the DISTINCT, but not GROUP BY:
		 *
		 *  HashAggregate
		 *     -> Redistribute (according to GROUP BY)
		 *         -> HashAggregate (to eliminate duplicates)
		 *             -> input (hashed by GROUP BY + DISTINCT)
		 *
		 * 2. If the input's locus matches the GROUP BY:
		 *
		 *  HashAggregate (to aggregate)
		 *     -> HashAggregate (to eliminate duplicates)
		 *           -> input (hashed by GROUP BY)
		 *
		 * The main planner should already have created the single-stage
		 * Group Agg path.
		 *
		 * XXX: not sure if this makes sense. If hash distinct is a good
		 * idea, why doesn't PostgreSQL's agg node implement that?
		 */
		path = (Path *) create_agg_path(root,
										output_rel,
										orig_path,
										input_target,
										AGG_HASHED,
										AGGSPLIT_SIMPLE,
										false, /* streaming */
										dqa_group_clause,
										NIL,
										ctx->agg_partial_costs, /* FIXME */
										clamp_row_est(dNumDistinctGroups / (double) num_input_segments));

		if (group_need_redistribute)
			path = cdbpath_create_motion_path(root, path, NIL, false,
											  group_locus);

		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										ctx->target,
										ctx->groupClause ? AGG_HASHED : AGG_PLAIN,
										AGGSPLIT_DEDUPLICATED,
										false, /* streaming */
										ctx->groupClause,
										ctx->havingQual,
										ctx->agg_final_costs,
										dNumGroups);
		add_path(output_rel, path, root);

		/*
		 *  Finalize Aggregate
		 *     -> Gather Motion
		 *          -> Partial Aggregate
		 *              -> HashAggregate, to remove duplicates
		 *                          -> input
		 */
		path = (Path *) create_agg_path(root,
										output_rel,
										orig_path,
										input_target,
										AGG_HASHED,
										AGGSPLIT_SIMPLE,
										false, /* streaming */
										dqa_group_clause,
										NIL,
										ctx->agg_partial_costs, /* FIXME */
										clamp_row_est(dNumDistinctGroups / CdbPathLocus_NumSegments(distinct_locus)));

		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										strip_aggdistinct(ctx->partial_grouping_target),
										ctx->groupClause ? AGG_HASHED : AGG_PLAIN,
										AGGSPLIT_INITIAL_SERIAL | AGGSPLITOP_DEDUPLICATED,
										false, /* streaming */
										ctx->groupClause,
										NIL,
										ctx->agg_partial_costs,
										estimate_num_groups_on_segment(ctx->dNumGroupsTotal, input_rows, path->locus));
		if (group_need_redistribute)
			path = cdbpath_create_motion_path(root, path, NIL, false,
										  group_locus);

		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										ctx->target,
										ctx->groupClause ? AGG_HASHED : AGG_PLAIN,
										AGGSPLIT_FINAL_DESERIAL | AGGSPLITOP_DEDUPLICATED,
										false, /* streaming */
										ctx->groupClause,
										ctx->havingQual,
										ctx->agg_final_costs,
										dNumGroups);

		/*
		 * Try disable other paths if users are eager this one.
		 */
		if (gp_eager_distinct_dedup)
		{
			ListCell *lc;
			foreach(lc, output_rel->pathlist)
			{
				Path *oldpath = (Path *) lfirst(lc);
				oldpath->total_cost += disable_cost;
			}
		}
		add_path(output_rel, path, root);
	}
	else if (CdbPathLocus_IsHashed(group_locus))
	{
		double		input_rows = path->rows;

		/*
		 *  HashAgg (to aggregate)
		 *     -> HashAgg (to eliminate duplicates)
		 *          -> Redistribute (according to GROUP BY)
		 *               -> Streaming HashAgg (to eliminate duplicates)
		 *                    -> input
		 *
		 * It may seem silly to have two Aggs on top of each other like this,
		 * but the Agg node can't do DISTINCT-aggregation by hashing at the
		 * moment. So we have to do it with two separate Aggs steps.
		 */
		if (gp_enable_dqa_pruning)
			path = (Path *) create_agg_path(root,
											output_rel,
											path,
											input_target,
											AGG_HASHED,
											AGGSPLIT_SIMPLE,
											true, /* streaming */
											dqa_group_clause,
											NIL,
											ctx->agg_partial_costs, /* FIXME */
											estimate_num_groups_on_segment(dNumDistinctGroups,
																		   input_rows, path->locus));

		path = cdbpath_create_motion_path(root, path, NIL, false,
										  group_locus);
		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										input_target,
										AGG_HASHED,
										AGGSPLIT_SIMPLE,
										false, /* streaming */
										dqa_group_clause,
										NIL,
										ctx->agg_partial_costs, /* FIXME */
										clamp_row_est(dNumDistinctGroups / CdbPathLocus_NumSegments(group_locus)));

		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										ctx->target,
										ctx->groupClause ? AGG_HASHED : AGG_PLAIN,
										AGGSPLIT_DEDUPLICATED,
										false, /* streaming */
										ctx->groupClause,
										ctx->havingQual,
										ctx->agg_final_costs,
										dNumGroups);
		add_path(output_rel, path, root);
	}
	else if (CdbPathLocus_IsHashed(distinct_locus))
	{
		double			input_rows = path->rows;

		/*
		 *  Finalize Aggregate
		 *     -> Gather Motion
		 *          -> Partial Aggregate
		 *              -> HashAggregate, to remove duplicates
		 *                  -> Redistribute Motion (according to DISTINCT arg)
		 *                      -> Streaming HashAgg (to eliminate duplicates)
		 *                          -> input
		 */
		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										input_target,
										AGG_HASHED,
										AGGSPLIT_SIMPLE,
										true, /* streaming */
										dqa_group_clause,
										NIL,
										ctx->agg_partial_costs, /* FIXME */
										estimate_num_groups_on_segment(dNumDistinctGroups,
																	   input_rows, path->locus));

		path = cdbpath_create_motion_path(root, path, NIL, false,
										  distinct_locus);
		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										input_target,
										AGG_HASHED,
										AGGSPLIT_SIMPLE,
										false, /* streaming */
										dqa_group_clause,
										NIL,
										ctx->agg_partial_costs, /* FIXME */
										clamp_row_est(dNumDistinctGroups / CdbPathLocus_NumSegments(distinct_locus)));

		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										strip_aggdistinct(ctx->partial_grouping_target),
										ctx->groupClause ? AGG_HASHED : AGG_PLAIN,
										AGGSPLIT_INITIAL_SERIAL | AGGSPLITOP_DEDUPLICATED,
										false, /* streaming */
										ctx->groupClause,
										NIL,
										ctx->agg_partial_costs,
										estimate_num_groups_on_segment(ctx->dNumGroupsTotal, input_rows, path->locus));
		path = cdbpath_create_motion_path(root, path, NIL, false,
										  group_locus);

		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										ctx->target,
										ctx->groupClause ? AGG_HASHED : AGG_PLAIN,
										AGGSPLIT_FINAL_DESERIAL | AGGSPLITOP_DEDUPLICATED,
										false, /* streaming */
										ctx->groupClause,
										ctx->havingQual,
										ctx->agg_final_costs,
										dNumGroups);

		add_path(output_rel, path, root);
	}
	else
		return;
}

/*
 * Create Paths for Multiple DISTINCT-qualified aggregates.
 *
 * The goal is that using a single execution path to handle all DQAs, so
 * before removing duplication a SplitTuple node is created. This node handles
 * each input tuple to n output tuples(n is DQA expr number). Each output tuple
 * only contains an AggExprId, one DQA expr and all GROUP by expr. For example,
 * SELECT DQA(a), DQA(b) FROM foo GROUP BY c;
 * After the tuple split, two tuples are generated:
 * -------------------
 * | 1 | a | n/a | c |
 * -------------------
 * -------------------
 * | 2 | n/a | b | c |
 * -------------------
 *
 * In an aggregate executor, if the input tuple contains AggExprId, that means
 * the tuple is split. Each value of AggExprId points to a bitmap set to
 * represent args AttrNumber. In the Agg executor, each transfunc also keeps
 * its own args bitmap set. The transfunc is invoked only if bitmapset matches
 * with each other.
 */
static void
add_multi_dqas_hash_agg_path(PlannerInfo *root,
							 Path *path,
							 cdb_agg_planning_context *ctx,
							 RelOptInfo *output_rel,
							 cdb_multi_dqas_info *info)
{
	List	   *dqa_group_tles;
	CdbPathLocus distinct_locus;
	bool		distinct_need_redistribute;

	/*
	 * If subpath is projection capable, we do not want to generate a
	 * projection plan. The reason is that the projection plan does not
	 * constrain a child tlist when it creates subplan. Thus, GROUP BY expr
	 * may not be found in the scan targetlist.
	 */
	path = apply_projection_to_path(root, path->parent, path, info->input_proj_target);

	/*
	 * Finalize Aggregate
	 *   -> Gather Motion
	 *        -> Partial Aggregate
	 *             -> HashAggregate, to remote duplicates
	 *                  -> Redistribute Motion
	 *                       -> TupleSplit (according to DISTINCT expr)
	 *                            -> input
	 */
	path = (Path *) create_tup_split_path(root,
										  output_rel,
										  path,
										  info->tup_split_target,
										  ctx->groupClause,
										  info->dqa_expr_lst);

	AggClauseCosts DedupCost = {};
	get_agg_clause_costs(root,
						 AGGSPLIT_SIMPLE,
						 &DedupCost);

	if (gp_enable_dqa_pruning)
	{
		/*
		 * If we are grouping, we charge an additional cpu_operator_cost per
		 * **grouping column** per input tuple for grouping comparisons.
		 *
		 * But in the tuple split case, other columns not for this DQA are
		 * NULLs, the actual cost is way less than the number calculating based
		 * on the length of grouping clause.
		 *
		 * So here we create a dummy grouping clause whose length is 1 (the
		 * most common case of DQA), use it to calculate the cost, then set the
		 * actual one back into the path.
		 */
		List *dummy_group_clause = list_make1(list_head(info->dqa_group_clause));

		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										info->tup_split_target,
										AGG_HASHED,
										AGGSPLIT_SIMPLE,
										true, /* streaming */
										dummy_group_clause, /* only its length 1 is being used here */
										NIL,
										&DedupCost,
										estimate_num_groups_on_segment(info->dNumDistinctGroups,
																	   path->rows, path->locus));

		/* set the actual group clause back */
		((AggPath *)path)->groupClause = info->dqa_group_clause;
	}

	dqa_group_tles = get_common_group_tles(info->tup_split_target,
										   info->dqa_group_clause, NIL);
	distinct_locus = choose_grouping_locus(root, path, dqa_group_tles,
										   &distinct_need_redistribute);

	if (distinct_need_redistribute)
		path = cdbpath_create_motion_path(root, path, NIL, false,
										  distinct_locus);

	AggStrategy split = AGG_PLAIN;
	unsigned long DEDUPLICATED_FLAG = 0;
	PathTarget *partial_target = info->partial_target;
	double		input_rows = path->rows;

	if (ctx->groupClause)
	{
		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										info->tup_split_target,
										AGG_HASHED,
										AGGSPLIT_SIMPLE,
										false, /* streaming */
										info->dqa_group_clause,
										NIL,
										&DedupCost,
										clamp_row_est(info->dNumDistinctGroups / CdbPathLocus_NumSegments(distinct_locus)));

		split = AGG_HASHED;
		DEDUPLICATED_FLAG = AGGSPLITOP_DEDUPLICATED;
		partial_target = strip_aggdistinct(info->partial_target);
	}

	path = (Path *) create_agg_path(root,
									output_rel,
									path,
									partial_target,
									split,
									AGGSPLIT_INITIAL_SERIAL | DEDUPLICATED_FLAG,
									false, /* streaming */
									ctx->groupClause,
									NIL,
									ctx->agg_partial_costs,
									estimate_num_groups_on_segment(ctx->dNumGroupsTotal,
																   input_rows, path->locus));

	CdbPathLocus singleQE_locus;
	CdbPathLocus_MakeSingleQE(&singleQE_locus, getgpsegmentCount());
	path = cdbpath_create_motion_path(root,
									  path,
									  NIL,
									  false,
									  singleQE_locus);

	path = (Path *) create_agg_path(root,
									output_rel,
									path,
									info->final_target,
									split,
									AGGSPLIT_FINAL_DESERIAL | DEDUPLICATED_FLAG,
									false, /* streaming */
									ctx->groupClause,
									ctx->havingQual,
									ctx->agg_final_costs,
									ctx->dNumGroupsTotal);

	add_path(output_rel, path, root);
}

/*
 * Get the common expressions in all grouping sets as a target list.
 *
 * In case of a simple GROUP BY, it's just all the grouping column. With
 * multiple grouping sets, identify the set of common entries, and return
 * a list of those. For example, if you do:
 *
 *   GROUP BY GROUPING SETS ((a, b, c), (b, c))
 *
 * the common cols are b and c.
 */
List *
get_common_group_tles(PathTarget *target,
					  List *groupClause,
					  List *rollups)
{
	List	   *tlist = make_tlist_from_pathtarget(target);
	List	   *group_tles;
	ListCell   *lc;
	Bitmapset  *common_groupcols = NULL;
	int			x;

	if (rollups)
	{
		ListCell   *lc;
		bool		first = true;

		foreach(lc, rollups)
		{
			RollupData *rollup = lfirst_node(RollupData, lc);
			ListCell   *lc2;

			foreach(lc2, rollup->gsets)
			{
				List	   *colidx_lists = (List *) lfirst(lc2);
				ListCell   *lc3;
				Bitmapset  *this_groupcols = NULL;

				foreach(lc3, colidx_lists)
				{
					int			colidx = lfirst_int(lc3);
					SortGroupClause *sc = list_nth(rollup->groupClause, colidx);

					this_groupcols = bms_add_member(this_groupcols, sc->tleSortGroupRef);
				}

				if (first)
					common_groupcols = this_groupcols;
				else
				{
					common_groupcols = bms_int_members(common_groupcols, this_groupcols);
					bms_free(this_groupcols);
				}
				first = false;
			}
		}
	}
	else
	{
		foreach(lc, groupClause)
		{
			SortGroupClause *sc = lfirst(lc);

			common_groupcols = bms_add_member(common_groupcols, sc->tleSortGroupRef);
		}
	}

	x = -1;
	group_tles = NIL;
	while ((x = bms_next_member(common_groupcols, x)) >= 0)
	{
		TargetEntry *tle = get_sortgroupref_tle(x, tlist);

		group_tles = lappend(group_tles, tle);
	}

	return group_tles;
}

static List *
get_all_rollup_groupclauses(List *rollups)
{
	List	   *sortcls = NIL;
	ListCell   *lc;
	Bitmapset  *all_sortrefs = NULL;

	foreach(lc, rollups)
	{
		RollupData *rollup = lfirst_node(RollupData, lc);
		ListCell   *lc2;

		foreach(lc2, rollup->gsets)
		{
			List	   *colidx_lists = (List *) lfirst(lc2);
			ListCell   *lc3;

			foreach(lc3, colidx_lists)
			{
				int			colidx = lfirst_int(lc3);
				SortGroupClause *sc = list_nth(rollup->groupClause, colidx);

				if (!bms_is_member(sc->tleSortGroupRef, all_sortrefs))
				{
					sortcls = lappend(sortcls, sc);
					all_sortrefs = bms_add_member(all_sortrefs, sc->tleSortGroupRef);
				}
			}
		}
	}
	return sortcls;
}

/*
 * Choose a data distribution to perform the grouping.
 *
 * 'group_tles' is a target list that represents the grouping columns,
 * or all the common columns in all the grouping sets if there are
 * multple grouping sets. Use get_common_group_tles() to build that
 * list.
 */
CdbPathLocus
choose_grouping_locus(PlannerInfo *root, Path *path,
					  List *group_tles,
					  bool *need_redistribute_p)
{
	CdbPathLocus locus;
	bool		need_redistribute;

	/*
	 * If the input is already collected to a single segment, just perform the
	 * aggregation there. We could redistribute it, so that we could perform
	 * the aggregation in parallel, but Motions are pretty expensive so it's
	 * probably not worthwhile.
	 */
	if (CdbPathLocus_IsBottleneck(path->locus))
	{
		need_redistribute = false;
		CdbPathLocus_MakeNull(&locus);
	}
	/* If there are no GROUP BY columns, we have no choice but gather everything to a single node */
	else if (!group_tles)
	{
		need_redistribute = true;
		CdbPathLocus_MakeSingleQE(&locus, getgpsegmentCount());
	}
	/* If the input is already suitably distributed, no need to redistribute */
	else if (!CdbPathLocus_IsHashedOJ(path->locus) &&
			 cdbpathlocus_is_hashed_on_tlist(path->locus, group_tles, true))
	{
		need_redistribute = false;
		CdbPathLocus_MakeNull(&locus);
	}
	/*
	 * If the query's final result locus collocates with the GROUP BY, then
	 * redistribute directly to that locus and avoid a possible redistribute
	 * step later. (We might still need to redistribute the data for later
	 * windowing, LIMIT or similar, but this is a pretty good heuristic.)
	 */
	else if (CdbPathLocus_IsHashed(root->final_locus) &&
			 cdbpathlocus_is_hashed_on_tlist(root->final_locus, group_tles, true))
	{
		need_redistribute = true;
		locus = root->final_locus;
	}
	/*
	 * Construct a new locus from the GROUP BY columns. We greedily use as
	 * many columns as possible, to maximimize distribution. (It might be
	 * cheaper to pick only one or two columns, as long as they distribute
	 * the data evenly enough, but we're not that smart.)
	 */
	else
	{
		List	   *hash_exprs;
		List	   *hash_opfamilies;
		List	   *hash_sortrefs;
		ListCell   *lc;

		hash_exprs = NIL;
		hash_opfamilies = NIL;
		hash_sortrefs = NIL;
		foreach(lc, group_tles)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);
			Oid			typeoid = exprType((Node *) tle->expr);
			Oid			opfamily;
			Oid			eqopoid;

			opfamily = cdb_default_distribution_opfamily_for_type(typeoid);
			if (!OidIsValid(opfamily))
				continue;

			/*
			 * If the datatype isn't mergejoinable, then we cannot represent
			 * the grouping in the locus. Skip such expressions.
			 */
			eqopoid = cdb_eqop_in_hash_opfamily(opfamily, typeoid);
			if (!op_mergejoinable(eqopoid, typeoid))
				continue;

			hash_exprs = lappend(hash_exprs, tle->expr);
			hash_opfamilies = lappend_oid(hash_opfamilies, opfamily);
			hash_sortrefs = lappend_int(hash_sortrefs, tle->ressortgroupref);
		}

		if (hash_exprs)
			locus = cdbpathlocus_from_exprs(root,
											path->parent,
											hash_exprs,
											hash_opfamilies,
											hash_sortrefs,
											getgpsegmentCount(),
											path->parallel_workers);
		else
			CdbPathLocus_MakeSingleQE(&locus, getgpsegmentCount());
		need_redistribute = true;
	}

	*need_redistribute_p = need_redistribute;
	return locus;
}

static DQAType
recognize_dqa_type(cdb_agg_planning_context *ctx)
{
	ListCell    *lc, *lcc;
	List        *dqaArgs = NIL;
	ctx->type = INVALID_DQA;

	foreach (lc, ctx->agg_costs->distinctAggrefs)
	{
		Aggref *aggref = (Aggref *) lfirst(lc);
		SortGroupClause *arg_sortcl;

		/* I can not give a case for a DQA have order by yet. */
		if (aggref->aggorder != NIL)
			return ctx->type;

		foreach (lcc, aggref->aggdistinct)
		{
			arg_sortcl = (SortGroupClause *) lfirst(lcc);
			if (!arg_sortcl->hashable)
			{
				/*
				 * XXX: I'm not sure if the hashable flag is always set correctly
				 * for DISTINCT args. DISTINCT aggs are never implemented with hashing
				 * in PostgreSQL.
				 */
				return ctx->type;
			}
		}

		/* get the first dqa arguments */
		if (dqaArgs == NIL)
		{
			dqaArgs = aggref->args;
			ctx->type = SINGLE_DQA;
		}
		/* if there is another dqa with different args, it's MULTI_DQAS */
		else if (!equal(dqaArgs, aggref->args))
		{
			ctx->type = MULTI_DQAS;
			break;
		}
	}

	if (ctx->type != INVALID_DQA)
	{
		/* Check that there are no non-DISTINCT aggregates mixed in. */
		List *varnos = pull_var_clause((Node *) ctx->target->exprs,
									   PVC_INCLUDE_AGGREGATES |
									   PVC_INCLUDE_WINDOWFUNCS |
									   PVC_INCLUDE_PLACEHOLDERS);
		foreach (lc, varnos)
		{
			Node	   *node = lfirst(lc);

			if (IsA(node, Aggref))
			{
				Aggref	   *aggref = (Aggref *) node;

				if (!aggref->aggdistinct)
				{
					/* mixing DISTINCT and non-DISTINCT aggs */
					if (ctx->type == SINGLE_DQA)
						ctx->type = SINGLE_DQA_WITHAGG;
					else
						ctx->type = MULTI_DQAS_WITHAGG;

					return ctx->type;
				}
			}
		}
	}

	return ctx->type;
}

/*
 * fetch_multi_dqas_info
 *
 * 1. fetch all dqas path required information as single dqa's function.
 *
 * 2. append an AggExprId into Pathtarget to indicate which DQA expr is
 * in the output tuple after TupleSplit.
 */
static void
fetch_multi_dqas_info(PlannerInfo *root,
					  Path *path,
					  cdb_agg_planning_context *ctx,
					  cdb_multi_dqas_info *info)
{
	ListCell    *lc;
	ListCell    *lcc;
	Index		maxRef = 0;
	PathTarget *proj_target;
	int			num_input_segments;
	double		num_total_input_rows;
	List	   *group_exprs;
	double		dNumDistinctGroups;

	if (CdbPathLocus_IsPartitioned(path->locus))
		num_input_segments = CdbPathLocus_NumSegments(path->locus);
	else
		num_input_segments = 1;
	num_total_input_rows = path->rows * num_input_segments;

	group_exprs = get_sortgrouplist_exprs(ctx->groupClause,
										  make_tlist_from_pathtarget(path->pathtarget));

	proj_target = copy_pathtarget(path->pathtarget);
	if (proj_target->sortgrouprefs)
	{
		for (int idx = 0; idx < list_length(proj_target->exprs); idx++)
		{
			if (proj_target->sortgrouprefs[idx] > maxRef)
				maxRef = proj_target->sortgrouprefs[idx];
		}
	}
	else
		proj_target->sortgrouprefs = (Index *) palloc0(list_length(proj_target->exprs) * sizeof(Index));

	info->dqa_expr_lst = NIL;

	/*
	 * assign numDisDQAs and agg_args_id_bms
	 *
	 * find all DQAs with different args, count the number, store their args bitmapsets
	 */
	dNumDistinctGroups = 0;
	forboth(lc, ctx->agg_partial_costs->distinctAggrefs,
	        lcc, ctx->agg_final_costs->distinctAggrefs)
	{
		Aggref	        *aggref = (Aggref *) lfirst(lc);
		Aggref	        *aggref_final = (Aggref *) lfirst(lcc);
		SortGroupClause *arg_sortcl;
		TargetEntry     *arg_tle;
		ListCell        *lc2;
		Bitmapset       *bms = NULL;
		List		   *this_dqa_group_exprs;

		this_dqa_group_exprs = list_copy(group_exprs);

		foreach (lc2, aggref->aggdistinct)
		{
			arg_sortcl = (SortGroupClause *) lfirst(lc2);
			arg_tle = get_sortgroupclause_tle(arg_sortcl, aggref->args);
			ListCell    *lc3;
			int         dqa_idx = 0;
			Expr		*naked_tle_expr = arg_tle->expr;

			/*
			 * When conversions between two binary-compatible types happen in
			 * DQA expressions, the expr(s) in arg_tle and proj_target->exprs
			 * may be wrapped with a RelabelType node. The RelabelType node doesn't
			 * affect the semantics, so we ignore it here.
			 * For conversions that are not binary-compatible, the exprs are wrapped
			 * with other types of node, e.g., CoerceViaIO.
			 * Relevent bug report: https://github.com/greenplum-db/gpdb/issues/14096
			 */
			while (naked_tle_expr && IsA(naked_tle_expr, RelabelType))
				naked_tle_expr = ((RelabelType *) naked_tle_expr)->arg;

			foreach (lc3, proj_target->exprs)
			{
				Expr    *expr = lfirst(lc3);
				Expr	*naked_expr = expr;
				/* Ignore the RelabelType node. */
				while (naked_expr && IsA(naked_expr, RelabelType))
					naked_expr = ((RelabelType *) naked_expr)->arg;

				if (equal(naked_tle_expr, naked_expr))
					break;

				dqa_idx++;
			}

			/*
			 * DQA expr is not in PathTarget
			 *
			 * SELECT DQA(a + b) from foo;
			 */
			if (dqa_idx == list_length(proj_target->exprs))
			{
				add_column_to_pathtarget(proj_target, arg_tle->expr, ++maxRef);

				SortGroupClause *sortcl;

				sortcl = copyObject(arg_sortcl);
				sortcl->tleSortGroupRef = maxRef;
				sortcl->hashable = true;	/* we verified earlier that it's hashable */

				info->dqa_group_clause = lappend(info->dqa_group_clause, sortcl);
				this_dqa_group_exprs = lappend(this_dqa_group_exprs, arg_tle->expr);

				bms = bms_add_member(bms, maxRef);
			}
			else if (proj_target->sortgrouprefs[dqa_idx] == 0)
			{
				/*
				 * DQA expr in PathTarget but no reference
				 *
				 * SELECT DQA(a) FROM foo ;
				 */
				proj_target->sortgrouprefs[dqa_idx] = ++maxRef;

				SortGroupClause *sortcl;

				sortcl = copyObject(arg_sortcl);
				sortcl->tleSortGroupRef = maxRef;
				sortcl->hashable = true;	/* we verified earlier that it's hashable */

				info->dqa_group_clause = lappend(info->dqa_group_clause, sortcl);
				this_dqa_group_exprs = lappend(this_dqa_group_exprs, arg_tle->expr);

				bms = bms_add_member(bms, maxRef);
			}
			else
			{
				/*
				 * DQA expr in PathTarget and referenced by GROUP BY clause
				 *
				 * SELECT DQA(a) FROM foo GROUP BY a;
				 */
				Index exprRef = proj_target->sortgrouprefs[dqa_idx];
				bms = bms_add_member(bms, exprRef);
			}
		}

		/*
		 * DQA(a, b) and DQA(b, a) and their filter is same, as well as, they
		 * do not contain volatile expression, then they can share one split
		 * tuple.
		 */
		Index agg_expr_id ;
		if (!contain_volatile_functions((Node *)aggref->aggfilter))
		{
			ListCell *lc_dqa;
			agg_expr_id = 1;
			foreach (lc_dqa, info->dqa_expr_lst)
			{
				DQAExpr *dqaExpr = (DQAExpr *)lfirst(lc_dqa);

				if (bms_equal(bms, dqaExpr->agg_args_id_bms)
					&& equal(aggref->aggfilter, dqaExpr->agg_filter))
					break;

				agg_expr_id++;
			}
		}
		else
		{
			agg_expr_id = list_length(info->dqa_expr_lst) + 1;
		}

		/* If DQA(expr1) FILTER (WHERE expr2) is different with previous, create new one */
		if ((agg_expr_id - 1) == list_length(info->dqa_expr_lst))
		{
			DQAExpr *dqaExpr= makeNode(DQAExpr);

			dqaExpr->agg_expr_id = agg_expr_id;
			dqaExpr->agg_args_id_bms = bms;
			dqaExpr->agg_filter = (Expr *)copyObject(aggref->aggfilter);
			info->dqa_expr_lst = lappend(info->dqa_expr_lst, dqaExpr);

			/*
			 * How many distinct combinations of GROUP BY columns and the
			 * DISTINCT arguments of this aggregate are there? Add it to the
			 * total.
			 */
			dNumDistinctGroups += estimate_num_groups(root,
			                                          this_dqa_group_exprs,
			                                          num_total_input_rows,
													  NULL, NULL);
		}

		/* assign an agg_expr_id value to aggref*/
		aggref->agg_expr_id = agg_expr_id;

		/* rid of filter in aggref */
		aggref->aggfilter = NULL;
		aggref_final->aggfilter = NULL;
	}
	info->dNumDistinctGroups = dNumDistinctGroups;

	info->input_proj_target = proj_target;
	info->tup_split_target = copy_pathtarget(proj_target);
	{
		AggExprId *a_expr_id = makeNode(AggExprId);
		add_column_to_pathtarget(info->tup_split_target, (Expr *)a_expr_id, ++maxRef);

		Oid eqop;
		bool hashable;
		SortGroupClause *sortcl;
		get_sort_group_operators(INT4OID, false, true, false, NULL, &eqop, NULL, &hashable);

		sortcl = makeNode(SortGroupClause);
		sortcl->tleSortGroupRef = maxRef;
		sortcl->hashable = hashable;
		sortcl->eqop = eqop;
		info->dqa_group_clause = lcons(sortcl, info->dqa_group_clause);
	}

	info->dqa_group_clause = list_concat(info->dqa_group_clause,
										 list_copy(ctx->groupClause));

	info->partial_target= ctx->partial_grouping_target;
	info->final_target = ctx->target;
}

/*
 * fetch_single_dqa_info
 *
 * fetch single dqa path required information and store in cdb_multi_dqas_info
 *
 * info->input_target contains subpath target expr + all DISTINCT expr
 *
 * info->dqa_group_clause contains DISTINCT expr + GROUP BY expr
 */
static void
fetch_single_dqa_info(PlannerInfo *root,
					  Path *path,
					  cdb_agg_planning_context *ctx,
					  cdb_multi_dqas_info *info)
{
	Index		maxRef;
	List	   *dqa_group_exprs;

	/* Prepare a modifiable copy of the input path target */
	info->input_proj_target = copy_pathtarget(path->pathtarget);
	maxRef = 0;
	List *exprLst = info->input_proj_target->exprs;
	if (info->input_proj_target->sortgrouprefs)
	{
		for (int idx = 0; idx < list_length(exprLst); idx++)
		{
			if (info->input_proj_target->sortgrouprefs[idx] > maxRef)
				maxRef = info->input_proj_target->sortgrouprefs[idx];
		}
	}
	else
		info->input_proj_target->sortgrouprefs = (Index *) palloc0(list_length(exprLst) * sizeof(Index));

	dqa_group_exprs = get_sortgrouplist_exprs(ctx->groupClause,
											  make_tlist_from_pathtarget(path->pathtarget));

	Aggref	   *aggref = list_nth(ctx->agg_costs->distinctAggrefs, 0);
	SortGroupClause *arg_sortcl;
	SortGroupClause *sortcl = NULL;
	TargetEntry *arg_tle;
	int			idx = 0;
	ListCell   *lc;
	ListCell   *lcc;

	foreach (lc, aggref->aggdistinct)
	{
		arg_sortcl = (SortGroupClause *) lfirst(lc);
		arg_tle = get_sortgroupref_tle(arg_sortcl->tleSortGroupRef, aggref->args);

		/* Now find this expression in the sub-path's target list */
		idx = 0;
		foreach(lcc, info->input_proj_target->exprs)
		{
			Expr		*expr = lfirst(lcc);

			if (equal(expr, arg_tle->expr))
				break;
			idx++;
		}

		if (idx == list_length(info->input_proj_target->exprs))
			add_column_to_pathtarget(info->input_proj_target, arg_tle->expr, ++maxRef);
		else if (info->input_proj_target->sortgrouprefs[idx] == 0)
			info->input_proj_target->sortgrouprefs[idx] = ++maxRef;

		sortcl = copyObject(arg_sortcl);
		sortcl->tleSortGroupRef = info->input_proj_target->sortgrouprefs[idx];
		sortcl->hashable = true;	/* we verified earlier that it's hashable */

		info->dqa_group_clause = lappend(info->dqa_group_clause, sortcl);
		dqa_group_exprs = lappend(dqa_group_exprs, arg_tle->expr);
	}

	info->dqa_group_clause = list_concat(list_copy(ctx->groupClause),
										 info->dqa_group_clause);

	/*
	 * Estimate how many groups there are in DISTINCT + GROUP BY, per segment.
	 * For example in query:
	 *
	 * select count(distinct c) from t group by b;
	 *
	 * dNumDistinctGroups is the estimate of distinct combinations of b and c.
	 */
	double		num_total_input_rows;
	if (CdbPathLocus_IsPartitioned(path->locus))
		num_total_input_rows = path->rows * CdbPathLocus_NumSegments(path->locus);
	else
		num_total_input_rows = path->rows;
	info->dNumDistinctGroups = estimate_num_groups(root,
												   dqa_group_exprs,
												   num_total_input_rows,
												   NULL, NULL);
}

/*
 * Prepare the input path for sorted Agg node.
 *
 * The input to a (sorted) Agg node must be:
 *
 * 1. distributed so that rows belonging to the same group reside on the
 *    same segment, and
 *
 * 2. sorted according to the pathkeys.
 *
 * If the input is already suitably distributed, this is no different from
 * upstream, and we just add a Sort node if the input isn't already sorted.
 *
 * This also works for the degenerate case with no pathkeys, which means
 * simple aggregation without grouping. For that, all the rows must be
 * brought to a single node, but no sorting is needed.
 *
 * For non-sorted input, the logic is the same as in choose_grouping_locus()
 * (in fact this uses choose_grouping_locus()), except that if the input
 * is already sorted, we prefer to gather it to a single node to make
 * use of the pre-existing order, instead of redistributing and resorting
 * it.
 */
Path *
cdb_prepare_path_for_sorted_agg(PlannerInfo *root,
								bool is_sorted,
								int presorted_keys,
								/* args corresponding to create_sort_path */
								RelOptInfo *rel,
								Path *subpath,
								PathTarget *target,
								List *group_pathkeys,
								double limit_tuples,
								/* extra arguments */
								List *groupClause,
								List *rollups)
{
	CdbPathLocus locus;
	bool		need_redistribute;
	bool 		use_incremental_sort =  (presorted_keys != 0 && enable_incremental_sort);

	/*
	 * If the input is already collected to a single segment, just add a Sort
	 * node (if needed). We could redistribute it, so that we could perform the
	 * aggregation in parallel, but Motions are pretty expensive so it's
	 * probably not worthwhile.
	 */
	if (CdbPathLocus_IsBottleneck(subpath->locus))
	{
		need_redistribute = false;
	}
	else
	{
		List	   *group_tles;

		group_tles = get_common_group_tles(target,
										   groupClause,
										   rollups);

		locus = choose_grouping_locus(root, subpath, group_tles,
									  &need_redistribute);
	}
	if (!need_redistribute)
	{
		if (!is_sorted)
		{
			if (!use_incremental_sort)
				subpath = (Path *) create_sort_path(root,
													rel,
													subpath,
													group_pathkeys,
													-1.0);
			else
			{
				subpath = (Path *) create_incremental_sort_path(root,
																rel,
																subpath,
																group_pathkeys,
																presorted_keys,
																-1.0);
			}
		}

		
		return subpath;
	}

	if (is_sorted && group_pathkeys)
	{
		/*
		 * The input is already conveniently sorted. We could redistribute
		 * it by the grouping keys, but then we'd need to re-sort it. That
		 * doesn't seem like a good idea, so we prefer to gather it all, and
		 * take advantage of the sort order.
		 */
		CdbPathLocus_MakeSingleQE(&locus, getgpsegmentCount());
		subpath = cdbpath_create_motion_path(root,
											 subpath,
											 group_pathkeys,
											 false, locus);
	}
	else if (!is_sorted && group_pathkeys)
	{
		/*
		 * If we need to redistribute, it's usually best to redistribute
		 * the data first, and then sort in parallel on each segment.
		 *
		 * But if we don't have any expressions to redistribute on, i.e.
		 * if we are gathering all data to a single node to perform the
		 * aggregation, then it's better to sort all the data on the
		 * segments first, in parallel, and do a order-preserving motion
		 * to merge the inputs.
		 */
		if (CdbPathLocus_IsPartitioned(locus))
			subpath = cdbpath_create_motion_path(root, subpath, NIL,
												 false, locus);

		if (!use_incremental_sort)
			subpath = (Path *) create_sort_path(root,
												rel,
												subpath,
												group_pathkeys,
												-1.0);
		else
		{
			subpath = (Path *) create_incremental_sort_path(root,
															rel,
															subpath,
															group_pathkeys,
															presorted_keys,
															-1.0);
		}

		if (!CdbPathLocus_IsPartitioned(locus))
			subpath = cdbpath_create_motion_path(root, subpath,
												 group_pathkeys,
												 false, locus);
	}
	else
	{
		/*
		 * The grouping doesn't require any sorting, i.e. the GROUP BY
		 * consists entirely of (pseudo-)constants.
		 *
		 * The locus could be Hashed, which is a bit silly because with
		 * all-constant grouping keys, all the rows will end up on a
		 * single QE anyway. We could mark the locus as SingleQE here, so
		 * that in simple cases where the result needs to end up in the QD,
		 * the planner could Gather the result there directly. However, in
		 * other cases hashing the result to one QE node is more helpful
		 * for the plan above this.
		 */
		Assert(!group_pathkeys);
		subpath = cdbpath_create_motion_path(root,
											 subpath,
											 subpath->pathkeys,
											 false, locus);
	}

	return subpath;
}

/*
 * Prepare the input path for hashed Agg node.
 *
 * This is much simpler than the sorted case. We only need to care about
 * distribution, not sorting.
 */
Path *
cdb_prepare_path_for_hashed_agg(PlannerInfo *root,
								Path *subpath,
								PathTarget *target,
								/* extra arguments */
								List *groupClause,
								List *rollups)
{
	List	   *group_tles;
	CdbPathLocus locus;
	bool		need_redistribute;

	if (CdbPathLocus_IsBottleneck(subpath->locus))
		return subpath;

	group_tles = get_common_group_tles(target,
									   groupClause,
									   rollups);
	locus = choose_grouping_locus(root,
								  subpath,
								  group_tles,
								  &need_redistribute);

	/*
	 * Redistribute if needed.
	 *
	 * The hash agg doesn't care about input order, and it destroys any
	 * order there was, so don't bother with a order-preserving Motion even
	 * if we could.
	 */
	if (need_redistribute)
		subpath = cdbpath_create_motion_path(root,
											 subpath,
											 NIL /* pathkeys */,
											 false,
											 locus);

	return subpath;
}

#ifdef SERVERLESS
/*
 * Precheck if we could try append agg of a Query.
 * We will compare parse with view query if there were
 * exactly matched.
 * But we clould stop early if we are sure there is no chance.
 * It doesn't matter if we miss something here.
 */
static bool
try_append_agg(Query *parse)
{
	ListCell *lc;

	/* FIXME: could we handle order by, limit? */
	if ((parse->commandType != CMD_SELECT) ||
		(parse->rowMarks != NIL) ||
		(parse->distinctClause != NIL) ||
		(parse->scatterClause != NIL) ||
		(parse->cteList != NIL) ||
		(parse->groupingSets != NIL) ||
		(parse->havingQual != NULL) ||
		(parse->setOperations != NULL) ||
		parse->hasWindowFuncs ||
		parse->hasDistinctOn ||
		parse->hasModifyingCTE ||
		parse->groupDistinct ||
		(parse->parentStmtType == PARENTSTMTTYPE_REFRESH_MATVIEW) ||
		(parse->parentStmtType == PARENTSTMTTYPE_CTAS) ||
		parse->hasSubLinks)
	{
		return false;
	}

	/* As we will use views, make it strict to unmutable. */
	if (contain_mutable_functions((Node*)parse))
		return false;

	/* We want Agg with Group By. */
	if (!parse->hasAggs || (parse->groupClause == NIL))
		return false;

	/*
	 * Only aggs: count, sum, avg of single column are supported now. 
	 */
	foreach(lc, parse->targetList)
	{
		TargetEntry* tle = lfirst_node(TargetEntry, lc);

		if (tle->resjunk)
			return false;

		if (IsA(tle->expr, Var))
		{
			if (tle->ressortgroupref == 0)
				return false;
		}
		else if (IsA(tle->expr, Aggref))
		{
			Aggref *aggref = (Aggref *) tle->expr;
			const char *aggname = get_func_name(aggref->aggfnoid);
			/*
			 * FIXME: use func name is necessary but not sufficient
			 * should use fnoid to restrict later.
			 */
			if ((strcmp(aggname, "count") == 0) &&
				(strcmp(aggname, "sum") == 0) &&
				(strcmp(aggname, "avg") == 0))
				return false;

			if (aggref->aggorder ||
				(aggref->aggdirectargs != NIL) ||
				(aggref->aggdistinct != NIL) ||
				(aggref->aggfilter != NULL))
				return false;

			/* Star is ok. */
			if (aggref->aggstar)
				continue;
			
			if ((aggref->args == NIL ||
				(list_length(aggref->args) != 1)))
				return false;
		
			TargetEntry *tle = (TargetEntry *) linitial(aggref->args);
			if (!IsA(tle->expr, Var))
				return false;
		}
		else
			return false;
	}
	return true;
}

/*
 * expand_append_agg
 * Expand two-stage agg plan to a append agg plan,
 * return true if we succeed.
 */
static bool
expand_append_agg(PlannerInfo *root, cdb_agg_planning_context *ctx)
{
	Path		*cheapest_first_stage_path = NULL;
	Path		*underlying_seqscanpath = NULL;
	Query		*parse = root->parse;
	Relation	matviewRel = NULL;

	/*
	 * First-stage agg with seqscan on a normal table and/or with sort.
	 * Plan A:
	 * Partial Agg
	 * 		SeqScan
	 * 
	 * Plan B:
	 * Partial Agg
	 * 		Sort
	 *	 		SeqScan
	 * Plan B requires order on segments, we have to do more such as add a additonal
	 * Sort node above Append.
	 */
	set_cheapest(ctx->partial_rel);
	cheapest_first_stage_path = ctx->partial_rel->cheapest_total_path;

	if (!IsA(cheapest_first_stage_path, AggPath))
		return false;

	if ((castNode(AggPath, cheapest_first_stage_path)->aggsplit != AGGSPLIT_INITIAL_SERIAL))
		return false;

	if (castNode(AggPath, cheapest_first_stage_path)->aggstrategy == AGG_HASHED)
	{
		Path	*subpath = ((AggPath*) cheapest_first_stage_path)->subpath;

		if (subpath->pathtype != T_SeqScan)
			return false;

		/* Seqscan on a normal table. */
		if (subpath->parent->reloptkind != RELOPT_BASEREL)
			return false;

		underlying_seqscanpath = subpath;
	}
	else if (castNode(AggPath, cheapest_first_stage_path)->aggstrategy == AGG_SORTED)
	{
		Path	*subpath = ((AggPath*) cheapest_first_stage_path)->subpath;

		if (subpath->pathtype != T_Sort)
			return false;
		
		subpath = castNode(SortPath, subpath)->subpath;

		if (subpath->parent->reloptkind != RELOPT_BASEREL)
			return false;

		underlying_seqscanpath = subpath;
	}
	else
		return false;

	/* Find a matched view for input query. */
	matviewRel = simple_view_matching(parse);
	if (matviewRel == NULL)
		return false;

	expand_append_agg_guts(root, ctx, matviewRel);

	/* Now do not forget to identify delta seqscan.*/
	underlying_seqscanpath->basemv = RelationGetRelid(matviewRel);

	/* Not use matviewRel anymore, close here. */
	table_close(matviewRel, NoLock);

	return true; /* Succeed to rewrite. */
}

/*
 * simple_view_matching
 *	Match a view with given Query, return the view relation itself if succeed.
 *	Only a SELECT from a single table is supported.
 *
 *  parse - the Query we want to match
 * 
 *  A lock will be held if we find a matched view, the caller should handle that.
 */ 
static Relation
simple_view_matching(Query *parse)
{
	Query			*viewQuery; /* Query of view. */
	Relation		matviewRel = NULL; /* Matched view relation. */
	Relation		ruleDesc;
	SysScanDesc		rcscan;
	RewriteRule		*rule;
	Form_pg_rewrite	rewrite_tup;
	List			*actions;
	HeapTuple		tup;
	Node			*mvjtnode;
	RangeTblEntry 	*mvrte;
	int				varno;
	PlannerInfo 	*subroot;
	bool			need_close = false;

	/*
	 * We know it's a single table.
	 */
	Oid				underlying_relid = (rt_fetch(1, parse->rtable))->relid;

	ruleDesc = table_open(RewriteRelationId, AccessShareLock);
	rcscan = systable_beginscan(ruleDesc, InvalidOid, false,
								NULL, 0, NULL);
	while (HeapTupleIsValid(tup = systable_getnext(rcscan)))
	{
		CHECK_FOR_INTERRUPTS();
		if (need_close)
			table_close(matviewRel, AccessShareLock);

		rewrite_tup = (Form_pg_rewrite) GETSTRUCT(tup);

		matviewRel = table_open(rewrite_tup->ev_class, AccessShareLock);
		need_close = true;

		/*
		 * Consider IVM only has insert operation
		 * since lastest REFRESH and with partial agg results.
		 */ 
		if (!RelationIsPopulated(matviewRel) || 
			/* FIXME: uncomment below when IVM is enabled in hashdata cloud. */
			#if 0
			(!RelationIsIVM(matviewRel)) ||
			#endif
			!matviewRel->rd_rel->relhaspartialagg ||
			!matviewRel->rd_rel->relinsertonly)
			continue;

		if (matviewRel->rd_rel->relhasrules == false ||
			matviewRel->rd_rules->numLocks != 1)
			continue;

		rule = matviewRel->rd_rules->rules[0];

		/* Filter a SELECT action, and not instead. */
		if ((rule->event != CMD_SELECT) || !(rule->isInstead))
			continue;

		actions = rule->actions;
		if (list_length(actions) != 1)
			continue;

		viewQuery = copyObject(linitial_node(Query, actions));

		if (list_length(viewQuery->jointree->fromlist) != 1)
			continue;

		mvjtnode = (Node *) linitial(viewQuery->jointree->fromlist);
		if (!IsA(mvjtnode, RangeTblRef))
			continue;

		varno = ((RangeTblRef*) mvjtnode)->rtindex;
		mvrte = rt_fetch(varno, viewQuery->rtable);
		Assert(mvrte != NULL);

		if (mvrte->rtekind != RTE_RELATION)
			continue;

		if (mvrte->relid != underlying_relid)
			continue;

		/* Transform actions to a normal parse tree. */
		aqumv_adjust_simple_parse(viewQuery);

		/*
		 * See AQUMV_FIXME_MVP in aqumv.c
		 */
		mvrte = rt_fetch(1, viewQuery->rtable);
		mvrte->inh = false;
		/*
		 * This is fool way to make comparison pass.
		 */
		mvrte->checkAsUser = InvalidOid;
		
		/* To make equal parse tree, need root to assign aggno in precess_aggrefs. */
		subroot = (PlannerInfo *) palloc0(sizeof(PlannerInfo));
		subroot->parse = viewQuery;
		subroot->processed_tlist = viewQuery->targetList;
		if (viewQuery->hasAggs)
		{
			preprocess_aggrefs(subroot, (Node *) subroot->processed_tlist);
		}

		/*
		 * This is fool way, but we don't want to compare them.
		 */
		viewQuery->stmt_location = parse->stmt_location;
		viewQuery->stmt_len = parse->stmt_len;

		/*
		 * Before we compare Query, quals need to be preprocessed becuase
		 * A signle qual may be a OpExpr or a list with one element.
		 * Both are legal but we can't use equal() with different node tag.
		 * Wrap to list if it was.
		 */
		if ((viewQuery->jointree->quals != NULL) && (!IsA(viewQuery->jointree->quals, List)))
			viewQuery->jointree->quals = (Node *)list_make1(viewQuery->jointree->quals);

		/* Query and viewQuery must be exatcly matched now. */
		if (equal(viewQuery, parse))
		{
			/*
			 * As we rewrite path directly without any cost,
			 * stop searching once a view is found.
			 */
			need_close = false;
			break;
		}
	}
	systable_endscan(rcscan);
	table_close(ruleDesc, AccessShareLock);

	if (need_close)
	{
		table_close(matviewRel, AccessShareLock);
		matviewRel = NULL;
	}
	return matviewRel;
}


/*
 * Make path targets from TupleDesc.
 * This is only used if we know it's an exactly matched
 * view with query we want.
 * So we could use TupleDesc with the same order and definations.
 * But IVM has extra invisible columns for maintenance besides
 * the columns in original query, result in different tuple desc
 * with the target list. Drop those for correct.
 */
static PathTarget*
make_pathtarget_from_tupledesc(TupleDesc tupdes)
{
	PathTarget *target = makeNode(PathTarget);
	int			i;

	/* We are a plain select, there should be no group cloumns. */
	target->sortgrouprefs = (Index *) palloc0((tupdes->natts) * sizeof(Index));

	for (i = 0; i < tupdes->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdes, i);

		/*
		 * IVM has invisible columns, drop that.
		 */
		if (isIvmName(NameStr(attr->attname)))
			continue;

		Var *newVar =  makeVar(2, /* See comments assign relid to 2. */
							attr->attnum,
							attr->atttypid,
							attr->atttypmod,
							attr->attcollation,
							0);

		target->exprs = lappend(target->exprs, (Expr*) newVar);
	}

	/*
	 * Unknown, but we have called contain_mutable_functions check.
	 * And that's more restrict.
	 */
	target->has_volatile_expr = VOLATILITY_UNKNOWN;

	return target;
}

/*
 * expand_append_agg_guts
 *
 * Do the real rewrite.
 * 
 * 1.HashAgg:
 * Partial Agg
 *		SeqScan on t
 * to:
 * Append
 *		Partial Agg
 *			Delta SeqScan on t
 *		SeqScan on mv
 *
 * 2.GroupAgg:
 * Partial Agg
 * 	Sort
 *		SeqScan on t
 * to:
 * Sort 
 * 	Append
 *		Partial Agg
 *			Sort
 *				Delta SeqScan on t
 *		SeqScan on mv
 */
static void
expand_append_agg_guts(PlannerInfo *root, cdb_agg_planning_context *ctx, Relation matviewRel)
{
	Path	*mv_seqscan_path;

	/* Build Seq Scan path from view. */
	mv_seqscan_path = build_view_seqscan_path(root, ctx, matviewRel);

	/* Append partial agg with view seqscan.*/
	rewrite_to_append_agg_path(root, ctx, matviewRel, mv_seqscan_path);
}

static Path*
build_view_seqscan_path(PlannerInfo *root, cdb_agg_planning_context *ctx, Relation matviewRel)
{
	Path			*mv_seqscan_path;
	PlannerInfo 	*dummy_root;
	Query			*dummy_query;
	RangeTblEntry	*dummy_rte;
	Path			*cheapest_first_stage_path = ctx->partial_rel->cheapest_total_path;

	mv_seqscan_path = makeNode(Path);
	mv_seqscan_path->pathtype = T_SeqScan;
	mv_seqscan_path->basemv = 0;
	mv_seqscan_path->param_info = NULL;
	mv_seqscan_path->parallel_aware = false;
	mv_seqscan_path->parallel_safe = false;
	mv_seqscan_path->parallel_workers = 0;
	mv_seqscan_path->pathkeys = NIL;	/* seqscan has unordered result.*/
	mv_seqscan_path->locus = cheapest_first_stage_path->locus; /* Keep same with agg path for now.*/
	mv_seqscan_path->motionHazard = false;
	mv_seqscan_path->barrierHazard = false;
	mv_seqscan_path->rescannable = true;
	mv_seqscan_path->sameslice_relids = cheapest_first_stage_path->parent->relids;

	/* Build dmmmy planner info for reloptions.*/
	dummy_query = makeNode(Query);
	dummy_query->commandType = CMD_SELECT;
	dummy_root = makeNode(PlannerInfo);
	dummy_root->parse = dummy_query;
	dummy_root->glob = makeNode(PlannerGlobal); /* Avoid crash during planner.*/
	dummy_root->query_level = 1;
	dummy_root->planner_cxt = CurrentMemoryContext;
	dummy_root->wt_param_id = -1;

	/* Build dmmmy rte for reloptions.*/
	dummy_rte = makeNode(RangeTblEntry);
	dummy_rte->rtekind = RTE_RELATION;
	dummy_rte->relid = RelationGetRelid(matviewRel);
	dummy_rte->relkind = RELKIND_MATVIEW;
	dummy_rte->rellockmode = AccessShareLock;
	dummy_rte->lateral = false;
	dummy_rte->inh = false;
	dummy_rte->inFromCl = true;
	/*
	 * Build eref for explain purpose.
	 */
	dummy_rte->eref = makeAlias(RelationGetRelationName(matviewRel), NIL);

	/*
	 * Build RelOptInfo 
	 * Hack here:
	 * we know that parse only has one table, so just make a NULL rte here
	 * and assign dummy_rte relid to 2 later.
	 */
	dummy_query->rtable = list_make2(NULL, dummy_rte);

	/* Set up RTE/RelOptInfo arrays and assign parent.*/
	setup_simple_rel_arrays(dummy_root);
	mv_seqscan_path->parent = build_simple_rel(dummy_root, 2, NULL);

	/*
	 * Now build pathtarget from mv.
	 * But we don't have targetList from dummy_parse here,
	 * use mv's TupleDesc to get a plain select * from mv.
	 * As we only have a exatly matched SQL now,
	 * a star * means only user defined columns are included,
	 * extra columns of IVM should not be inside.
	 */
	mv_seqscan_path->pathtarget = make_pathtarget_from_tupledesc(matviewRel->rd_att);

	/* Adjust planner info for view scan. */
	root->parse->rtable = lappend(root->parse->rtable, dummy_rte);
	root->simple_rel_array_size++;
	root->simple_rte_array = repalloc(root->simple_rte_array, (root->simple_rel_array_size)* sizeof(RangeTblEntry *));
	root->simple_rte_array[root->simple_rel_array_size - 1] = dummy_rte;
	root->simple_rel_array = repalloc(root->simple_rel_array, (root->simple_rel_array_size)* sizeof(RelOptInfo *));
	root->simple_rel_array[root->simple_rel_array_size - 1] = mv_seqscan_path->parent;

	return mv_seqscan_path;
}

/*
 * This is a hack way to build special append path,
 * only for expanding append agg plan.
 */
static void
rewrite_to_append_agg_path(PlannerInfo *root, cdb_agg_planning_context *ctx, Relation matviewRel, Path *mv_seqscan_path)
{
	AppendPath	*pathnode;
	Path		*cheapest_first_stage_path = ctx->partial_rel->cheapest_total_path;

	/* Build append path */
	pathnode = makeNode(AppendPath);
	pathnode->append_agg = true;
	pathnode->path.pathtype = T_Append;
	pathnode->path.parent = cheapest_first_stage_path->parent;
	pathnode->path.pathtarget = cheapest_first_stage_path->parent->reltarget;
	pathnode->path.param_info = NULL;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = false;
	pathnode->path.parallel_workers = 0;
	pathnode->path.pathkeys = NIL;
	pathnode->path.motionHazard = false;
	pathnode->path.barrierHazard = false;
	pathnode->path.rescannable = true;
	pathnode->path.locus = cheapest_first_stage_path->locus;
	pathnode->subpaths = list_make2(cheapest_first_stage_path, mv_seqscan_path);

	/* Set best path for partial_rel with ours. */
	ctx->partial_rel->cheapest_total_path = (Path *)pathnode;

	/*
	 * If it's a Group Agg, the order is required for final agg.
	 * The subpaths of Append node is unordered together, we have
	 * to add a Sort above it.
	 */ 
	if (castNode(AggPath, cheapest_first_stage_path)->aggstrategy == AGG_SORTED)
	{
		Path *path;
		path = (Path *) create_sort_path(root,
										 ctx->partial_rel,
										 (Path *)pathnode,
										 ctx->partial_sort_pathkeys,
										 -1.0);

		ctx->partial_rel->cheapest_total_path = path;
		/* For Group Agg, second stage is based on pathlist. */
		add_path(ctx->partial_rel, path ,root);
	}
}

/*
 * This should be refactor after CBDB github expose these functions.
 * Keep for now.
 * Wrap of aqumv_adjust_varno, expose for other places.
 * Adjust view's actions to a parse tree that can be processed as normal.
 * This in-place update the parse param.
 */
void aqumv_adjust_simple_parse(Query *parse)
{
	ListCell	*lc;
	/*
	 * AQUMV
	 * We have to rewrite now before we do the real Equivalent
	 * Transformation 'rewrite'.  
	 * Because actions sotored in rule is not a normal query tree,
	 * it can't be used directly, ex: new/old realtions used to
	 * refresh mv.
	 * Earse unused relatoins, keep the right one.
	 */
	foreach(lc, parse->rtable)
	{
		RangeTblEntry* rtetmp = lfirst(lc);
		if ((rtetmp->relkind == RELKIND_MATVIEW) &&
			(rtetmp->alias != NULL) &&
			(strcmp(rtetmp->alias->aliasname, "new") == 0 ||
			strcmp(rtetmp->alias->aliasname,"old") == 0))
		{
			foreach_delete_current(parse->rtable, lc);
		}
	}

	/*
	 * Now we have the right relation, adjust
	 * varnos in its query tree.
	 * AQUMV_FIXME_MVP: Only one single relation
	 * is supported now, we could assign varno
	 * to 1 opportunistically.
	 */
	aqumv_adjust_varno(parse, 1);

}
static void
aqumv_adjust_varno(Query* parse, int varno)
{
	aqumv_adjust_varno_context context;
	context.varno = varno;
	parse = query_tree_mutator(parse, aqumv_adjust_varno_mutator, &context, QTW_DONT_COPY_QUERY);
}

/* 
 * Adjust varno and rindex with delta. 
 */ 
static Node *aqumv_adjust_varno_mutator(Node *node, aqumv_adjust_varno_context *context)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, Var))
	{
		((Var *)node)->varno = context->varno;
		((Var *)node)->varnosyn = context->varno; /* NB: This should be backported to CBDB github! */
	}
	else if (IsA(node, RangeTblRef))
 		/* AQUMV_FIXME_MVP: currently we have only one relation */
		((RangeTblRef*) node)->rtindex = context->varno;
	return expression_tree_mutator(node, aqumv_adjust_varno_mutator, context);
}

#endif