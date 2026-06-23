/*-------------------------------------------------------------------------
 *
 * vacuum.h
 *	  header file for postgres vacuum cleaner and statistics analyzer
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/vacuum.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VACUUM_H
#define VACUUM_H

#include "fmgr.h"
#include "access/htup.h"
#include "access/genam.h"
#include "access/parallel.h"
#include "access/tidstore.h"
#include "catalog/pg_class.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_type.h"
#include "parser/parse_node.h"
#include "storage/buf.h"
#include "storage/lock.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"

/*
 * Flags for amparallelvacuumoptions to control the participation of bulkdelete
 * and vacuumcleanup in parallel vacuum.
 */

/*
 * Both bulkdelete and vacuumcleanup are disabled by default.  This will be
 * used by IndexAM's that don't want to or cannot participate in parallel
 * vacuum.  For example, if an index AM doesn't have a way to communicate the
 * index statistics allocated by the first ambulkdelete call to the subsequent
 * ones until amvacuumcleanup, the index AM cannot participate in parallel
 * vacuum.
 */
#define VACUUM_OPTION_NO_PARALLEL			0

/*
 * bulkdelete can be performed in parallel.  This option can be used by
 * index AMs that need to scan indexes to delete tuples.
 */
#define VACUUM_OPTION_PARALLEL_BULKDEL		(1 << 0)

/*
 * vacuumcleanup can be performed in parallel if bulkdelete is not performed
 * yet.  This will be used by IndexAM's that can scan the index if the
 * bulkdelete is not performed.
 */
#define VACUUM_OPTION_PARALLEL_COND_CLEANUP	(1 << 1)

/*
 * vacuumcleanup can be performed in parallel even if bulkdelete has already
 * processed the index.  This will be used by IndexAM's that scan the index
 * during the cleanup phase of index irrespective of whether the index is
 * already scanned or not during bulkdelete phase.
 */
#define VACUUM_OPTION_PARALLEL_CLEANUP		(1 << 2)

/* value for checking vacuum flags */
#define VACUUM_OPTION_MAX_VALID_VALUE		((1 << 3) - 1)

/* Abstract type for parallel vacuum state */
typedef struct ParallelVacuumState ParallelVacuumState;

/*----------
 * ANALYZE builds one of these structs for each attribute (column) that is
 * to be analyzed.  The struct and subsidiary data are in anl_context,
 * so they live until the end of the ANALYZE operation.
 *
 * The type-specific typanalyze function is passed a pointer to this struct
 * and must return true to continue analysis, false to skip analysis of this
 * column.  In the true case it must set the compute_stats and minrows fields,
 * and can optionally set extra_data to pass additional info to compute_stats.
 * minrows is its request for the minimum number of sample rows to be gathered
 * (but note this request might not be honored, eg if there are fewer rows
 * than that in the table).
 *
 * The compute_stats routine will be called after sample rows have been
 * gathered.  Aside from this struct, it is passed:
 *		fetchfunc: a function for accessing the column values from the
 *				   sample rows
 *		samplerows: the number of sample tuples
 *		totalrows: estimated total number of rows in relation
 * The fetchfunc may be called with rownum running from 0 to samplerows-1.
 * It returns a Datum and an isNull flag.
 *
 * compute_stats should set stats_valid true if it is able to compute
 * any useful statistics.  If it does, the remainder of the struct holds
 * the information to be stored in a pg_statistic row for the column.  Be
 * careful to allocate any pointed-to data in anl_context, which will NOT
 * be CurrentMemoryContext when compute_stats is called.
 *
 * Note: all comparisons done for statistical purposes should use the
 * underlying column's collation (attcollation), except in situations
 * where a noncollatable container type contains a collatable type;
 * in that case use the type's default collation.  Be sure to record
 * the appropriate collation in stacoll.
 *----------
 */
typedef struct VacAttrStats *VacAttrStatsP;

typedef Datum (*AnalyzeAttrFetchFunc) (VacAttrStatsP stats, int rownum,
									   bool *isNull);

typedef void (*AnalyzeAttrComputeStatsFunc) (VacAttrStatsP stats,
											 AnalyzeAttrFetchFunc fetchfunc,
											 int samplerows,
											 double totalrows);

typedef struct VacAttrStats
{
	/*
	 * These fields are set up by the main ANALYZE code before invoking the
	 * type-specific typanalyze function.  They don't necessarily match what
	 * is in pg_attribute, because some index opclasses store a different type
	 * than the underlying column/expression.  Therefore, use these fields for
	 * information about the datatype being fed to the typanalyze function.
	 */
	int			attstattarget;	/* -1 to use default */
	Oid			attrtypid;		/* type of data being analyzed */
	int32		attrtypmod;		/* typmod of data being analyzed */
	Form_pg_type attrtype;		/* copy of pg_type row for attrtypid */
	Oid			attrcollid;		/* collation of data being analyzed */
	MemoryContext anl_context;	/* where to save long-lived data */
	int16		elevel;			/* set to LOG for ANALYZE VERBOSE */

	/*
	 * These fields must be filled in by the typanalyze routine, unless it
	 * returns false.
	 */
	AnalyzeAttrComputeStatsFunc compute_stats;	/* function pointer */
	int			minrows;		/* Minimum # of rows wanted for stats */
	void	   *extra_data;		/* for extra type-specific data */

	/* These fields are used to compute stawidth during the compute_stats routine. */
	double                  totalwidelength;/* total length of toowide row */
	int                     widerow_num;    /* # of toowide row */

	/*
	 * These fields are to be filled in by the compute_stats routine. (They
	 * are initialized to zero when the struct is created.)
	 */
	bool		stats_valid;
	float4		stanullfrac;	/* fraction of entries that are NULL */
	int32		stawidth;		/* average width of column values */
	float4		stadistinct;	/* # distinct values */
	double		stadistinctbyseg;	/* # distinct values by segments*/
	int16		stakind[STATISTIC_NUM_SLOTS];
	Oid			staop[STATISTIC_NUM_SLOTS];
	Oid			stacoll[STATISTIC_NUM_SLOTS];
	int			numnumbers[STATISTIC_NUM_SLOTS];
	float4	   *stanumbers[STATISTIC_NUM_SLOTS];
	int			numvalues[STATISTIC_NUM_SLOTS];
	Datum	   *stavalues[STATISTIC_NUM_SLOTS];

	bytea *stahll;			/* storing hyperloglog counter for sampled data */
	bytea *stahll_full;			/* storing hyperloglog counter for entire table scan */
	/*
	 * These fields describe the stavalues[n] element types. They will be
	 * initialized to match attrtypid, but a custom typanalyze function might
	 * want to store an array of something other than the analyzed column's
	 * elements. It should then overwrite these fields.
	 */
	Oid			statypid[STATISTIC_NUM_SLOTS];
	int16		statyplen[STATISTIC_NUM_SLOTS];
	bool		statypbyval[STATISTIC_NUM_SLOTS];
	char		statypalign[STATISTIC_NUM_SLOTS];

	/*
	 * These fields are private to the main ANALYZE code and should not be
	 * looked at by type-specific functions.
	 */
	int			tupattnum;		/* attribute number within tuples */
	HeapTuple  *rows;			/* access info for std fetch function */
	TupleDesc	tupDesc;
	Datum	   *exprvals;		/* access info for index fetch function */
	bool	   *exprnulls;
	int			rowstride;
	bool		merge_stats;
	bool		corrnull;	  /* whether correlation value is null */
	bool		partitiontbl_qd; /* analyze is on QD and the policy of table is partitioned */
	float4		corrval;	  /* correlation gathered from segments */
} VacAttrStats;

<<<<<<< HEAD
typedef enum VacuumOption
{
	VACOPT_VACUUM = 1 << 0,		/* do VACUUM */
	VACOPT_ANALYZE = 1 << 1,	/* do ANALYZE */
	VACOPT_VERBOSE = 1 << 2,	/* print progress info */
	VACOPT_FREEZE = 1 << 3,		/* FREEZE option */
	VACOPT_FULL = 1 << 4,		/* FULL (non-concurrent) vacuum */
	VACOPT_SKIP_LOCKED = 1 << 5,	/* skip if cannot get lock */
	VACOPT_PROCESS_MAIN = 1 << 6,	/* skip if cannot get lock */
	VACOPT_PROCESS_TOAST = 1 << 7,	/* process the TOAST table, if any */
	VACOPT_DISABLE_PAGE_SKIPPING = 1 << 8,	/* don't skip any pages */
	VACOPT_BUFFER_USAGE_LIMIT = 1 << 9,	/* vacuum buffer usage limit */
	VACOPT_PARALLEL = 1 << 10,	/* parallel vacuum */

	/* Extra GPDB options */
	VACOPT_AO_AUX_ONLY = 1 << 11,
	VACOPT_ROOTONLY = 1 << 12,
	VACOPT_FULLSCAN = 1 << 13,
	VACOPT_SKIP_DATABASE_STATS = 1 << 14,
	VACOPT_ONLY_DATABASE_STATS = 1 << 15,

	/* AO vacuum phases. Mutually exclusive */
	VACOPT_AO_PRE_CLEANUP_PHASE = 1 << 16,
	VACOPT_AO_COMPACT_PHASE = 1 << 17,
	VACOPT_AO_POST_CLEANUP_PHASE = 1 << 18,
	VACOPT_UPDATE_DATFROZENXID = 1 << 19
} VacuumOption;

#define VACUUM_AO_PHASE_MASK (VACOPT_AO_PRE_CLEANUP_PHASE | \
							  VACOPT_AO_COMPACT_PHASE | \
							  VACOPT_AO_POST_CLEANUP_PHASE)
=======
/* flag bits for VacuumParams->options */
#define VACOPT_VACUUM 0x01		/* do VACUUM */
#define VACOPT_ANALYZE 0x02		/* do ANALYZE */
#define VACOPT_VERBOSE 0x04		/* output INFO instrumentation messages */
#define VACOPT_FREEZE 0x08		/* FREEZE option */
#define VACOPT_FULL 0x10		/* FULL (non-concurrent) vacuum */
#define VACOPT_SKIP_LOCKED 0x20 /* skip if cannot get lock */
#define VACOPT_PROCESS_MAIN 0x40	/* process main relation */
#define VACOPT_PROCESS_TOAST 0x80	/* process the TOAST table, if any */
#define VACOPT_DISABLE_PAGE_SKIPPING 0x100	/* don't skip any pages */
#define VACOPT_SKIP_DATABASE_STATS 0x200	/* skip vac_update_datfrozenxid() */
#define VACOPT_ONLY_DATABASE_STATS 0x400	/* only vac_update_datfrozenxid() */
>>>>>>> REL_18_BETA1_branch

/*
 * Values used by index_cleanup and truncate params.
 *
 * VACOPTVALUE_UNSPECIFIED is used as an initial placeholder when VACUUM
 * command has no explicit value.  When that happens the final usable value
 * comes from the corresponding reloption (though the reloption default is
 * usually used).
 */
typedef enum VacOptValue
{
	VACOPTVALUE_UNSPECIFIED = 0,
	VACOPTVALUE_AUTO,
	VACOPTVALUE_DISABLED,
	VACOPTVALUE_ENABLED,
} VacOptValue;

/*
 * To avoid consuming too much memory during analysis and/or too much space
 * in the resulting pg_statistic rows, ANALYZE ignores varlena datums that are wider
 * than WIDTH_THRESHOLD (after detoasting!).  This is legitimate for MCV
 * and distinct-value calculations since a wide value is unlikely to be
 * duplicated at all, much less be a most-common value.  For the same reason,
 * ignoring wide values will not affect our estimates of histogram bin
 * boundaries very much.
 *
 * NOTE: In upstream, this is private to analyze.c, but GPDB needs it in
 * analyzefuncs.c
 */
#define WIDTH_THRESHOLD  1024

/*
 * VPgClassStats is used to hold the stats information that are stored in
 * pg_class. It is sent from QE to QD in a special libpq message , when a
 * QE runs VACUUM on a table.
 *
 * relallvisible
 * AO/AOCO does not currently have an equivalent to Heap's 'all visible pages',
 * relallvisible is always set to 0 in pg_class for AO/AOCO tabes. But QE use
 * this field in libpq to represent AO/AOCO's total file segment number when
 * vacuum ao tables.
 * Remember to reset relallvisible to 0 after qd get file segment number and
 * before updating pg_class.
 * See vac_update_relstats_from_list in vacuum.c and
 * vac_update_relstats in vacuum_ao.c.
 */
typedef struct VPgClassStats
{
	Oid			relid;
	BlockNumber rel_pages;
	double		rel_tuples;
	BlockNumber relallvisible;
} VPgClassStats;

typedef struct VPgClassStatsCombo
{
	Oid			relid;
	BlockNumber rel_pages;
	double		rel_tuples;
	BlockNumber relallvisible;

	int			count; /* expect to equal to the number of dispatched segments */
} VPgClassStatsCombo;

/*
 * Parameters customizing behavior of VACUUM and ANALYZE.
 *
 * Note that at least one of VACOPT_VACUUM and VACOPT_ANALYZE must be set
 * in options.
 *
 * When adding a new VacuumParam member, consider adding it to vacuumdb as
 * well.
 */
typedef struct VacuumParams
{
	bits32		options;		/* bitmask of VACOPT_* */
	int			freeze_min_age; /* min freeze age, -1 to use default */
	int			freeze_table_age;	/* age at which to scan whole table */
	int			multixact_freeze_min_age;	/* min multixact freeze age, -1 to
											 * use default */
	int			multixact_freeze_table_age; /* multixact age at which to scan
											 * whole table */
	bool		is_wraparound;	/* force a for-wraparound vacuum */
	int			log_min_duration;	/* minimum execution threshold in ms at
									 * which autovacuum is logged, -1 to use
									 * default */
	VacOptValue index_cleanup;	/* Do index vacuum and cleanup */
	VacOptValue truncate;		/* Truncate empty pages at the end */
	Oid			toast_parent;	/* for privilege checks when recursing */

	/*
	 * Fraction of pages in a relation that vacuum can eagerly scan and fail
	 * to freeze. Only applicable for table AMs using visibility maps. Derived
	 * from GUC or table storage parameter. 0 if disabled.
	 */
	double		max_eager_freeze_failure_rate;

	/*
	 * The number of parallel vacuum workers.  0 by default which means choose
	 * based on the number of indexes.  -1 indicates parallel vacuum is
	 * disabled.
	 */
	int			nworkers;
	int			buffer_usage_limit;
	bool auto_stats;      /* invoked via automatic statistic collection */
} VacuumParams;

typedef struct
{
	/* Table being sampled */
	Relation	onerel;
	int32		targrows;

	/* whether acquire inherited sample rows */
	bool        inherited;

	/* Sampled rows and estimated total number of rows in the table. */
	HeapTuple  *sample_rows;
	int			num_sample_rows;
	double		totalrows;
	double		totaldeadrows;

	/* Only used if current role is excutor */
	Datum*		stadistincts;

	/*
	 * Result tuple descriptor. Each returned row consists of three "fixed"
	 * columns, plus all the columns of the sampled table (excluding dropped
	 * columns).
	 */
	TupleDesc	outDesc;
#define NUM_SAMPLE_FIXED_COLS 4

	/* SRF state, to track which rows have already been returned. */
	int			index;
	bool		summary_sent;
} gp_acquire_sample_rows_context;

typedef struct
{
	/* Table being analyzed */
	Relation	onerel;

	/* whether acquire inherited table's correlations */
	bool        inherited;

	/*
	 * Result tuple descriptor.
	 */
	TupleDesc	outDesc;

	/* SRF state, to track which rows have already been returned. */
	int			index;
	int			totalAttr;
} gp_acquire_correlation_context;

/*
 * VacuumCutoffs is immutable state that describes the cutoffs used by VACUUM.
 * Established at the beginning of each VACUUM operation.
 */
struct VacuumCutoffs
{
	/*
	 * Existing pg_class fields at start of VACUUM
	 */
	TransactionId relfrozenxid;
	MultiXactId relminmxid;

	/*
	 * OldestXmin is the Xid below which tuples deleted by any xact (that
	 * committed) should be considered DEAD, not just RECENTLY_DEAD.
	 *
	 * OldestMxact is the Mxid below which MultiXacts are definitely not seen
	 * as visible by any running transaction.
	 *
	 * OldestXmin and OldestMxact are also the most recent values that can
	 * ever be passed to vac_update_relstats() as frozenxid and minmulti
	 * arguments at the end of VACUUM.  These same values should be passed
	 * when it turns out that VACUUM will leave no unfrozen XIDs/MXIDs behind
	 * in the table.
	 */
	TransactionId OldestXmin;
	MultiXactId OldestMxact;

	/*
	 * FreezeLimit is the Xid below which all Xids are definitely frozen or
	 * removed in pages VACUUM scans and cleanup locks.
	 *
	 * MultiXactCutoff is the value below which all MultiXactIds are
	 * definitely removed from Xmax in pages VACUUM scans and cleanup locks.
	 */
	TransactionId FreezeLimit;
	MultiXactId MultiXactCutoff;
};

/*
 * VacDeadItemsInfo stores supplemental information for dead tuple TID
 * storage (i.e. TidStore).
 */
typedef struct VacDeadItemsInfo
{
	size_t		max_bytes;		/* the maximum bytes TidStore can use */
	int64		num_items;		/* current # of entries */
} VacDeadItemsInfo;

/* GUC parameters */
extern PGDLLIMPORT int default_statistics_target;	/* PGDLLIMPORT for PostGIS */
extern PGDLLIMPORT int vacuum_freeze_min_age;
extern PGDLLIMPORT int vacuum_freeze_table_age;
extern PGDLLIMPORT int vacuum_multixact_freeze_min_age;
extern PGDLLIMPORT int vacuum_multixact_freeze_table_age;
extern PGDLLIMPORT int vacuum_failsafe_age;
extern PGDLLIMPORT int vacuum_multixact_failsafe_age;
extern PGDLLIMPORT bool track_cost_delay_timing;
extern PGDLLIMPORT bool vacuum_truncate;

/*
 * Relevant for vacuums implementing eager scanning. Normal vacuums may
 * eagerly scan some all-visible but not all-frozen pages. Since the goal
 * is to freeze these pages, an eager scan that fails to set the page
 * all-frozen in the VM is considered to have "failed". This is the
 * fraction of pages in the relation vacuum may scan and fail to freeze
 * before disabling eager scanning.
 */
extern PGDLLIMPORT double vacuum_max_eager_freeze_failure_rate;

/*
 * Maximum value for default_statistics_target and per-column statistics
 * targets.  This is fairly arbitrary, mainly to prevent users from creating
 * unreasonably large statistics that the system cannot handle well.
 */
#define MAX_STATISTICS_TARGET 10000

/* Variables for cost-based parallel vacuum */
extern PGDLLIMPORT pg_atomic_uint32 *VacuumSharedCostBalance;
extern PGDLLIMPORT pg_atomic_uint32 *VacuumActiveNWorkers;
extern PGDLLIMPORT int VacuumCostBalanceLocal;

extern PGDLLIMPORT bool VacuumFailsafeActive;
extern PGDLLIMPORT double vacuum_cost_delay;
extern PGDLLIMPORT int vacuum_cost_limit;

extern PGDLLIMPORT int64 parallel_vacuum_worker_delay_ns;

/* in commands/vacuum.c */
extern void ExecVacuum(ParseState *pstate, VacuumStmt *vacstmt, bool isTopLevel, bool auto_stats);
extern void vacuum(List *relations, VacuumParams *params,
				   BufferAccessStrategy bstrategy, MemoryContext vac_context,
				   bool isTopLevel);
extern void vac_open_indexes(Relation relation, LOCKMODE lockmode,
							 int *nindexes, Relation **Irel);
extern void vac_close_indexes(int nindexes, Relation *Irel, LOCKMODE lockmode);
extern double vac_estimate_reltuples(Relation relation,
									 BlockNumber total_pages,
									 BlockNumber scanned_pages,
									 double scanned_tuples);
extern void vac_send_relstats_to_qd(Relation relation,
						BlockNumber num_pages,
						double num_tuples,
						BlockNumber num_all_visible_pages);
extern void vac_update_relstats(Relation relation,
								BlockNumber num_pages,
								double num_tuples,
								BlockNumber num_all_visible_pages,
								BlockNumber num_all_frozen_pages,
								bool hasindex,
								TransactionId frozenxid,
								MultiXactId minmulti,
								bool *frozenxid_updated,
								bool *minmulti_updated,
								bool in_outer_xact,
								bool isvacuum);
extern bool vacuum_get_cutoffs(Relation rel, const VacuumParams *params,
							   struct VacuumCutoffs *cutoffs);
extern bool vacuum_xid_failsafe_check(const struct VacuumCutoffs *cutoffs);
extern void vac_update_datfrozenxid(void);
<<<<<<< HEAD
extern void vacuum_delay_point(void);
extern bool vacuum_is_relation_owner(Oid relid, Form_pg_class reltuple,
									 bits32 options);
=======
extern void vacuum_delay_point(bool is_analyze);
extern bool vacuum_is_permitted_for_relation(Oid relid, Form_pg_class reltuple,
											 bits32 options);
>>>>>>> REL_18_BETA1_branch
extern Relation vacuum_open_relation(Oid relid, RangeVar *relation,
									 bits32 options, bool verbose,
									 LOCKMODE lmode);
extern IndexBulkDeleteResult *vac_bulkdel_one_index(IndexVacuumInfo *ivinfo,
													IndexBulkDeleteResult *istat,
													TidStore *dead_items,
													VacDeadItemsInfo *dead_items_info);
extern IndexBulkDeleteResult *vac_cleanup_one_index(IndexVacuumInfo *ivinfo,
													IndexBulkDeleteResult *istat);

/* In postmaster/autovacuum.c */
extern void AutoVacuumUpdateCostLimit(void);
extern void VacuumUpdateCosts(void);
extern void vacuum_set_xid_limits(Relation rel,
								  int freeze_min_age, int freeze_table_age,
								  int multixact_freeze_min_age,
								  int multixact_freeze_table_age,
								  TransactionId *oldestXmin,
								  TransactionId *freezeLimit,
								  TransactionId *xidFullScanLimit,
								  MultiXactId *multiXactCutoff,
								  MultiXactId *mxactFullScanLimit);
/* in commands/vacuumparallel.c */
extern ParallelVacuumState *parallel_vacuum_init(Relation rel, Relation *indrels,
												 int nindexes, int nrequested_workers,
												 int vac_work_mem, int elevel,
												 BufferAccessStrategy bstrategy);
extern void parallel_vacuum_end(ParallelVacuumState *pvs, IndexBulkDeleteResult **istats);
extern TidStore *parallel_vacuum_get_dead_items(ParallelVacuumState *pvs,
												VacDeadItemsInfo **dead_items_info_p);
extern void parallel_vacuum_reset_dead_items(ParallelVacuumState *pvs);
extern void parallel_vacuum_bulkdel_all_indexes(ParallelVacuumState *pvs,
												long num_table_tuples,
												int num_index_scans);
extern void parallel_vacuum_cleanup_all_indexes(ParallelVacuumState *pvs,
												long num_table_tuples,
												int num_index_scans,
												bool estimated_count);
extern void parallel_vacuum_main(dsm_segment *seg, shm_toc *toc);

extern bool vacuumStatement_IsTemporary(Relation onerel);

/* in commands/analyze.c */
extern void analyze_rel(Oid relid, RangeVar *relation,
						VacuumParams *params, List *va_cols, bool in_outer_xact,
						BufferAccessStrategy bstrategy, gp_acquire_sample_rows_context *ctx);

/* in commands/vacuumlazy.c */
extern void lazy_vacuum_rel_heap(Relation onerel,
							VacuumParams *params, BufferAccessStrategy bstrategy);
extern void scan_index(Relation indrel, Relation aorel, int elevel, BufferAccessStrategy bstrategy);

/* in commands/vacuum_ao.c */
extern void ao_vacuum_rel(Relation rel, VacuumParams *params, BufferAccessStrategy bstrategy);

extern bool std_typanalyze(VacAttrStats *stats);

/* in utils/misc/sampling.c --- duplicate of declarations in utils/sampling.h */
extern double anl_random_fract(void);
extern double anl_init_selection_state(int n);
extern double anl_get_next_S(double t, int n, double *stateptr);

/* in commands/analyzefuncs.c */
extern Datum gp_acquire_sample_rows(PG_FUNCTION_ARGS);
extern Datum gp_acquire_correlations(PG_FUNCTION_ARGS);
extern Oid gp_acquire_sample_rows_col_type(Oid typid);

extern bool gp_vacuum_needs_update_stats(void);

#endif							/* VACUUM_H */
