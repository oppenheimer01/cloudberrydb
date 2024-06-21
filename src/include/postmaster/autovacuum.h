/*-------------------------------------------------------------------------
 *
 * autovacuum.h
 *	  header file for integrated autovacuum daemon
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/postmaster/autovacuum.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef AUTOVACUUM_H
#define AUTOVACUUM_H

#include "pgstat.h"
#include "catalog/pg_database.h"
#include "commands/vacuum.h"
#include "storage/block.h"
#include "utils/rel.h"

/*
 * Other processes can request specific work from autovacuum, identified by
 * AutoVacuumWorkItem elements.
 */
typedef enum
{
	AVW_BRINSummarizeRange
} AutoVacuumWorkItemType;

/* struct to keep track of tables to vacuum and/or analyze, in 1st pass */
typedef struct av_relation
{
	Oid		ar_toastrelid;	/* hash key - must be first */
	Oid		ar_relid;
	bool		ar_hasrelopts;
	AutoVacOpts ar_reloptions;	/* copy of AutoVacOpts from the main table's
								 * reloptions, or NULL if none */
} av_relation;

/* struct to keep track of tables to vacuum and/or analyze, after rechecking */
typedef struct autovac_table
{
	Oid		at_relid;
	VacuumParams	at_params;
	double		at_vacuum_cost_delay;
	int		at_vacuum_cost_limit;
	bool		at_dobalance;
	bool		at_sharedrel;
	char	   	*at_relname;
	char	   	*at_nspname;
	char	   	*at_datname;
} autovac_table;

/* GUC variables */
extern bool autovacuum_start_daemon;
extern int	autovacuum_max_workers;
extern int	autovacuum_work_mem;
extern int	autovacuum_naptime;
extern int	autovacuum_vac_thresh;
extern double autovacuum_vac_scale;
extern int	autovacuum_vac_ins_thresh;
extern double autovacuum_vac_ins_scale;
extern int	autovacuum_anl_thresh;
extern double autovacuum_anl_scale;
extern int	autovacuum_freeze_max_age;
extern int	autovacuum_multixact_freeze_max_age;
extern double autovacuum_vac_cost_delay;
extern int	autovacuum_vac_cost_limit;

/* autovacuum launcher PID, only valid when worker is shutting down */
extern int	AutovacuumLauncherPid;

extern int	Log_autovacuum_min_duration;

/* Status inquiry functions */
extern bool AutoVacuumingActive(void);
extern bool IsAutoVacuumLauncherProcess(void);
extern bool IsAutoVacuumWorkerProcess(void);

#define IsAnyAutoVacuumProcess() \
	(IsAutoVacuumLauncherProcess() || IsAutoVacuumWorkerProcess())

/* Hook for plugins to get control in AutoVacLauncher */
typedef void (*AutoVacLauncherMain_hook_type)(int argc, char *argv[]);
extern PGDLLIMPORT AutoVacLauncherMain_hook_type AutoVacLauncherMain_hook;

/* Hook for plugins to get control in vacuum relation list */
typedef List* (*AutoVacRelationList_hook_type)(Relation classRel,
					       Form_pg_database dbForm,
					       HTAB *table_toast_map,
					       PgStat_StatDBEntry *shared,
					       PgStat_StatDBEntry *dbentry,
					       TupleDesc pg_class_desc,
					       int effective_multixact_freeze_max_age,
					       MemoryContext AutovacMemCxt);
extern PGDLLIMPORT AutoVacRelationList_hook_type AutoVacRelationList_hook;

/* Hook for plugins to get control in recheck auto-vacuum/analyze table */
typedef autovac_table* (*TableRecheckAutoVac_hook_type) (Oid relid, HTAB *table_toast_map,
							 TupleDesc pg_class_desc,
							 int effective_multixact_freeze_max_age,
							 int default_freeze_min_age,
							 int default_freeze_table_age,
							 int default_multixact_freeze_min_age,
							 int default_multixact_freeze_table_age);
extern PGDLLIMPORT TableRecheckAutoVac_hook_type TableRecheckAutoVac_hook;

/* Functions to start autovacuum process, called from postmaster */
extern void autovac_init(void);
extern int	StartAutoVacLauncher(void);
extern int	StartAutoVacWorker(void);

/* called from postmaster when a worker could not be forked */
extern void AutoVacWorkerFailed(void);

/* autovacuum cost-delay balancer */
extern void AutoVacuumUpdateDelay(void);

#ifdef EXEC_BACKEND
extern void AutoVacLauncherMain(int argc, char *argv[]) pg_attribute_noreturn();
extern void AutoVacWorkerMain(int argc, char *argv[]) pg_attribute_noreturn();
extern void AutovacuumWorkerIAm(void);
extern void AutovacuumLauncherIAm(void);
#endif

extern bool AutoVacuumRequestWork(AutoVacuumWorkItemType type,
								  Oid relationId, BlockNumber blkno);

/* shared memory stuff */
extern Size AutoVacuumShmemSize(void);
extern void AutoVacuumShmemInit(void);

#endif							/* AUTOVACUUM_H */
