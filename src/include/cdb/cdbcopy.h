/*--------------------------------------------------------------------------
 *
 * cdbcopy.h
 *	 Definitions and API functions for cdbcopy.c
 *	 These are functions that are used by the backend
 *	 COPY command in Greenplum Database.
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/cdbcopy.h
 *
 *--------------------------------------------------------------------------
 */
#ifndef CDBCOPY_H
#define CDBCOPY_H

#include "access/aosegfiles.h" /* for InvalidFileSegNumber const */ 
#include "lib/stringinfo.h"
#include "cdb/cdbgang.h"

#define COPYOUT_CHUNK_SIZE 16 * 1024

struct pg_result;                   /* PGresult ... #include "libpq-fe.h" */

struct CdbDispatcherState;
struct CopyFromStateData;
struct CopyToStateData;

typedef struct CdbCopy
{
	int			total_segs;		/* total number of segments in cdb */
	bool		copy_in;		/* direction: true for COPY FROM false for COPY TO */

	StringInfoData	copy_out_buf;/* holds a chunk of data from the database */

	List		*seglist;    	/* segs that currently take part in copy.
								 * for copy out, once a segment gave away all it's
								 * data rows, it is taken out of the list */
	struct CdbDispatcherState *dispatcherState;
} CdbCopy;



/* global function declarations */
extern CdbCopy *makeCdbCopyFrom(struct CopyFromStateData *cstate);
extern CdbCopy *makeCdbCopyTo(struct CopyToStateData *cstate);
extern void cdbCopyStart(CdbCopy *cdbCopy, CopyStmt *stmt, int file_encoding);
extern void cdbCopySendDataToAll(CdbCopy *c, const char *buffer, int nbytes);
extern void cdbCopySendData(CdbCopy *c, int target_seg, const char *buffer, int nbytes);
extern bool cdbCopyGetData(CdbCopy *c, bool cancel, uint64 *rows_processed);
extern void cdbCopyAbort(CdbCopy *c);
extern void cdbCopyEnd(CdbCopy *c,
		   int64 *total_rows_completed_p,
		   int64 *total_rows_rejected_p);

typedef void (*CopyProcessResult_hook_type) (struct pg_result *res);
extern PGDLLIMPORT CopyProcessResult_hook_type CopyProcessResult_hook;

typedef void (*CdbCopyEnd_hook_type) (void);
extern PGDLLIMPORT CdbCopyEnd_hook_type CdbCopyEnd_hook;

#endif   /* CDBCOPY_H */
