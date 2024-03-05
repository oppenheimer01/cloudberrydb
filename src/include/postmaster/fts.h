/*-------------------------------------------------------------------------
 *
 * fts.h
 *	  Interface for fault tolerance service (FTS).
 *
 * Portions Copyright (c) 2005-2010, Greenplum Inc.
 * Portions Copyright (c) 2011, EMC Corp.
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *		src/include/postmaster/fts.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FTS_H
#define FTS_H

#include "fts_comm.h"
#include "utils/guc.h"
#include "cdb/cdbutil.h"


typedef struct FtsResponse
{
	bool IsMirrorUp;
	bool IsInSync;
	bool IsSyncRepEnabled;
	bool IsRoleMirror;
	bool RequestRetry;
} FtsResponse;

extern bool am_ftsprobe;
extern bool am_ftshandler;
extern bool am_mirror;

/* buffer size for SQL command */
#define SQL_CMD_BUF_SIZE     1024

/*
 * Interface for checking if FTS is active
 */
extern bool FtsIsActive(void);

extern void SetSkipFtsProbe(bool skipFtsProbe);

/*
 * Interface for WALREP specific checking
 */
extern void HandleFtsMessage(const char* query_string);
extern void probeWalRepUpdateConfig(int16 dbid, int16 segindex, Oid warehouseid, 
									char role, bool IsSegmentAlive, bool IsInSync);

extern bool FtsProbeStartRule(Datum main_arg);
extern void FtsProbeMain (Datum main_arg);
extern pid_t FtsProbePID(void);

#endif   /* FTS_H */
