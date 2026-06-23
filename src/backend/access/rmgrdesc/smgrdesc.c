/*-------------------------------------------------------------------------
 *
 * smgrdesc.c
 *	  rmgr descriptor routines for catalog/storage.c
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/smgrdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/storage_xlog.h"


void
smgr_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == XLOG_SMGR_CREATE)
	{
		xl_smgr_create *xlrec = (xl_smgr_create *) rec;
<<<<<<< HEAD
		char	   *path = relpathperm(xlrec->rlocator, xlrec->forkNum);
#ifndef FRONTEND
		appendStringInfo(buf, "%s; smgr: %s", path, smgr_get_name(xlrec->impl));
#else
		appendStringInfo(buf, "%s; smgr: %s", path, xlrec->impl == SMGR_MD ? "heap" : (xlrec->impl == SMGR_AO ? "ao" : "unknown"));
#endif
		pfree(path);
=======

		appendStringInfoString(buf,
							   relpathperm(xlrec->rlocator, xlrec->forkNum).str);
>>>>>>> REL_18_BETA1_branch
	}
	else if (info == XLOG_SMGR_TRUNCATE)
	{
		xl_smgr_truncate *xlrec = (xl_smgr_truncate *) rec;

		appendStringInfo(buf, "%s to %u blocks flags %d",
						 relpathperm(xlrec->rlocator, MAIN_FORKNUM).str,
						 xlrec->blkno, xlrec->flags);
	}
}

const char *
smgr_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_SMGR_CREATE:
			id = "CREATE";
			break;
		case XLOG_SMGR_TRUNCATE:
			id = "TRUNCATE";
			break;
	}

	return id;
}
