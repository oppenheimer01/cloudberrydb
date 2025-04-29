/*-------------------------------------------------------------------------
 *
 * cdbrelsize.c
 *
 * Portions Copyright (c) 2006-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdbrelsize.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "catalog/catalog.h"
#include "cdb/cdbvars.h"
#include "miscadmin.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "libpq-fe.h"
#include "lib/stringinfo.h"
#include "utils/int8.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "utils/rel.h"

#include "cdb/cdbrelsize.h"

/*
 * Get the max size of the relation across segments
 */
int64
cdbRelMaxSegSize(Relation rel)
{
	int64		size = 0;

	/*
	 * Let's ask the QEs for the size of the relation
	 *
	 * Relation Oids are assumed to be in sync in all nodes.
	 */
	size = DatumGetInt64(DirectFunctionCall2(pg_relation_size,
			ObjectIdGetDatum(RelationGetRelid(rel)), CStringGetTextDatum("main")));

	return size;
}
