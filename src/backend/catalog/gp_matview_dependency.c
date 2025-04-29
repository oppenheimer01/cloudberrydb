/*-------------------------------------------------------------------------
 *
 * gp_matview_dependency.c
 *	  Routines to support inter-object dependencies.
 *
 * Portions Copyright (c) 2024, HashData Technology Limited.
 *
 *
 * IDENTIFICATION
 *		src/backend/catalog/gp_matview_dependency.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/genam.h"
#include "access/table.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/gp_matview_dependency.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "utils/snapmgr.h"

void create_matview_dependency_tuple(Oid matviewOid, Relids relids, bool defer)
{
    Relation gp_matview_dependency;
    HeapTuple tup;
    Datum values[Natts_gp_matview_dependency];
    bool nulls[Natts_gp_matview_dependency];
    
    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));

    Oid *oids = palloc(sizeof(Oid) * bms_num_members(relids));

    int relid = -1;
    int i = 0;
    while ((relid = bms_next_member(relids, relid)) >= 0)
    {
        oids[i++] = relid;
    }
    oidvector  *depend_ids = buildoidvector(oids, bms_num_members(relids));

    gp_matview_dependency = table_open(MatviewDependencyId, RowExclusiveLock);

    values[Anum_gp_matview_dependency_matviewid - 1] = ObjectIdGetDatum(matviewOid);
    values[Anum_gp_matview_dependency_relids - 1] = PointerGetDatum(depend_ids);
    values[Anum_gp_matview_dependency_defer - 1] = BoolGetDatum(defer);
    values[Anum_gp_matview_dependency_partial - 1 ] = BoolGetDatum(true);
    values[Anum_gp_matview_dependency_trans_version - 1] = UInt64GetDatum(0);
    values[Anum_gp_matview_dependency_combine_version - 1] = UInt64GetDatum(0);
    values[Anum_gp_matview_dependency_isvaild - 1] = BoolGetDatum(true);
    values[Anum_gp_matview_dependency_refresh_time - 1] = 0;

    tup = heap_form_tuple(RelationGetDescr(gp_matview_dependency), values, nulls);
    CatalogTupleInsert(gp_matview_dependency, tup);
    heap_freetuple(tup);

    table_close(gp_matview_dependency, RowExclusiveLock);
    pfree(oids);

    CommandCounterIncrement();

    return;
}

Datum get_matview_dependency_relids(Oid matviewOid)
{
    Relation gp_matview_dependency;
    Datum result;

    ScanKeyData skey;
    SysScanDesc scan;
    HeapTuple tuple;
    bool    isnull;

    gp_matview_dependency = table_open(MatviewDependencyId, RowExclusiveLock);

    ScanKeyInit(&skey,
                Anum_gp_matview_dependency_matviewid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(matviewOid));

    scan = systable_beginscan(gp_matview_dependency, InvalidOid, false,
                              NULL, 1, &skey);

    tuple = systable_getnext(scan);
    if (!HeapTupleIsValid(tuple))
    {
        systable_endscan(scan);
        table_close(gp_matview_dependency, RowExclusiveLock);
        elog(ERROR, "cache lookup failed for matview %u", matviewOid);
    }

    result = heap_getattr(tuple, Anum_gp_matview_dependency_relids,
                          RelationGetDescr(gp_matview_dependency), &isnull);


    systable_endscan(scan);
    table_close(gp_matview_dependency, RowExclusiveLock);

    return result;
}

void
mark_matview_dependency_valid(Oid matviewOid, bool isvaild)
{
    Relation gp_matview_dependency;
    HeapTuple tup;
    SysScanDesc scanDescriptor = NULL;
    ScanKeyData scanKey[1];
    Datum       values[Natts_gp_matview_dependency];
    bool        nulls[Natts_gp_matview_dependency];
    bool        doreplace[Natts_gp_matview_dependency];

    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));
    memset(doreplace, false, sizeof(doreplace));

    gp_matview_dependency = table_open(MatviewDependencyId, RowExclusiveLock);

    ScanKeyInit(&scanKey[0], Anum_gp_matview_dependency_matviewid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(matviewOid));

    scanDescriptor = systable_beginscan(gp_matview_dependency, InvalidOid,
                                        false, NULL, 1, scanKey);

    while (HeapTupleIsValid(tup = systable_getnext(scanDescriptor)))
    {
        values[Anum_gp_matview_dependency_isvaild - 1] = BoolGetDatum(isvaild);
        doreplace[Anum_gp_matview_dependency_isvaild - 1] = true;

        tup = heap_modify_tuple(tup, RelationGetDescr(gp_matview_dependency), values, nulls, doreplace);
        CatalogTupleUpdate(gp_matview_dependency, &tup->t_self, tup);
        heap_freetuple(tup);
    }

    systable_endscan(scanDescriptor);
    table_close(gp_matview_dependency, RowExclusiveLock);
}

void
remove_matview_dependency_byoid(Oid matviewOid)
{
    Relation    gp_matview_dependency;
    HeapTuple   tup;
    SysScanDesc scanDescriptor = NULL;
    ScanKeyData scanKey[1];

    gp_matview_dependency = table_open(MatviewDependencyId, RowExclusiveLock);

    ScanKeyInit(&scanKey[0], Anum_gp_matview_dependency_matviewid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(matviewOid));

    scanDescriptor = systable_beginscan(gp_matview_dependency, InvalidOid,
                                        true, NULL, 1, scanKey);

    while (HeapTupleIsValid(tup = systable_getnext(scanDescriptor)))
    {
        CatalogTupleDelete(gp_matview_dependency, &tup->t_self);
    }

    systable_endscan(scanDescriptor);
    table_close(gp_matview_dependency, RowExclusiveLock);
}

uint64
get_restart_trans_version(Oid matviewOid, Snapshot snapshot)
{
    Relation gp_matview_dependency;
    Datum result;

    ScanKeyData skey;
    SysScanDesc scan;
    HeapTuple tuple;
    bool    isnull;

    if (snapshot == InvalidSnapshot)
        snapshot = GetLatestSnapshot();
    
    gp_matview_dependency = table_open(MatviewDependencyId, RowExclusiveLock);

    ScanKeyInit(&skey,
                Anum_gp_matview_dependency_matviewid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(matviewOid));

    scan = systable_beginscan(gp_matview_dependency, InvalidOid,
                                        false, snapshot, 1, &skey);

    tuple = systable_getnext(scan);
    if (!HeapTupleIsValid(tuple))
    {
        systable_endscan(scan);
        table_close(gp_matview_dependency, RowExclusiveLock);
        elog(ERROR, "cache lookup failed for matview %u", matviewOid);
    }

    result = heap_getattr(tuple, Anum_gp_matview_dependency_trans_version,
                          RelationGetDescr(gp_matview_dependency), &isnull);
    if (isnull)
    {
        result = UInt64GetDatum(0);
    }

    systable_endscan(scan);
    table_close(gp_matview_dependency, RowExclusiveLock);

    return DatumGetUInt64(result);
}

void
record_restart_trans_version(Oid matviewOid, uint64 version, TimestampTz ftime)
{
    Relation gp_matview_dependency;
    HeapTuple tup;
    SysScanDesc scanDescriptor = NULL;
    ScanKeyData scanKey[1];
    Datum       values[Natts_gp_matview_dependency];
    bool        nulls[Natts_gp_matview_dependency];
    bool        doreplace[Natts_gp_matview_dependency];

    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));
    memset(doreplace, false, sizeof(doreplace));

    gp_matview_dependency = table_open(MatviewDependencyId, RowExclusiveLock);

    ScanKeyInit(&scanKey[0], Anum_gp_matview_dependency_matviewid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(matviewOid));

    scanDescriptor = systable_beginscan(gp_matview_dependency, InvalidOid,
                                        false, NULL, 1, scanKey);

    while (HeapTupleIsValid(tup = systable_getnext(scanDescriptor)))
    {
        values[Anum_gp_matview_dependency_trans_version - 1] = UInt64GetDatum(version);
        doreplace[Anum_gp_matview_dependency_trans_version - 1] = true;

        values[Anum_gp_matview_dependency_refresh_time - 1] = TimestampTzGetDatum(ftime);
        doreplace[Anum_gp_matview_dependency_refresh_time - 1] = true;

        tup = heap_modify_tuple(tup, RelationGetDescr(gp_matview_dependency), values, nulls, doreplace);
        CatalogTupleUpdate(gp_matview_dependency, &tup->t_self, tup);
        heap_freetuple(tup);
    }

    systable_endscan(scanDescriptor);
    table_close(gp_matview_dependency, RowExclusiveLock);
}

void
record_restart_combine_version(Oid matviewOid, uint64 version, TimestampTz ftime)
{
    Relation gp_matview_dependency;
    HeapTuple tup;
    SysScanDesc scanDescriptor = NULL;
    ScanKeyData scanKey[1];
    Datum       values[Natts_gp_matview_dependency];
    bool        nulls[Natts_gp_matview_dependency];
    bool        doreplace[Natts_gp_matview_dependency];

    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));
    memset(doreplace, false, sizeof(doreplace));

    gp_matview_dependency = table_open(MatviewDependencyId, RowExclusiveLock);

    ScanKeyInit(&scanKey[0], Anum_gp_matview_dependency_matviewid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(matviewOid));

    scanDescriptor = systable_beginscan(gp_matview_dependency, InvalidOid,
                                        false, NULL, 1, scanKey);

    while (HeapTupleIsValid(tup = systable_getnext(scanDescriptor)))
    {
        values[Anum_gp_matview_dependency_combine_version - 1] = UInt64GetDatum(version);
        doreplace[Anum_gp_matview_dependency_combine_version - 1] = true;

        values[Anum_gp_matview_dependency_refresh_time - 1] = TimestampTzGetDatum(ftime);
        doreplace[Anum_gp_matview_dependency_refresh_time - 1] = true;

        tup = heap_modify_tuple(tup, RelationGetDescr(gp_matview_dependency), values, nulls, doreplace);
        CatalogTupleUpdate(gp_matview_dependency, &tup->t_self, tup);
        heap_freetuple(tup);
    }

    systable_endscan(scanDescriptor);
    table_close(gp_matview_dependency, RowExclusiveLock);
}
