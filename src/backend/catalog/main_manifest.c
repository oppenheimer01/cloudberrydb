/*-------------------------------------------------------------------------
 *
 * main_manifest.c
 *	  save all storage manifest info.
 *
 * Portions Copyright (c) 2023, HashData Technology Limited.
 *
 *
 * IDENTIFICATION
 *		src/backend/catalog/main_manifest.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/genam.h"
#include "access/table.h"
#include "catalog/indexing.h"
#include "catalog/main_manifest.h"
#include "utils/rel.h"
#include "catalog/dependency.h"

/*
 * RemoveMainManifestByRelnode
 *      Remove the main manifest record for the relnode.
 */
void
RemoveManifestRecord(RelFileNodeId relnode)
{
    Relation    main_manifest;
    HeapTuple   tuple;
    SysScanDesc scanDescriptor = NULL;
    ScanKeyData scanKey[1];

    main_manifest = table_open(ManifestRelationId, RowExclusiveLock);
    ScanKeyInit(&scanKey[0], Anum_main_manifest_relnode, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(relnode));

    scanDescriptor = systable_beginscan(main_manifest, InvalidOid,
                                        false, NULL, 1, scanKey);

    while (HeapTupleIsValid(tuple = systable_getnext(scanDescriptor)))
    {
        CatalogTupleDelete(main_manifest, &tuple->t_self);
    }

    systable_endscan(scanDescriptor);
    table_close(main_manifest, RowExclusiveLock);
}

void
InsertManifestRecord(Oid relid, RelFileNodeId relfilenode, text *path)
{
	Datum			values[2];
	HeapTuple		tuple;
	ObjectAddress	dep;
	ObjectAddress	ref;
	Relation		rel = heap_open(ManifestRelationId, RowExclusiveLock);
	bool 			nulls[2];

	values[0] = UInt64GetDatum(relfilenode);
	values[1] = PointerGetDatum(path);
	nulls[0] = false;
	nulls[1] = false;

	tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);

	CatalogTupleInsert(rel, tuple);

	table_close(rel, RowExclusiveLock);

	dep.classId = ManifestRelationId;
	dep.objectId = DatumGetObjectId(relfilenode);
	dep.objectSubId = 0;
	ref.classId = RelationRelationId;
	ref.objectId = relid;
	ref.objectSubId = 0;

	recordDependencyOn(&dep, &ref, DEPENDENCY_INTERNAL);
}

void
UpdateManifestRecord(RelFileNodeId relfilenode, text *path)
{
	Datum		values[2];
	HeapTuple	newtuple;
	HeapTuple	oldtuple;
	ScanKeyData	key;
	SysScanDesc	scan;
	bool		nulls[2];
	Relation	rel = heap_open(ManifestRelationId, RowExclusiveLock);

	ScanKeyInit(&key, Anum_main_manifest_relnode, BTEqualStrategyNumber,
				F_INT8EQ, UInt64GetDatum(relfilenode));

	scan = systable_beginscan(rel, InvalidOid, false, NULL, 1, &key);

	oldtuple = systable_getnext(scan);
	if (!HeapTupleIsValid(oldtuple))
		ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
						errmsg("write manifest catalog error")));

	values[0] = UInt64GetDatum(relfilenode);
	values[1] = PointerGetDatum(path);
	nulls[0] = false;
	nulls[1] = false;

	newtuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);

    CatalogTupleUpdate(rel, &oldtuple->t_self, newtuple);

	systable_endscan(scan);
	heap_close(rel, NoLock);
}

void
DeleteManifestCatalog(RelFileNodeId relnode)
{
	Relation entrance_rel = heap_open(ManifestRelationId, RowExclusiveLock);
	SysScanDesc	scan;
	HeapTuple tuple;

	ScanKeyData key;
	ScanKeyInit(&key, 1, BTEqualStrategyNumber, F_INT8EQ,
				UInt64GetDatum(relnode));
	scan = systable_beginscan(entrance_rel, InvalidOid, false, NULL, 1, &key);

	tuple = systable_getnext(scan);
	if (HeapTupleIsValid(tuple))
		CatalogTupleDelete(entrance_rel, &tuple->t_self);

	systable_endscan(scan);
	table_close(entrance_rel, RowExclusiveLock);
}