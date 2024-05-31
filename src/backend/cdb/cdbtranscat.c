
#include "postgres.h"

#include "fmgr.h"
#include "access/xact.h"
#include "access/nbtree.h"
#include "access/relation.h"
#include "access/htup_details.h"
#include "access/relscan.h"
#include "access/skey.h"
#include "access/valid.h"
#include "catalog/namespace.h"
#include "catalog/partition.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_language.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_config_map.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_ts_parser.h"
#include "catalog/pg_ts_template.h"
#include "catalog/pg_type.h"
#include "catalog/pg_user_mapping.h"
#include "cdb/cdbsreh.h"
#include "cdb/cdbtranscat.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbsrlz.h"
#include "commands/dbcommands.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "executor/functions.h"
#include "executor/nodeAgg.h"
#include "executor/tuptable.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "mb/pg_wchar.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "optimizer/optimizer.h"
#include "partitioning/partdesc.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteManip.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/partcache.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/rangetypes.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/varlena.h"

bool inPlPgsql = false;

TransferReset_hook_type TransferReset_hook = NULL;
IsTransferOn_hook_type IsTransferOn_hook = NULL;
SetTransferOff_hook_type SetTransferOff_hook = NULL;
SetTransferOn_hook_type SetTransferOn_hook = NULL;
RelationStored_hook_type RelationStored_hook = NULL;
RelationStoredCheck_hook_type RelationStoredCheck_hook = NULL;
TransStoreTuple_hook_type TransStoreTuple_hook = NULL;
TransRemoveTuple_hook_type TransRemoveTuple_hook = NULL;

GetTransferNode_hook_type GetTransferNode_hook = NULL;
SystemTupleStoreReset_hook_type SystemTupleStoreReset_hook = NULL;
SystemTupleStoreInit_hook_type SystemTupleStoreInit_hook = NULL;
getSystemTupleList_hook_type getSystemTupleList_hook = NULL;
PlFuncStored_hook_type PlFuncStored_hook = NULL;

void TransferReset(void)
{
	if (TransferReset_hook)
		(*TransferReset_hook) ();
}

bool IsTransferOn(void)
{
	if (IsTransferOn_hook)
		return (*IsTransferOn_hook) ();
	else
		return false;
}

void SetTransferOff(void)
{
	if (SetTransferOff_hook)
		(*SetTransferOff_hook) ();
}
void SetTransferOn(void)
{
	if (SetTransferOn_hook)
		(*SetTransferOn_hook) ();
}
bool RelationStored(Oid relid)
{
	if (RelationStored_hook)
		return (*RelationStored_hook) (relid);
	else
		return true;
}
bool RelationStoredCheck(Oid relid)
{
	if (RelationStoredCheck_hook)
		return (*RelationStoredCheck_hook) (relid);
	else
		return true;
}
void TransStoreTuple(HeapTuple htup)
{
	if (TransStoreTuple_hook)
		(*TransStoreTuple_hook) (htup);
}
void TransRemoveTuple(Oid tableOid, ItemPointerData tid)
{
	if (TransRemoveTuple_hook)
		(*TransRemoveTuple_hook) (tableOid, tid);
}
SystemTableTransferNode *GetTransferNode(void)
{
	if (GetTransferNode_hook)
		return (*GetTransferNode_hook) ();
	else
		return NULL;
}
void SystemTupleStoreReset(void)
{
	if (SystemTupleStoreReset_hook)
		(*SystemTupleStoreReset_hook) ();
}
void SystemTupleStoreInit(const char *catalogBuffer, int catalogSize)
{
	if (SystemTupleStoreInit_hook)
		(*SystemTupleStoreInit_hook) (catalogBuffer, catalogSize);
}
List *getSystemTupleList(Oid relid)
{
	if (getSystemTupleList_hook)
		return (*getSystemTupleList_hook) (relid);
	else
		return NIL;
}
bool PlFuncStored(Oid funcid)
{
	if (PlFuncStored_hook)
		return (*PlFuncStored_hook) (funcid);
	else
		return true;
}


systup_store_beginscan_hook_type systup_store_beginscan_hook = NULL;
systup_store_endscan_hook_type systup_store_endscan_hook = NULL;
systup_store_getnextslot_hook_type systup_store_getnextslot_hook = NULL;
systup_store_active_hook_type systup_store_active_hook = NULL;
systup_store_sorted_active_hook_type systup_store_sorted_active_hook = NULL;

TableScanDesc systup_store_beginscan(Relation relation, int nkeys, ScanKey key,
										uint32 flags)
{
	if (systup_store_beginscan_hook)
		return (*systup_store_beginscan_hook) (relation, nkeys, key, flags);
	else
		return NULL;
}
void systup_store_endscan(TableScanDesc sscan)
{
	if (systup_store_endscan_hook)
		(*systup_store_endscan_hook) (sscan);
}
bool systup_store_getnextslot(TableScanDesc sscan, TupleTableSlot *slot)
{
	if (systup_store_getnextslot_hook)
		return (*systup_store_getnextslot_hook) (sscan, slot);
	else
		return false;
}
bool systup_store_active(void)
{
	if (systup_store_active_hook)
		return (*systup_store_active_hook) ();
	else
		return false;
}
bool systup_store_sorted_active(void)
{
	if (systup_store_sorted_active_hook)
		return (*systup_store_sorted_active_hook) ();
	else
		return false;
}

DefaultValueStore_hook_type DefaultValueStore_hook = NULL;
InTypeStore_hook_type InTypeStore_hook = NULL;
TypeStore_hook_type TypeStore_hook = NULL;
InitQuery_hook_type InitQuery_hook = NULL;

void DefaultValueStore(char *bin)
{
	if (DefaultValueStore_hook)
		(*DefaultValueStore_hook) (bin);
}
bool InTypeStore(void)
{
	if (InTypeStore_hook)
		return (*InTypeStore_hook) ();
	else
		return true;
}
void TypeStore(Oid typeOid, int flags)
{
	if (TypeStore_hook)
		(*TypeStore_hook) (typeOid, flags);
}
void InitQuery(const char *query_string)
{
	if (InitQuery_hook)
		(*InitQuery_hook) (query_string);
}