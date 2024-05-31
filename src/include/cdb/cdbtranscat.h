#ifndef CDBTANSCAT_H
#define CDBTANSCAT_H

#include "access/heapam.h"
#include "access/htup.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "utils/hsearch.h"

typedef struct TransferTuple
{
	NodeTag type;

	uint32			t_len;
	ItemPointerData t_self;
	Oid				t_tableOid;
	char		   *t_data;
} TransferTuple;

typedef struct SystemTableTransferNode
{
	NodeTag	type;

	Oid 	my_temp_namespace;
	Oid 	my_temp_toast_namespace;
	List   *transfer_tuples;
} SystemTableTransferNode;

extern bool inPlPgsql;

extern void TransferReset(void);
extern bool IsTransferOn(void);
extern void SetTransferOff(void);
extern void SetTransferOn(void);
extern bool RelationStored(Oid relid);
extern bool RelationStoredCheck(Oid relid);
extern void TransStoreTuple(HeapTuple htup);
extern void TransRemoveTuple(Oid tableOid, ItemPointerData tid);
extern SystemTableTransferNode *GetTransferNode(void);
extern void SystemTupleStoreReset(void);
extern void SystemTupleStoreInit(const char *catalogBuffer, int catalogSize);
extern List *getSystemTupleList(Oid relid);
extern bool PlFuncStored(Oid funcid);

typedef void (*TransferReset_hook_type) (void);
extern PGDLLIMPORT TransferReset_hook_type TransferReset_hook;
typedef bool (*IsTransferOn_hook_type) (void);
extern PGDLLIMPORT IsTransferOn_hook_type IsTransferOn_hook;
typedef void (*SetTransferOff_hook_type) (void);
extern PGDLLIMPORT SetTransferOff_hook_type SetTransferOff_hook;
typedef void (*SetTransferOn_hook_type) (void);
extern PGDLLIMPORT SetTransferOn_hook_type SetTransferOn_hook;
typedef bool (*RelationStored_hook_type) (Oid relid);
extern PGDLLIMPORT RelationStored_hook_type RelationStored_hook;
typedef bool (*RelationStoredCheck_hook_type) (Oid relid);
extern PGDLLIMPORT RelationStoredCheck_hook_type RelationStoredCheck_hook;
typedef void (*TransStoreTuple_hook_type) (HeapTuple htup);
extern PGDLLIMPORT TransStoreTuple_hook_type TransStoreTuple_hook;
typedef void (*TransRemoveTuple_hook_type) (Oid tableOid, ItemPointerData tid);
extern PGDLLIMPORT TransRemoveTuple_hook_type TransRemoveTuple_hook;

typedef SystemTableTransferNode *(*GetTransferNode_hook_type) (void);
extern PGDLLIMPORT GetTransferNode_hook_type GetTransferNode_hook;
typedef void (*SystemTupleStoreReset_hook_type) (void);
extern PGDLLIMPORT SystemTupleStoreReset_hook_type SystemTupleStoreReset_hook;
typedef void (*SystemTupleStoreInit_hook_type) (const char *catalogBuffer, int catalogSize);
extern PGDLLIMPORT SystemTupleStoreInit_hook_type SystemTupleStoreInit_hook;
typedef List *(*getSystemTupleList_hook_type) (Oid relid);
extern PGDLLIMPORT getSystemTupleList_hook_type getSystemTupleList_hook;
typedef bool (*PlFuncStored_hook_type) (Oid funcid);
extern PGDLLIMPORT PlFuncStored_hook_type PlFuncStored_hook;



extern TableScanDesc systup_store_beginscan(Relation relation, int nkeys, ScanKey key,
											uint32 flags);
extern void systup_store_endscan(TableScanDesc sscan);
extern bool systup_store_getnextslot(TableScanDesc sscan, TupleTableSlot *slot);
extern bool systup_store_active(void);
extern bool systup_store_sorted_active(void);


typedef TableScanDesc (*systup_store_beginscan_hook_type) (Relation relation, int nkeys,
														   ScanKey key, uint32 flags);
extern PGDLLIMPORT systup_store_beginscan_hook_type systup_store_beginscan_hook;
typedef void (*systup_store_endscan_hook_type) (TableScanDesc sscan);
extern PGDLLIMPORT systup_store_endscan_hook_type systup_store_endscan_hook;
typedef bool (*systup_store_getnextslot_hook_type) (TableScanDesc sscan,
													TupleTableSlot *slot);
extern PGDLLIMPORT systup_store_getnextslot_hook_type systup_store_getnextslot_hook;
typedef bool (*systup_store_active_hook_type) (void);
extern PGDLLIMPORT systup_store_active_hook_type systup_store_active_hook;
typedef bool (*systup_store_sorted_active_hook_type) (void);
extern PGDLLIMPORT systup_store_sorted_active_hook_type systup_store_sorted_active_hook;


extern void DefaultValueStore(char *bin);
extern bool InTypeStore(void);
extern void TypeStore(Oid typeOid, int flags);
extern void InitQuery(const char *query_string);

typedef void (*DefaultValueStore_hook_type) (char *bin);
typedef bool (*InTypeStore_hook_type) (void);
typedef void (*TypeStore_hook_type) (Oid typeOid, int flags);
typedef void (*InitQuery_hook_type) (const char *query_string);
extern PGDLLIMPORT DefaultValueStore_hook_type DefaultValueStore_hook;
extern PGDLLIMPORT InTypeStore_hook_type InTypeStore_hook;
extern PGDLLIMPORT TypeStore_hook_type TypeStore_hook;
extern PGDLLIMPORT InitQuery_hook_type InitQuery_hook;


#endif //CDBTANSCAT_H