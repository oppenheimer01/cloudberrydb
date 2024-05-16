#include "postgres.h"

#include "funcapi.h"
#include "pg_config.h"

#include "access/genam.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/gp_warehouse.h"
#include "cdb/cdbvars.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"

PG_MODULE_MAGIC;

#define MAX_WAREHOUSE_SIZE (8)
#define CHAOS_LOCK_NAME "hashdata_chaos_lock"
#define CHAOS_OBJ_NAME "HashdataChaosShmem"

/* TODO: we need a local cache and pin some warehouse if chaos test start */
typedef struct HashdataChaosShmem_s {
	float8	threshold;
	int8	numWarehouses;
	Oid		oidWarehouses[MAX_WAREHOUSE_SIZE];
} HashdataChaosShmem_s;

/* function declarations */
void _PG_init(void);
void _PG_fini(void);

extern Datum hashdata_chaos_warehouse_begin(PG_FUNCTION_ARGS);
extern Datum hashdata_chaos_warehouse_end(PG_FUNCTION_ARGS);

static void hashdata_chaos_shmem_startup(void);
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static void hashdata_chaos_simple_query_hook(void (*exec_simple_query)(const char *), void *whereToSendOutput);

/* Links to shared memory state */
static HashdataChaosShmem_s *chaosShmem = NULL;
static LWLock *chaosLock = NULL;

/*
 * Module load callback
 */
void
_PG_init(void)
{
#ifdef FAULT_INJECTOR
	if (!process_shared_preload_libraries_in_progress)
		return;

	RequestNamedLWLockTranche(CHAOS_LOCK_NAME, 1);
	RequestAddinShmemSpace(sizeof(HashdataChaosShmem_s));

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = hashdata_chaos_shmem_startup;

	execSimpleQuery_Hook = hashdata_chaos_simple_query_hook;
#endif /* FAULT_INJECTOR*/
}

/*
 * shmem_startup hook: allocate or attach to shared memory,
 * then load any pre-existing statistics from file.
 * Also create and load the query-texts file, which is expected to exist
 * (even if empty) while the module is enabled.
 */
static void
hashdata_chaos_shmem_startup(void)
{
	bool	found;

	if (prev_shmem_startup_hook)
	{
		prev_shmem_startup_hook();
	}

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	chaosLock = &(GetNamedLWLockTranche(CHAOS_LOCK_NAME))->lock;

	chaosShmem = ShmemInitStruct(CHAOS_OBJ_NAME,
								 sizeof(HashdataChaosShmem_s),
								 &found);

	/* First time through */
	if (!found)
		MemSet(chaosShmem, 0, sizeof(HashdataChaosShmem_s));

	LWLockRelease(AddinShmemInitLock);
}

PG_FUNCTION_INFO_V1(hashdata_chaos_warehouse_begin);
Datum
hashdata_chaos_warehouse_begin(PG_FUNCTION_ARGS)
{
#ifdef FAULT_INJECTOR
	bool	ret = false;
	float8	threshold = PG_GETARG_FLOAT8(0);
	text	*pattern = cstring_to_text("chaos_%");

	if (threshold > 1 || threshold < 0)
		PG_RETURN_BOOL(ret);

	LWLockAcquire(chaosLock, LW_EXCLUSIVE);
	if (chaosShmem->numWarehouses == 0 &&
		chaosShmem->threshold == 0.0)
	{
		Relation rel;
		SysScanDesc scan;
		HeapTuple tuple;

		rel = table_open(GpWarehouseRelationId, AccessShareLock);
		scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);

		while ((tuple = systable_getnext(scan)) != NULL)
		{
			if (HeapTupleIsValid(tuple))
			{
				text *warehouse_name = &((Form_gp_warehouse)GETSTRUCT(tuple))->warehouse_name;
				Oid warehouseOid = ((Form_gp_warehouse)GETSTRUCT(tuple))->oid;

				if (OidFunctionCall2(F_LIKE_TEXT_TEXT,
									 PointerGetDatum(warehouse_name),
									 PointerGetDatum(pattern)))
				{
					chaosShmem->oidWarehouses[chaosShmem->numWarehouses++] = warehouseOid;
				}
			}

			if (chaosShmem->numWarehouses >= MAX_WAREHOUSE_SIZE)
				break;
		}

		chaosShmem->threshold = threshold;
		systable_endscan(scan);
		table_close(rel, NoLock);
		ret = (chaosShmem->numWarehouses != 0);
	}
	LWLockRelease(chaosLock);

	PG_RETURN_BOOL(ret);
#else
	ereport(WARNING,
			(errcode(ERRCODE_CHAOS_FRAMEWORK),
			 errmsg("chaos framework disable, enable with flags --enable-faultinjector ")));

	PG_RETURN_BOOL(false);
#endif
}

PG_FUNCTION_INFO_V1(hashdata_chaos_warehouse_end);
Datum hashdata_chaos_warehouse_end(PG_FUNCTION_ARGS)
{
#ifdef FAULT_INJECTOR
	LWLockAcquire(chaosLock, LW_EXCLUSIVE);
	MemSet(chaosShmem, 0, sizeof(HashdataChaosShmem_s));
	LWLockRelease(chaosLock);

	PG_RETURN_BOOL(true);
#else
	ereport(WARNING,
			(errcode(ERRCODE_CHAOS_FRAMEWORK),
			 errmsg("chaos framework disable, enable with flags --enable-faultinjector ")));

	PG_RETURN_BOOL(false);
#endif
}

static bool 
generate_cmd(Oid id, char *buffer, int lengh)
{
	bool		success = false;
	HeapTuple	tuple;
	Relation	rel;
	ScanKeyData	key[1];
	SysScanDesc	scan;

	ScanKeyInit(&key[0],
				Anum_gp_warehouse_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				id);
	rel = table_open(GpWarehouseRelationId, AccessShareLock);
	scan = systable_beginscan(rel, GpWarehouseOidIndexId, true, NULL, 1, key);
	tuple = systable_getnext(scan);
	if (HeapTupleIsValid(tuple))
	{
		text *warehouse_name = &((Form_gp_warehouse) GETSTRUCT(tuple))->warehouse_name;
		snprintf(buffer, lengh,"set warehouse to %s;", text_to_cstring(warehouse_name));
        success = true;
	}
	systable_endscan(scan);
	table_close(rel, NoLock);

	return success;
}

static void
hashdata_chaos_simple_query_hook(void (*exec_simple_query)(const char *),
								 void *whereToSendOutput)
{
	static char cmd[MAXPATHLEN];
	bool doswitch = false;

	if (Gp_role != GP_ROLE_DISPATCH)
	{
		return;
	}

	start_xact_command();
	memset(cmd, 0, MAXPATHLEN);

	if (!IsTransactionState())
	{
		return;
	}

	LWLockAcquire(chaosLock, LW_SHARED);
	if (chaosShmem->numWarehouses > 0 && chaosShmem->threshold > 0)
	{
		float8 lottery = DatumGetFloat8(OidFunctionCall0(F_RANDOM));
		doswitch = DatumGetBool(OidFunctionCall2(F_FLOAT8LT,
												 Float8GetDatum(lottery),
												 Float8GetDatum(chaosShmem->threshold)));
		if (doswitch)
		{
			int warehouseid = random() % chaosShmem->numWarehouses;
			doswitch = generate_cmd(chaosShmem->oidWarehouses[warehouseid],
									cmd, MAXPATHLEN);
			ereport(LOG,
					(errcode(ERRCODE_CHAOS_FRAMEWORK),
					 errmsg("auto switch warehouse: %s", cmd)));
		}
	}
	LWLockRelease(chaosLock);

	if (doswitch)
	{
		CommandDest orig_whereToSendOutput = *(CommandDest *)whereToSendOutput;
		*(CommandDest *)whereToSendOutput = DestNone;
		exec_simple_query(cmd);
		*(CommandDest *)whereToSendOutput = orig_whereToSendOutput;
	}
}