/*-------------------------------------------------------------------------
 *
 * catalog.c
 *		routines concerned with catalog naming conventions and other
 *		bits of hard-wired knowledge
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/catalog.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "access/transam.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_auth_time_constraint.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_database.h"
#include "catalog/pg_largeobject.h"
#include "catalog/pg_db_role_setting.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_replication_origin.h"
#include "catalog/pg_shdepend.h"
#include "catalog/pg_shdescription.h"
#include "catalog/pg_shseclabel.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_tag.h"
#include "catalog/pg_tag_description.h"
#include "catalog/pg_task.h"
#include "catalog/pg_task_run_history.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "catalog/gp_configuration_history.h"
#include "catalog/gp_id.h"
#include "catalog/gp_storage_server.h"
#include "catalog/gp_storage_user_mapping.h"
#include "catalog/gp_version_at_initdb.h"
#include "catalog/gp_warehouse.h"
#include "catalog/pg_event_trigger.h"
#include "catalog/pg_largeobject_metadata.h"
#include "catalog/pg_resourcetype.h"
#include "catalog/pg_resqueue.h"
#include "catalog/pg_resqueuecapability.h"
#include "catalog/pg_resgroup.h"
#include "catalog/pg_resgroupcapability.h"
#include "catalog/pg_profile.h"
#include "catalog/pg_password_history.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_stat_last_operation.h"
#include "catalog/pg_stat_last_shoperation.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_trigger.h"
#include "catalog/gp_matview_aux.h"
#include "catalog/gp_matview_tables.h"
#include "cdb/cdbvars.h"

#include "catalog/gp_indexing.h"
#ifdef USE_INTERNAL_FTS
#include "catalog/gp_segment_configuration_indexing.h"
#endif

static bool IsAoSegmentClass(Form_pg_class reltuple);
static bool IsExtAuxClass(Form_pg_class reltuple);
bool system_relation_modified = false;

/*
 * Like relpath(), but gets the directory containing the data file
 * and the filename separately.
 */
void
reldir_and_filename(RelFileNode node, BackendId backend, ForkNumber forknum,
					char **dir, char **filename)
{
	char	   *path;
	int			i;

	path = relpathbackend(node, backend, forknum);

	/*
	 * The base path is like "<path>/<rnode>". Split it into
	 * path and filename parts.
	 */
	for (i = strlen(path) - 1; i >= 0; i--)
	{
		if (path[i] == '/')
			break;
	}
	if (i <= 0 || path[i] != '/')
		elog(ERROR, "unexpected path: \"%s\"", path);

	*dir = pnstrdup(path, i);
	*filename = pstrdup(&path[i + 1]);

	pfree(path);
}

/*
 * Like relpathbackend(), but more convenient when dealing with
 * AO relations. The filename pattern is the same as for heap
 * tables, but this variant takes also 'segno' as argument.
 *
 * XXX This is very similar to _mdfd_segpath(), let's use that one
 */
char *
aorelpathbackend(RelFileNode node, BackendId backend, int32 segno)
{
	char	   *fullpath;
	char	   *path;

	path = relpathbackend(node, backend, MAIN_FORKNUM);
	if (segno == 0)
		fullpath = path;
	else
	{
		/* be sure we have enough space for the '.segno' */
		fullpath = (char *) palloc(strlen(path) + 12);
		sprintf(fullpath, "%s.%u", path, segno);
		pfree(path);
	}
	return fullpath;
}

/*
 * Parameters to determine when to emit a log message in
 * GetNewOidWithIndex()
 */
#define GETNEWOID_LOG_THRESHOLD 1000000
#define GETNEWOID_LOG_MAX_INTERVAL 128000000

/*
 * IsSystemRelation
 *		True iff the relation is either a system catalog or a toast table.
 *		See IsCatalogRelation for the exact definition of a system catalog.
 *
 *		We treat toast tables of user relations as "system relations" for
 *		protection purposes, e.g. you can't change their schemas without
 *		special permissions.  Therefore, most uses of this function are
 *		checking whether allow_system_table_mods restrictions apply.
 *		For other purposes, consider whether you shouldn't be using
 *		IsCatalogRelation instead.
 *
 *		This function does not perform any catalog accesses.
 *		Some callers rely on that!
 */
bool
IsSystemRelation(Relation relation)
{
	return IsSystemClass(RelationGetRelid(relation), relation->rd_rel);
}

/*
 * IsSystemClass
 *		Like the above, but takes a Form_pg_class as argument.
 *		Used when we do not want to open the relation and have to
 *		search pg_class directly.
 */
bool
IsSystemClass(Oid relid, Form_pg_class reltuple)
{
	/* IsCatalogRelationOid is a bit faster, so test that first */
	return (IsCatalogRelationOid(relid) || IsToastClass(reltuple) ||
			IsExtAuxClass(reltuple) ||
			IsAoSegmentClass(reltuple));
}

bool
IsSystemClassByRelid(Oid relid)
{
	Oid relnamespace = get_rel_namespace(relid);
	return (IsCatalogRelationOid(relid) || IsToastNamespace(relnamespace) ||
			IsAoSegmentNamespace(relnamespace));
}

/*
 * IsCatalogRelation
 *		True iff the relation is a system catalog.
 *
 *		By a system catalog, we mean one that is created during the bootstrap
 *		phase of initdb.  That includes not just the catalogs per se, but
 *		also their indexes, and TOAST tables and indexes if any.
 *
 *		This function does not perform any catalog accesses.
 *		Some callers rely on that!
 */
bool
IsCatalogRelation(Relation relation)
{
	return IsCatalogRelationOid(RelationGetRelid(relation));
}

/*
 * IsCatalogRelationOid
 *		True iff the relation identified by this OID is a system catalog.
 *
 *		By a system catalog, we mean one that is created during the bootstrap
 *		phase of initdb.  That includes not just the catalogs per se, but
 *		also their indexes, and TOAST tables and indexes if any.
 *
 *		This function does not perform any catalog accesses.
 *		Some callers rely on that!
 */
bool
IsCatalogRelationOid(Oid relid)
{
	/*
	 * We consider a relation to be a system catalog if it has an OID that was
	 * manually assigned or assigned by genbki.pl.  This includes all the
	 * defined catalogs, their indexes, and their TOAST tables and indexes.
	 *
	 * This rule excludes the relations in information_schema, which are not
	 * integral to the system and can be treated the same as user relations.
	 * (Since it's valid to drop and recreate information_schema, any rule
	 * that did not act this way would be wrong.)
	 *
	 * This test is reliable since an OID wraparound will skip this range of
	 * OIDs; see GetNewObjectId().
	 */
	return (relid < (Oid) FirstBootstrapObjectId);
}

/*
 * IsToastRelation
 *		True iff relation is a TOAST support relation (or index).
 *
 *		Does not perform any catalog accesses.
 */
bool
IsToastRelation(Relation relation)
{
	/*
	 * What we actually check is whether the relation belongs to a pg_toast
	 * namespace.  This should be equivalent because of restrictions that are
	 * enforced elsewhere against creating user relations in, or moving
	 * relations into/out of, a pg_toast namespace.  Notice also that this
	 * will not say "true" for toast tables belonging to other sessions' temp
	 * tables; we expect that other mechanisms will prevent access to those.
	 */
	return IsToastNamespace(RelationGetNamespace(relation));
}

/*
 * IsToastClass
 *		Like the above, but takes a Form_pg_class as argument.
 *		Used when we do not want to open the relation and have to
 *		search pg_class directly.
 */
bool
IsToastClass(Form_pg_class reltuple)
{
	Oid			relnamespace = reltuple->relnamespace;

	return IsToastNamespace(relnamespace);
}

/*
 * IsAoSegmentClass
 *		Like the above, but takes a Form_pg_class as argument.
 *		Used when we do not want to open the relation and have to
 *		search pg_class directly.
 */
static bool
IsAoSegmentClass(Form_pg_class reltuple)
{
	Oid			relnamespace = reltuple->relnamespace;

	return IsAoSegmentNamespace(relnamespace);
}

static bool
IsExtAuxClass(Form_pg_class reltuple)
{
	Oid			relnamespace = reltuple->relnamespace;

	return IsExtAuxNamespace(relnamespace);
}

/*
 * IsCatalogNamespace
 *		True iff namespace is pg_catalog.
 *
 *		Does not perform any catalog accesses.
 *
 * NOTE: the reason this isn't a macro is to avoid having to include
 * catalog/pg_namespace.h in a lot of places.
 */
bool
IsCatalogNamespace(Oid namespaceId)
{
	return namespaceId == PG_CATALOG_NAMESPACE;
}

/*
 * IsToastNamespace
 *		True iff namespace is pg_toast or my temporary-toast-table namespace.
 *
 *		Does not perform any catalog accesses.
 *
 * Note: this will return false for temporary-toast-table namespaces belonging
 * to other backends.  Those are treated the same as other backends' regular
 * temp table namespaces, and access is prevented where appropriate.
 * If you need to check for those, you may be able to use isAnyTempNamespace,
 * but beware that that does involve a catalog access.
 */
bool
IsToastNamespace(Oid namespaceId)
{
	return (namespaceId == PG_TOAST_NAMESPACE) ||
		isTempToastNamespace(namespaceId);
}

/*
 * IsAoSegmentNamespace
 *		True iff namespace is pg_aoseg.
 *
 * NOTE: the reason this isn't a macro is to avoid having to include
 * catalog/pg_namespace.h in a lot of places.
 */
bool
IsAoSegmentNamespace(Oid namespaceId)
{
	return namespaceId == PG_AOSEGMENT_NAMESPACE;
}

bool
IsExtAuxNamespace(Oid namespaceId)
{
	return namespaceId == PG_EXTAUX_NAMESPACE;
}

/*
 * IsReservedName
 *		True iff name starts with the pg_ prefix.
 *
 *		For some classes of objects, the prefix pg_ is reserved for
 *		system objects only.  As of 8.0, this was only true for
 *		schema and tablespace names.  With 9.6, this is also true
 *		for roles.
 */
bool
IsReservedName(const char *name)
{
	/* ugly coding for speed */
	return name[0] == 'p' && name[1] == 'g' && name[2] == '_';
}

/*
 * IsReservedGpName
 *		True iff name starts with the gp_ prefix.
 *
 *		Counterpart of IsReservedName but checks GPDB reserved name(s).
 * 		As of Greenplum 4.0 we reserve the prefix gp_ for schema and
 * 		tablespace names. We do not reserve it for role names to avoid 
 * 		impact to pre-7.0 users and also because the reason to reserve
 * 		pg_ for role names does not apply to pg_ (see #15259). 
 *
 *		As of 7.0 we do not reserve 'gp_toolkit' because it has been made
 * 		an extension which can be created or dropped by the user.
 */
bool
IsReservedGpName(const char *name)
{
	/* ugly coding for speed */
	return name[0] == 'g' && name[1] == 'p' && name[2] == '_' && 
							(strlen(name) < 10 || strcmp("gp_toolkit", name) != 0);
}

/*
 * GetReservedPrefix
 *		Given a string that is a reserved name return the portion of
 *      the name that makes it reserved - the reserved prefix.
 *
 *      Current return values include "pg_" and "gp_"
 */
char *
GetReservedPrefix(const char *name)
{
	char		*prefix = NULL;

	if (IsReservedName(name) || IsReservedGpName(name))
	{
		prefix = palloc(4);
		memcpy(prefix, name, 3);
		prefix[3] = '\0';
	}

	return prefix;
}

/*
 * IsSharedRelation
 *		Given the OID of a relation, determine whether it's supposed to be
 *		shared across an entire database cluster.
 *
 * In older releases, this had to be hard-wired so that we could compute the
 * locktag for a relation and lock it before examining its catalog entry.
 * Since we now have MVCC catalog access, the race conditions that made that
 * a hard requirement are gone, so we could look at relaxing this restriction.
 * However, if we scanned the pg_class entry to find relisshared, and only
 * then locked the relation, pg_class could get updated in the meantime,
 * forcing us to scan the relation again, which would definitely be complex
 * and might have undesirable performance consequences.  Fortunately, the set
 * of shared relations is fairly static, so a hand-maintained list of their
 * OIDs isn't completely impractical.
 */
bool
IsSharedRelation(Oid relationId)
{
	/* These are the shared catalogs (look for BKI_SHARED_RELATION) */
	if (relationId == AuthIdRelationId ||
		relationId == AuthMemRelationId ||
		relationId == DatabaseRelationId ||
		relationId == SharedDescriptionRelationId ||
		relationId == SharedDependRelationId ||
		relationId == SharedSecLabelRelationId ||
		relationId == TableSpaceRelationId ||
		relationId == DbRoleSettingRelationId ||
		relationId == ReplicationOriginRelationId ||
		relationId == SubscriptionRelationId)
		return true;

	/* GPDB additions */
	if (relationId == GpIdRelationId ||
		relationId == GpVersionRelationId ||

		/* MPP-6929: metadata tracking */
		relationId == StatLastShOpRelationId ||

		relationId == ResQueueRelationId ||
		relationId == ResourceTypeRelationId ||
		relationId == ResQueueCapabilityRelationId ||
		relationId == ResGroupRelationId ||
		relationId == ResGroupCapabilityRelationId ||
		relationId == GpConfigHistoryRelationId ||
#ifdef USE_INTERNAL_FTS
		relationId == GpSegmentConfigRelationId ||
#endif
		relationId == AuthTimeConstraintRelationId ||

		relationId == StorageUserMappingRelationId ||
		relationId == StorageServerRelationId ||

		relationId == ProfileRelationId ||
		relationId == PasswordHistoryRelationId ||
		
		relationId == TagRelationId ||
		relationId == TagDescriptionRelationId)
		return true;

	/* These are their indexes */
	if (relationId == AuthIdRolnameIndexId ||
		relationId == AuthIdOidIndexId ||
		relationId == AuthMemRoleMemIndexId ||
		relationId == AuthMemMemRoleIndexId ||
		relationId == DatabaseNameIndexId ||
		relationId == DatabaseOidIndexId ||
		relationId == SharedDescriptionObjIndexId ||
		relationId == SharedDependDependerIndexId ||
		relationId == SharedDependReferenceIndexId ||
		relationId == SharedSecLabelObjectIndexId ||
		relationId == TablespaceOidIndexId ||
		relationId == TablespaceNameIndexId ||
		relationId == DbRoleSettingDatidRolidIndexId ||
		relationId == ReplicationOriginIdentIndex ||
		relationId == ReplicationOriginNameIndex ||
		relationId == SubscriptionObjectIndexId ||
		relationId == SubscriptionNameIndexId)
		return true;

	/* GPDB added indexes */
	if (/* MPP-6929: metadata tracking */
		relationId == StatLastShOpClassidObjidIndexId ||
		relationId == StatLastShOpClassidObjidStaactionnameIndexId ||
		relationId == ResQueueOidIndexId ||
		relationId == ResQueueRsqnameIndexId ||
		relationId == ResourceTypeOidIndexId ||
		relationId == ResourceTypeRestypidIndexId ||
		relationId == ResourceTypeResnameIndexId ||
		relationId == ResQueueCapabilityResqueueidIndexId ||
		relationId == ResQueueCapabilityRestypidIndexId ||
		relationId == ResGroupOidIndexId ||
		relationId == ResGroupRsgnameIndexId ||
		relationId == ResGroupCapabilityResgroupidIndexId ||
		relationId == ResGroupCapabilityResgroupidResLimittypeIndexId ||
		relationId == AuthIdRolResQueueIndexId ||
		relationId == AuthIdRolResGroupIndexId ||
#ifdef USE_INTERNAL_FTS
		relationId == GpSegmentConfigContentPreferred_roleWarehouseIndexId ||
		relationId == GpSegmentConfigDbidWarehouseIndexId ||
#endif
		relationId == AuthTimeConstraintAuthIdIndexId ||
		relationId == AuthIdRolProfileIndexId ||
		relationId == StorageUserMappingOidIndexId ||
		relationId == StorageUserMappingServerIndexId ||
		relationId == StorageServerOidIndexId ||
		relationId == StorageServerNameIndexId ||
		relationId == ProfilePrfnameIndexId ||
		relationId == ProfileOidIndexId ||
		relationId == ProfileVerifyFunctionIndexId ||
		relationId == PasswordHistoryRolePasswordIndexId ||
		relationId == PasswordHistoryRolePasswordsetatIndexId ||
		relationId == TagNameIndexId ||
		relationId == TagOidIndexId ||
		relationId == TagDescriptionIndexId ||
		relationId == TagDescriptionTagidvalueIndexId ||
		relationId == TagDescriptionOidIndexId)
	{
		return true;
	}

	/* These are their toast tables and toast indexes */
	if (relationId == PgAuthidToastTable ||
		relationId == PgAuthidToastIndex ||
		relationId == PgDatabaseToastTable ||
		relationId == PgDatabaseToastIndex ||
		relationId == PgDbRoleSettingToastTable ||
		relationId == PgDbRoleSettingToastIndex ||
		relationId == PgReplicationOriginToastTable ||
		relationId == PgReplicationOriginToastIndex ||
		relationId == PgShdescriptionToastTable ||
		relationId == PgShdescriptionToastIndex ||
		relationId == PgShseclabelToastTable ||
		relationId == PgShseclabelToastIndex ||
		relationId == PgSubscriptionToastTable ||
		relationId == PgSubscriptionToastIndex ||
		relationId == PgTablespaceToastTable ||
		relationId == PgTablespaceToastIndex ||
		relationId == PgPasswordHistoryToastTable ||
		relationId == PgPasswordHistoryToastIndex)
		return true;
#ifdef USE_INTERNAL_FTS
	/* GPDB added toast tables and their indexes */
	if (relationId == GpSegmentConfigToastTable ||
		relationId == GpSegmentConfigToastIndex)
	{
		return true;
	}
#endif

	/* GPDB added toast tables and toast indexes */
	if (relationId == GpStorageUserMappingToastTable ||
		relationId == GpStorageUserMappingToastIndex ||
		relationId == GpStorageServerToastTable ||
		relationId == GpStorageServerToastIndex)
		return true;

	/* GPDB added task tables and their indexes */
	if (relationId == TaskRelationId ||
		relationId == TaskJobNameUserNameIndexId ||
		relationId == TaskJobIdIndexId ||
		relationId == TaskRunHistoryRelationId ||
		relationId == TaskRunHistoryJobIdIndexId ||
		relationId == TaskRunHistoryRunIdIndexId)
	{
		return true;
	}

	/* warehouse table and its indexes */
	if (relationId == GpWarehouseRelationId ||
		relationId == GpWarehouseOidIndexId ||
		relationId == GpWarehouseNameIndexId)
	{
		return true;
	}

	return false;
}

/*
 * OIDs for catalog object are normally allocated in the master, and
 * executor nodes should just use the OIDs passed by the master. But
 * there are some exceptions.
 */
static bool
RelationNeedsSynchronizedOIDs(Relation relation)
{
	if (IsCatalogNamespace(RelationGetNamespace(relation)))
	{
		switch(RelationGetRelid(relation))
		{
			/*
			 * pg_largeobject is more like a user table, and has
			 * different contents in each segment and master.
			 *
			 * Large objects don't work very consistently in GPDB. They are not
			 * distributed in the segments, but rather stored in the master node.
			 * Or actually, it depends on which node the lo_create() function
			 * happens to run, which isn't very deterministic.
			 */
			case LargeObjectRelationId:
			case LargeObjectMetadataRelationId:
				return false;

			/*
			 * We don't currently synchronize the OIDs of these catalogs.
			 * It's a bit sketchy that we don't, but we get away with it
			 * because these OIDs don't appear in any of the Node structs
			 * that are dispatched from master to segments. (Except for the
			 * OIDs, the contents of these tables should be in sync.)
			 */
			case RewriteRelationId:
			case TriggerRelationId:
				return false;

			/* Event triggers are only stored and fired in the QD. */
			case EventTriggerRelationId:
				return false;
		}

		/*
		 * All other system catalogs are assumed to need synchronized
		 * OIDs.
		 */
		return true;
	}
	return false;
}

/*
 * GetNewOidWithIndex
 *		Generate a new OID that is unique within the system relation.
 *
 * Since the OID is not immediately inserted into the table, there is a
 * race condition here; but a problem could occur only if someone else
 * managed to cycle through 2^32 OIDs and generate the same OID before we
 * finish inserting our row.  This seems unlikely to be a problem.  Note
 * that if we had to *commit* the row to end the race condition, the risk
 * would be rather higher; therefore we use SnapshotAny in the test, so that
 * we will see uncommitted rows.  (We used to use SnapshotDirty, but that has
 * the disadvantage that it ignores recently-deleted rows, creating a risk
 * of transient conflicts for as long as our own MVCC snapshots think a
 * recently-deleted row is live.  The risk is far higher when selecting TOAST
 * OIDs, because SnapshotToast considers dead rows as active indefinitely.)
 *
 * Note that we are effectively assuming that the table has a relatively small
 * number of entries (much less than 2^32) and there aren't very long runs of
 * consecutive existing OIDs.  This is a mostly reasonable assumption for
 * system catalogs.
 *
 * Caller must have a suitable lock on the relation.
 */
Oid
GetNewOidWithIndex(Relation relation, Oid indexId, AttrNumber oidcolumn)
{
	Oid			newOid;
	SysScanDesc scan;
	ScanKeyData key;
	bool		collides;
	uint64		retries = 0;
	uint64		retries_before_log = GETNEWOID_LOG_THRESHOLD;

	/* Only system relations are supported */
	Assert(IsSystemRelation(relation));

	/* In bootstrap mode, we don't have any indexes to use */
	if (IsBootstrapProcessingMode())
		return GetNewObjectId();

	/*
	 * We should never be asked to generate a new pg_type OID during
	 * pg_upgrade; doing so would risk collisions with the OIDs it wants to
	 * assign.  Hitting this assert means there's some path where we failed to
	 * ensure that a type OID is determined by commands in the dump script.
	 */
	Assert(!IsBinaryUpgrade || RelationGetRelid(relation) != TypeRelationId);

	/* Generate new OIDs until we find one not in the table */
	do
	{
		CHECK_FOR_INTERRUPTS();

		newOid = GetNewObjectId();

		ScanKeyInit(&key,
					oidcolumn,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(newOid));

		/* see notes above about using SnapshotAny */
		scan = systable_beginscan(relation, indexId, true,
								  SnapshotAny, 1, &key);

		collides = HeapTupleIsValid(systable_getnext(scan));

		/* GPDB: Also check that this OID hasn't been preallocated */
		if (!collides && !IsOidAcceptable(newOid))
			collides = true;

		systable_endscan(scan);

		/*
		 * Log that we iterate more than GETNEWOID_LOG_THRESHOLD but have not
		 * yet found OID unused in the relation. Then repeat logging with
		 * exponentially increasing intervals until we iterate more than
		 * GETNEWOID_LOG_MAX_INTERVAL. Finally repeat logging every
		 * GETNEWOID_LOG_MAX_INTERVAL unless an unused OID is found. This
		 * logic is necessary not to fill up the server log with the similar
		 * messages.
		 */
		if (retries >= retries_before_log)
		{
			ereport(LOG,
					(errmsg("still searching for an unused OID in relation \"%s\"",
							RelationGetRelationName(relation)),
					 errdetail_plural("OID candidates have been checked %llu time, but no unused OID has been found yet.",
									  "OID candidates have been checked %llu times, but no unused OID has been found yet.",
									  retries,
									  (unsigned long long) retries)));

			/*
			 * Double the number of retries to do before logging next until it
			 * reaches GETNEWOID_LOG_MAX_INTERVAL.
			 */
			if (retries_before_log * 2 <= GETNEWOID_LOG_MAX_INTERVAL)
				retries_before_log *= 2;
			else
				retries_before_log += GETNEWOID_LOG_MAX_INTERVAL;
		}

		retries++;
	} while (collides);

	/*
	 * If at least one log message is emitted, also log the completion of OID
	 * assignment.
	 */
	if (retries > GETNEWOID_LOG_THRESHOLD)
	{
		ereport(LOG,
				(errmsg_plural("new OID has been assigned in relation \"%s\" after %llu retry",
							   "new OID has been assigned in relation \"%s\" after %llu retries",
							   retries,
							   RelationGetRelationName(relation), (unsigned long long) retries)));
	}

	/*
	 * Most catalog objects need to have the same OID in the master and all
	 * segments. When creating a new object, the master should allocate the
	 * OID and tell the segments to use the same, so segments should have no
	 * need to ever allocate OIDs on their own. Therefore, give a WARNING if
	 * GetNewOid() is called in a segment. (There are a few exceptions, see
	 * RelationNeedsSynchronizedOIDs()).
	 */
	if (Gp_role == GP_ROLE_EXECUTE && RelationNeedsSynchronizedOIDs(relation))
		elog(PANIC, "allocated OID %u for relation \"%s\" in segment",
			 newOid, RelationGetRelationName(relation));

	return newOid;
}

static bool
GpCheckRelFileCollision(RelFileNodeBackend rnode)
{
	char	   *rpath;
	bool		collides;

	/* Check for existing file of same name */
	rpath = relpath(rnode, MAIN_FORKNUM);
	if (access(rpath, F_OK) == 0)
		collides = true;
	else
	{
		/*
		 * Here we have a little bit of a dilemma: if errno is something
		 * other than ENOENT, should we declare a collision and loop? In
		 * practice it seems best to go ahead regardless of the errno.  If
		 * there is a colliding file we will get an smgr failure when we
		 * attempt to create the new relation file.
		 */
		collides = false;
	}

	pfree(rpath);

	return collides;
}

/*
 * GetNewRelFileNode
 *		Generate a new relfilenode number that is unique within the
 *		database of the given tablespace.
 *
 * If the relfilenode will also be used as the relation's OID, pass the
 * opened pg_class catalog, and this routine will guarantee that the result
 * is also an unused OID within pg_class.  If the result is to be used only
 * as a relfilenode for an existing relation, pass NULL for pg_class.
 * (in GPDB, 'pg_class' is unused, there is a different mechanism to avoid
 * clashes, across the whole cluster.)
 *
 * As with GetNewOidWithIndex(), there is some theoretical risk of a race
 * condition, but it doesn't seem worth worrying about.
 *
 * Note: we don't support using this in bootstrap mode.  All relations
 * created by bootstrap have preassigned OIDs, so there's no need.
 */
Oid
GetNewRelFileNode(Oid reltablespace, Relation pg_class, char relpersistence)
{
	RelFileNodeBackend rnode;
	bool		collides;
	BackendId	backend;

	/*
	 * If we ever get here during pg_upgrade, there's something wrong; all
	 * relfilenode assignments during a binary-upgrade run should be
	 * determined by commands in the dump script.
	 *
	 * GPDB: Totally OK in Cloudberry. We don't use the table's OID as its
	 * initial relfilenode, and rely on this in binary upgrade, too.
	 */
	//Assert(!IsBinaryUpgrade);

	switch (relpersistence)
	{
		case RELPERSISTENCE_TEMP:
			backend = BackendIdForTempRelations();
			break;
		case RELPERSISTENCE_UNLOGGED:
		case RELPERSISTENCE_PERMANENT:
			backend = InvalidBackendId;
			break;
		default:
			elog(ERROR, "invalid relpersistence: %c", relpersistence);
			return InvalidOid;	/* placate compiler */
	}

	/* This logic should match RelationInitPhysicalAddr */
	rnode.node.spcNode = reltablespace ? reltablespace : MyDatabaseTableSpace;
	rnode.node.dbNode = (rnode.node.spcNode == GLOBALTABLESPACE_OID) ? InvalidOid : MyDatabaseId;

	/*
	 * The relpath will vary based on the backend ID, so we must initialize
	 * that properly here to make sure that any collisions based on filename
	 * are properly detected.
	 */
	rnode.backend = backend;

	do
	{
        CHECK_FOR_INTERRUPTS();

        /* Generate the Relfilenode */
        rnode.node.relNode = GetNewSegRelfilenode();

		collides = GpCheckRelFileCollision(rnode);

		if (!collides && rnode.node.spcNode != GLOBALTABLESPACE_OID)
		{
			/*
			 * GPDB_91_MERGE_FIXME: check again for a collision with a temp
			 * table (if this is a normal relation) or a normal table (if this
			 * is a temp relation).
			 *
			 * The shared buffer manager currently assumes that relfilenodes of
			 * relations stored in shared buffers can't conflict, which is
			 * trivially true in upstream because temp tables don't use shared
			 * buffers at all. We have to make this additional check to make
			 * sure of that.
			 */
			rnode.backend = (backend == InvalidBackendId) ? TempRelBackendId
														  : InvalidBackendId;
			collides = GpCheckRelFileCollision(rnode);
		}
	} while (collides);

	elog(DEBUG1, "Calling GetNewRelFileNode returns new relfilenode = %u", rnode.node.relNode);

	return rnode.node.relNode;
}

/*
 * SQL callable interface for GetNewOidWithIndex().  Outside of initdb's
 * direct insertions into catalog tables, and recovering from corruption, this
 * should rarely be needed.
 *
 * Function is intentionally not documented in the user facing docs.
 */
Datum
pg_nextoid(PG_FUNCTION_ARGS)
{
	Oid			reloid = PG_GETARG_OID(0);
	Name		attname = PG_GETARG_NAME(1);
	Oid			idxoid = PG_GETARG_OID(2);
	Relation	rel;
	Relation	idx;
	HeapTuple	atttuple;
	Form_pg_attribute attform;
	AttrNumber	attno;
	Oid			newoid;

	/*
	 * As this function is not intended to be used during normal running, and
	 * only supports system catalogs (which require superuser permissions to
	 * modify), just checking for superuser ought to not obstruct valid
	 * usecases.
	 */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to call pg_nextoid()")));

	rel = table_open(reloid, RowExclusiveLock);
	idx = index_open(idxoid, RowExclusiveLock);

	if (!IsSystemRelation(rel))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pg_nextoid() can only be used on system catalogs")));

	if (idx->rd_index->indrelid != RelationGetRelid(rel))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("index \"%s\" does not belong to table \"%s\"",
						RelationGetRelationName(idx),
						RelationGetRelationName(rel))));

	atttuple = SearchSysCacheAttName(reloid, NameStr(*attname));
	if (!HeapTupleIsValid(atttuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\" does not exist",
						NameStr(*attname), RelationGetRelationName(rel))));

	attform = ((Form_pg_attribute) GETSTRUCT(atttuple));
	attno = attform->attnum;

	if (attform->atttypid != OIDOID)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("column \"%s\" is not of type oid",
						NameStr(*attname))));

	if (IndexRelationGetNumberOfKeyAttributes(idx) != 1 ||
		idx->rd_index->indkey.values[0] != attno)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("index \"%s\" is not the index for column \"%s\"",
						RelationGetRelationName(idx),
						NameStr(*attname))));

	newoid = GetNewOidWithIndex(rel, idxoid, attno);

	ReleaseSysCache(atttuple);
	table_close(rel, RowExclusiveLock);
	index_close(idx, RowExclusiveLock);

	return newoid;
}
