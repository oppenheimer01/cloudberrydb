#-------------------------------------------------------------------------
#
# Makefile for backend/catalog
#
# Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/backend/catalog/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/catalog
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	aclchk.o \
	catalog.o \
	dependency.o \
	heap.o \
	index.o \
	indexing.o \
	namespace.o \
	objectaccess.o \
	objectaddress.o \
	partition.o \
	pg_aggregate.o \
	pg_attrdef.o \
	pg_cast.o \
	pg_class.o \
	pg_collation.o \
	pg_constraint.o \
	pg_conversion.o \
	pg_db_role_setting.o \
	pg_depend.o \
	pg_enum.o \
	pg_inherits.o \
	pg_largeobject.o \
	pg_namespace.o \
	pg_operator.o \
	pg_parameter_acl.o \
	pg_proc.o \
	pg_publication.o \
	pg_range.o \
	pg_shdepend.o \
	pg_subscription.o \
	pg_type.o \
	storage.o \
	toasting.o

OBJS += pg_extprotocol.o \
       pg_proc_callback.o \
       aoseg.o aoblkdir.o gp_fastsequence.o gp_segment_config.o \
       pg_attribute_encoding.o pg_compression.o aovisimap.o \
       pg_appendonly.o \
       oid_dispatch.o aocatalog.o storage_tablespace.o storage_database.o \
       storage_tablespace_twophase.o storage_tablespace_xact.o \
       gp_partition_template.o pg_task.o pg_task_run_history.o \
	   gp_matview_aux.o \
       pg_directory_table.o storage_directory_table.o

GP_SYSVIEW_IN = system_views_gp.in
GP_SYSVIEW_SQL = system_views_gp.sql

CATALOG_JSON:= $(addprefix $(top_srcdir)/gpMgmt/bin/gppylib/data/, $(addsuffix .json,$(GP_MAJORVERSION)))

include $(top_srcdir)/src/backend/common.mk

.PHONY: install-data
install-data: installdirs
	$(INSTALL_DATA) $(srcdir)/system_functions.sql '$(DESTDIR)$(datadir)/system_functions.sql'
	$(INSTALL_DATA) $(srcdir)/system_views.sql '$(DESTDIR)$(datadir)/system_views.sql'
	$(INSTALL_DATA) $(srcdir)/$(GP_SYSVIEW_SQL) '$(DESTDIR)$(datadir)/$(GP_SYSVIEW_SQL)'
	$(INSTALL_DATA) $(srcdir)/system_views_gp_summary.sql '$(DESTDIR)$(datadir)/system_views_gp_summary.sql'
	$(INSTALL_DATA) $(srcdir)/information_schema.sql '$(DESTDIR)$(datadir)/information_schema.sql'
	$(INSTALL_DATA) $(call vpathsearch,cdb_schema.sql) '$(DESTDIR)$(datadir)/cdb_init.d/cdb_schema.sql'
	$(INSTALL_DATA) $(srcdir)/sql_features.txt '$(DESTDIR)$(datadir)/sql_features.txt'
	$(INSTALL_DATA) $(srcdir)/fix-CVE-2024-4317.sql '$(DESTDIR)$(datadir)/fix-CVE-2024-4317.sql'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(datadir)'
	$(MKDIR_P) '$(DESTDIR)$(datadir)/cdb_init.d'

.PHONY: uninstall-data
uninstall-data:
	rm -f $(addprefix '$(DESTDIR)$(datadir)'/, system_functions.sql system_views.sql information_schema.sql sql_features.txt)
