#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for backend/nodes
#
# IDENTIFICATION
#    src/backend/nodes/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/nodes
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

# Input header files for gen_node_support.pl
NODE_SUPPORT_INPUT = \
	$(top_srcdir)/src/include/catalog/gp_distribution_policy.h \
	$(top_srcdir)/src/include/nodes/nodes.h \
	$(top_srcdir)/src/include/nodes/primnodes.h \
	$(top_srcdir)/src/include/nodes/parsenodes.h \
	$(top_srcdir)/src/include/nodes/pathnodes.h \
	$(top_srcdir)/src/include/nodes/plannodes.h \
	$(top_srcdir)/src/include/nodes/execnodes.h \
	$(top_srcdir)/src/include/access/amapi.h \
	$(top_srcdir)/src/include/access/sdir.h \
	$(top_srcdir)/src/include/access/tableam.h \
	$(top_srcdir)/src/include/access/tsmapi.h \
	$(top_srcdir)/src/include/commands/event_trigger.h \
	$(top_srcdir)/src/include/commands/trigger.h \
	$(top_srcdir)/src/include/executor/tuptable.h \
	$(top_srcdir)/src/include/foreign/fdwapi.h \
	$(top_srcdir)/src/include/nodes/bitmapset.h \
	$(top_srcdir)/src/include/nodes/extensible.h \
	$(top_srcdir)/src/include/nodes/lockoptions.h \
	$(top_srcdir)/src/include/nodes/miscnodes.h \
	$(top_srcdir)/src/include/nodes/replnodes.h \
	$(top_srcdir)/src/include/nodes/supportnodes.h \
	$(top_srcdir)/src/include/nodes/value.h \
	$(top_srcdir)/src/include/utils/rel.h

# Generated files from gen_node_support.pl
NODE_SUPPORT_STAMPS = node-support-stamp

NODE_SUPPORT_OUTPUT = \
	copyfuncs.funcs.c copyfuncs.switch.c \
	readfuncs.funcs.c readfuncs.switch.c \
	outfuncs.funcs.c outfuncs.switch.c

node-support-stamp: $(NODE_SUPPORT_INPUT) $(srcdir)/gen_node_support.pl
	$(PERL) $(srcdir)/gen_node_support.pl --outdir $(CURDIR) $(NODE_SUPPORT_INPUT)
	touch $@

# Generated files depend on the stamp
$(NODE_SUPPORT_OUTPUT): node-support-stamp

# Make sure node files are generated before compiling
copyfuncs.o: copyfuncs.funcs.c copyfuncs.switch.c
readfuncs.o: readfuncs.funcs.c
outfuncs.o: outfuncs.funcs.c outfuncs.switch.c

CLEANFILES = node-support-stamp $(NODE_SUPPORT_OUTPUT)

OBJS = \
	bitmapset.o \
	copyfuncs.o \
	equalfuncs.o \
	extensible.o \
	list.o \
	makefuncs.o \
	multibitmapset.o \
	nodeFuncs.o \
	nodes.o \
	outfast.o \
	outfuncs.o \
	params.o \
	print.o \
	read.o \
	readfast.o \
	readfuncs.o \
	tidbitmap.o \
	value.o

include $(top_srcdir)/src/backend/common.mk
