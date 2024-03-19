# contrib/hashdata_chaos/Makefile
subdir = contrib/hashdata_chaos
top_builddir = ../..

MODULE_big = hashdata_chaos
PGFILEDESC = "hashdata chaos framework"

OBJS = hashdata_chaos.o \
       reghouse.o

EXTENSION = hashdata_chaos
DATA = hashdata_chaos--1.0.sql

# REGRESS = 

# EXTRA_CLEAN = 

# REGRESS_OPTS =

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/hashodata_chaos
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
