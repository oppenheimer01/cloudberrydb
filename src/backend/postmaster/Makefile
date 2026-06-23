#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for src/backend/postmaster
#
# IDENTIFICATION
#    src/backend/postmaster/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/postmaster
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)

OBJS = \
	autovacuum.o \
	auxprocess.o \
	bgworker.o \
	bgwriter.o \
	checkpointer.o \
	fork_process.o \
	interrupt.o \
	launch_backend.o \
	pgarch.o \
	pmchild.o \
	postmaster.o \
	startup.o \
	syslogger.o \
<<<<<<< HEAD
	walwriter.o \
	loginmonitor.o

OBJS += backoff.o autostats.o
=======
	walsummarizer.o \
	walwriter.o
>>>>>>> REL_18_BETA1_branch

include $(top_srcdir)/src/backend/common.mk
