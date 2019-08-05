## This is a automake file, part of Unidata's netCDF package.
# Copyright 2018, see the COPYRIGHT file for more information.

# This file builds and runs DAP4 tests.

# Put together AM_CPPFLAGS and AM_LDFLAGS.
include $(top_srcdir)/lib_flags.am

#TEST_EXTENSIONS = .sh

#SH_LOG_DRIVER = $(SHELL) $(top_srcdir)/test-driver-verbose
#sh_LOG_DRIVER = $(SHELL) $(top_srcdir)/test-driver-verbose
#LOG_DRIVER = $(SHELL) $(top_srcdir)/test-driver-verbose
#TESTS_ENVIRONMENT = export SETX=1;

AM_CPPFLAGS += -I${top_builddir}/libzarr
AM_LDFLAGS += ${top_builddir}/liblib/libnetcdf.la
LDADD = ${top_builddir}/liblib/libnetcdf.la

# Set up the tests; do the .sh first, then .c
check_PROGRAMS =
TESTS =

check_PROGRAMS += ut_mapnc4
check_PROGRAMS += ut_projections
TESTS += run_unittests.sh

noinst_PROGRAMS = z4dump
z4dump_SOURCES = z4dump.c

EXTRA_DIST = run_unittests.sh \
	ref_ut_mapnc4_create.cdl ref_ut_mapnc4_meta.cdl \
	ref_ut_mapnc4_wmeta.cdl ref_ut_mapnc4_wmeta2.cdl \
	ref_ut_mapnc4_write.cdl ref_ut_mapnc4_rmeta.txt \
	ref_ut_mapnc4_proj1.txt

CLEANFILES = *.exe
CLEANFILES += testnc4.nc
CLEANFILES += ut_mapnc4_create.cdl ut_mapnc4_meta.cdl \
	ut_mapnc4_wmeta.cdl ut_mapnc4_wmeta2.cdl \
	ut_mapnc4_write.cdl ut_mapnc4_rmeta.txt \
	ut_proj1.txt

# This should only be left behind if using parallel io
CLEANFILES += tmp_*
