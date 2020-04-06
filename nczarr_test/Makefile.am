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

AM_CPPFLAGS += -I${top_builddir}/libnczarr
AM_LDFLAGS += ${top_builddir}/liblib/libnetcdf.la
LDADD = ${top_builddir}/liblib/libnetcdf.la

# Set up the tests; do the .sh first, then .c
check_PROGRAMS =
TESTS =

# Unit Tests
check_PROGRAMS += ut_map ut_json ut_projections

commonsrc = ut_util.c ut_test.c

ut_map_SOURCES = ut_map.c ${commonsrc}
ut_json_SOURCES = ut_json.c ${commonsrc}
ut_projections_SOURCES = ut_projections.c ${commonsrc}

# Integration Tests
#check_PROGRAMS += it_meta

TESTS += run_unittests.sh

#TESTS += run_meta_tests.sh

#noinst_PROGRAMS = z4dump
#z4dump_SOURCES = z4dump.c

# Given a netcdf4 file, dump the actual chunk contents.
# Used to validate nczarr chunking code.
noinst_PROGRAMS = ncdumpchunks
ncdumpchunks_SOURCES = ncdumpchunks.c

EXTRA_DIST = run_unittests.sh run_meta_tests.sh \
	ref_ut_mapnc4_create.cdl ref_ut_mapnc4_meta.cdl \
	ref_ut_mapnc4_wmeta.cdl ref_ut_mapnc4_wmeta2.cdl \
	ref_ut_mapnc4_write.cdl ref_ut_mapnc4_rmeta.txt \
	ref_ut_mapnc4_search.txt ref_ut_proj1.txt \
	ref_ut_json_build.txt ref_ut_json_parse.txt \
	ref_t_ncgen.cdl

CLEANFILES = *.exe
CLEANFILES += testnc4.nc
CLEANFILES += ut_mapnc4_create.cdl ut_mapnc4_meta.cdl \
	ut_mapnc4_wmeta.cdl ut_mapnc4_wmeta2.cdl \
	ut_mapnc4_write.cdl ut_mapnc4_rmeta.txt \
	ut_mapnc4_search.txt ut_proj1.txt \
	ut_json_build.txt ut_json_parse.txt
CLEANFILES += t_ncgen.cdl

# This should only be left behind if using parallel io
CLEANFILES += tmp_*
