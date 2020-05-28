# This is part of Unidata's netCDF package. Copyright 2018, see the
# COPYRIGHT file for more information.

# Build the ZARR dispatch layer.

# Dennis Heimbigner

# Get AM_CPPFLAGS and AM_LDFLAGS
include $(top_srcdir)/lib_flags.am
AM_CPPFLAGS += -D_LARGEFILE_SOURCE
AM_CPPFLAGS += -I$(top_srcdir)/libsrc4 -I$(top_srcdir)/libdap4
libnczarr_la_CPPFLAGS = ${AM_CPPFLAGS}
libnczarr_la_CXXFLAGS = 
libnczarr_la_LIBADD =

# Remember ENABLE_NCZARR implies USE_NETCDF4

# We may have to add to these later.
DISTCLEANFILES =
MAINTAINERCLEANFILES =
CLEANFILES =
EXTRA_DIST = 
LDADD=

# This is our output. The ZARR convenience library.
noinst_LTLIBRARIES = libnczarr.la

# The source files.
libnczarr_la_SOURCES = \
zarr.c \
zattr.c \
zcache.c \
zchunking.c \
zclose.c \
zcreate.c \
zcvt.c \
zdebug.c \
zdim.c \
zdispatch.c \
zfile.c \
zfilter.c \
zgrp.c \
zinternal.c \
zjson.c \
zodom.c \
zopen.c \
zprov.c \
zsync.c \
ztype.c \
zutil.c \
zvar.c \
zwalk.c \
zmap.c \
zmap_nz4.c \
zmap_nzf.c

HDRS= \
zdispatch.h \
zinternal.h \
zchunking.h \
zjson.h \
zincludes.h \
zmap.h \
zprovenance.h \
zdebug.h

if ENABLE_S3_SDK
libnczarr_la_SOURCES += zmap_s3sdk.c
libnczarr_la_SOURCES += zs3sdk.cpp
endif

# For now, ignore these
IGNORED=ztype.c 

# Package this for cmake build.
EXTRA_DIST += CMakeLists.txt
