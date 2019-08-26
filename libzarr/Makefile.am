# This is part of Unidata's netCDF package. Copyright 2018, see the
# COPYRIGHT file for more information.

# Build the ZARR dispatch layer.

# Dennis Heimbigner

# Get AM_CPPFLAGS.
include $(top_srcdir)/lib_flags.am
AM_CPPFLAGS += -I$(top_srcdir)/libsrc4
libzarr_la_CPPFLAGS = ${AM_CPPFLAGS}
libzarr_la_LIBADD =

# Remember ENABLE_ZARR implies USE_NETCDF4

# We may have to add to these later.
DISTCLEANFILES =
MAINTAINERCLEANFILES =
CLEANFILES =
EXTRA_DIST = 
LDADD=

# This is our output. The ZARR convenience library.
noinst_LTLIBRARIES = libzarr.la

# The source files.
libzarr_la_SOURCES = \
zdispatch.h \
zinternal.h \
zattr.c \
zcache.c \
zcreate.c \
zdim.c \
zdispatch.c \
zfile.c \
zgrp.c \
zinternal.c \
zopen.c \
zprov.c \
zvar.c \
zprovenance.h \
\
zarr.c \
zcvt.c \
zsync.c \
zodom.c \
zallchunks.c \
zchunkio.c \
zsliceindices.c \
zfile.c \
zclose.c \
zdim.c \
zmeta.c \
zjson.c \
zutil.c \
zmap.c \
zmap_nc4.c \
zdebug.c

HDRS= \
nczdispatch.h \
ncz.h \
zchunking.h \
zjson.h \
ztypes.h \
zincludes.h \
zmap.h \
zdebug.h


# For now, ignore these
IGNORED=ztype.c 

# Package this for cmake build.
EXTRA_DIST += CMakeLists.txt






