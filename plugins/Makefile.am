#    Copyright 2018, UCAR/Unidata
#    See netcdf/COPYRIGHT file for copying and redistribution conditions.

# Put Together AM_CPPFLAGS and AM_LDFLAGS
include $(top_srcdir)/lib_flags.am

AM_LDFLAGS += -module -avoid-version -shared -export-dynamic	\
 -rpath ${abs_builddir} ${NOUNDEFINED}

# BZIP2 version 1.0.8 (https://sourceware.org/bzip2/)
BZIP2HDRS = bzlib.h bzlib_private.h
BZIP2SRC = blocksort.c huffman.c crctable.c randtable.c compress.c decompress.c bzlib.c

BZ2PLUGINSRC = H5Zbzip2.c
BZ2PLUGINHDRS = h5bzip2.h

EXTRA_DIST = ${BZ2PLUGINSRC} ${BZ2PLUGINHDRS} \
             ${BZIP2SRC} ${BZIP2HDRS} \
             H5Ztemplate.c H5Zmisc.c H5Zutil.c H5Znoop.c h5noop.h CMakeLists.txt \
	     NCZdeflate.c NCZbzip2.c H5Zblosc.c H5Zblosc.h

if ENABLE_FILTER_TESTING

noinst_LTLIBRARIES = libh5misc.la libh5noop.la libh5noop1.la libnczdeflate.la libnczbzip2.la

lib_LTLIBRARIES = libh5bzip2.la
libh5bzip2_la_SOURCES = ${BZ2PLUGINSRC} ${BZIP2SRC}

if HAVE_BLOSC
if USE_HDF5
noinst_LTLIBRARIES += libh5blosc.la
libh5blosc_la_SOURCES = H5Zblosc.c H5Zblosc.h
endif
endif

libh5misc_la_SOURCES = H5Zmisc.c H5Zutil.c h5misc.h

libnczdeflate_la_SOURCES = NCZdeflate.c
libnczbzip2_la_SOURCES = NCZbzip2.c

# The noop filter is to allow testing of multifilters and filter order
# Need two distinct instances
libh5noop_la_SOURCES = H5Znoop.c H5Zutil.c h5noop.h
libh5noop1_la_SOURCES = H5Znoop1.c H5Zutil.c h5noop.h
endif #ENABLE_FILTER_TESTING

BUILT_SOURCES = H5Znoop1.c
DISTCLEANFILES = H5Znoop1.c ncjson.h
H5Znoop1.c: Makefile H5Znoop.c
	echo '#define NOOP_INSTANCE 1' > $@
	cat ${srcdir}/H5Znoop.c >> $@

# For reference: updating bzip2 
BZIP2DIR=/cygdrive/d/bzip2-1.0.8
updatebzip2:
	for b in ${BZIP2HDRS} ${BZIP2SRC} ; do cp -f ${BZIP2DIR}/$${b} . ; done
