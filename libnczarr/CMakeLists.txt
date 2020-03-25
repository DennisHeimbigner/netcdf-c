## This is a CMake file, part of Unidata's netCDF package.
# Copyright 2018, see the COPYRIGHT file for more information.
#
# This builds the libzarr dispatch layer.
#
# Dennis Heimbigner

# The source files for the HDF5 dispatch layer.
SET(libnczarr_SOURCES
zarr.c
zattr.c
zcache.c
zchunking.c
zclose.c
zcreate.c
zcvt.c
zdebug.c
zdim.c
zdispatch.c
zfile.c
zfilter.c
zgrp.c
zinternal.c
zjson.c
zmap.c
zmap_nc4.c
zodom.c
zopen.c
zprov.c
zsync.c
ztype.c
zutil.c
zvar.c
zwalk.c
)

IF(NOT ENABLE_DAP4)

SET(libnczarr_SOURCES ${libnczarr_SOURCES} ezxml.c
ENDIF()

# Build the Zarr dispatch layer as a library that will be included in
# the netCDF library.
add_library(nczarr OBJECT ${libnczarr_SOURCES})

# Remember to package this file for CMake builds.
ADD_EXTRA_DIST(${libnczarr_SOURCES} CMakeLists.txt)

