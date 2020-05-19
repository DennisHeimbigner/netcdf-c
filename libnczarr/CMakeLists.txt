## This is a CMake file, part of Unidata's netCDF package.
# Copyright 2018, see the COPYRIGHT file for more information.
#
# This builds the libzarr dispatch layer.
#
# Dennis Heimbigner

# The source files for the HDF5 dispatch layer.
SET(libnczarr_SOURCES
)

IF(ENABLE_S3_SDK) 
SET(libnczarr_SOURCES ${libnczarr_SOURCES} zmap_s3sdk.c zs3sdkwrap.cpp)
ENDIF()

# Build the Zarr dispatch layer as a library that will be included in
# the netCDF library.
add_library(nczarr OBJECT ${libnczarr_SOURCES})

# Remember to package this file for CMake builds.
ADD_EXTRA_DIST(${libnczarr_SOURCES} CMakeLists.txt)

