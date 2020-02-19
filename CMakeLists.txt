is_enabled(USE_SZIP HAS_SZLIB)
is_enabled(USE_SZIP HAS_SZLIB_WRITE)
is_enabled(STATUS_PNETCDF HAS_PNETCDF)
is_enabled(STATUS_PARALLEL HAS_PARALLEL)
is_enabled(ENABLE_PARALLEL4 HAS_PARALLEL4)
is_enabled(ENABLE_DAP HAS_DAP)
is_enabled(ENABLE_DAP HAS_DAP2)
is_enabled(ENABLE_DAP4 HAS_DAP4)
is_enabled(ENABLE_BYTERANGE HAS_BYTERANGE)
is_enabled(ENABLE_DISKLESS HAS_DISKLESS)
is_enabled(USE_MMAP HAS_MMAP)
is_enabled(JNA HAS_JNA)
is_enabled(ENABLE_ZERO_LENGTH_COORD_BOUND RELAX_COORD_BOUND)
is_enabled(USE_CDF5 HAS_CDF5)
is_enabled(ENABLE_ERANGE_FILL HAS_ERANGE_FILL)
is_enabled(HDF5_HAS_PAR_FILTERS HAS_PAR_FILTERS)
is_enabled(HAVE_H5Z_SZIP HAS_SZLIB)
is_enabled(ENABLE_NCZARR HAS_NCZARR)

# Generate file from template.
CONFIGURE_FILE("${CMAKE_CURRENT_SOURCE_DIR}/libnetcdf.settings.in"
  "${CMAKE_CURRENT_BINARY_DIR}/libnetcdf.settings"
  @ONLY)

# Read in settings file, print out.
# Avoid using system-specific calls so that this
# might also work on Windows.
FILE(READ "${CMAKE_CURRENT_BINARY_DIR}/libnetcdf.settings"
  LIBNETCDF_SETTINGS)
MESSAGE(${LIBNETCDF_SETTINGS})

# Install libnetcdf.settings file into same location
# as the libraries.
INSTALL(FILES "${netCDF_BINARY_DIR}/libnetcdf.settings"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  COMPONENT libraries)

#####
# End libnetcdf.settings section.
#####

#####
# Create 'netcdf_meta.h' include file.
#####
configure_file(
  ${netCDF_SOURCE_DIR}/include/netcdf_meta.h.in
  ${netCDF_SOURCE_DIR}/include/netcdf_meta.h @ONLY)

FILE(COPY ${netCDF_SOURCE_DIR}/include/netcdf_meta.h
  DESTINATION ${netCDF_BINARY_DIR}/include/)

#####
# Build test_common.sh
#####
SET(EXTRA_DIST ${EXTRA_DIST} ${CMAKE_CURRENT_SOURCE_DIR}/test_common.in)
SET(TOPSRCDIR "${CMAKE_CURRENT_SOURCE_DIR}")
SET(TOPBUILDDIR "${CMAKE_CURRENT_BINARY_DIR}")
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/test_common.in ${CMAKE_CURRENT_BINARY_DIR}/test_common.sh @ONLY NEWLINE_STYLE LF)


#####
# Build nc_test4/findplugin.sh
#####
SET(ISCMAKE "1")
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/nc_test4/findplugin.in ${CMAKE_CURRENT_BINARY_DIR}/nc_test4/findplugin.sh @ONLY NEWLINE_STYLE LF)

IF(ENABLE_EXAMPLES)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/nc_test4/findplugin.in ${CMAKE_CURRENT_BINARY_DIR}/examples/C/findplugin.sh @ONLY NEWLINE_STYLE LF)
ENDIF()

#####
# Build ncdap_test|dap4_test/findtestserver[4].c
#####
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/ncdap_test/findtestserver.c.in ${CMAKE_CURRENT_SOURCE_DIR}/ncdap_test/findtestserver.c @ONLY NEWLINE_STYLE LF)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/ncdap_test/findtestserver.c.in ${CMAKE_CURRENT_SOURCE_DIR}/dap4_test/findtestserver4.c @ONLY NEWLINE_STYLE LF)

#####
# Build dap4_test/pingurl4.c
#####
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/ncdap_test/pingurl.c ${CMAKE_CURRENT_SOURCE_DIR}/dap4_test/pingurl4.c @ONLY NEWLINE_STYLE LF)

####
# Export files
####

# Create CMake package configuration files. With these, other packages using
# cmake should be able to find netcdf using find_package and find_library.
# The EXPORT call is paired with one in liblib.
set(ConfigPackageLocation ${CMAKE_INSTALL_LIBDIR}/cmake/netCDF)

install(EXPORT netCDFTargets
  DESTINATION ${ConfigPackageLocation}
  COMPONENT headers
  NAMESPACE netCDF::
  )

include(CMakePackageConfigHelpers)
CONFIGURE_PACKAGE_CONFIG_FILE(
  "${CMAKE_CURRENT_SOURCE_DIR}/netCDFConfig.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/netCDFConfig.cmake"
  INSTALL_DESTINATION "${ConfigPackageLocation}"
  NO_CHECK_REQUIRED_COMPONENTS_MACRO
  PATH_VARS
  CMAKE_INSTALL_PREFIX
  CMAKE_INSTALL_INCLUDEDIR
  CMAKE_INSTALL_LIBDIR
  )

INSTALL(
  FILES "${CMAKE_CURRENT_BINARY_DIR}/netCDFConfig.cmake"
  DESTINATION "${ConfigPackageLocation}"
  COMPONENT headers
  )

add_library(netCDF::netcdf ALIAS netcdf)
target_include_directories(netcdf
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

# Create export configuration
write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/netCDF/netCDFConfigVersion.cmake"
  VERSION ${netCDF_VERSION}
  COMPATIBILITY SameMajorVersion
  )

install(
  FILES
  "${CMAKE_CURRENT_BINARY_DIR}/netCDF/netCDFConfigVersion.cmake"
  DESTINATION ${ConfigPackageLocation}
  COMPONENT headers
  )

####
# End export files
####



# CPack inclusion must come last.
# INCLUDE(CPack)
INCLUDE(CMakeInstallation.cmake)
