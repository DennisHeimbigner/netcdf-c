#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

set -e

export NETCDF_LOG_LEVEL=10
if ${execdir}/nctest |tee nctest.log ; then
ret=0
else
ret=1
fi
unset NETCDF_LOG_LEVEL
find . -name '*.nc'
${execdir}/ncdump/ncdump nctest_netcdf4_classic.nc
exit $ret
