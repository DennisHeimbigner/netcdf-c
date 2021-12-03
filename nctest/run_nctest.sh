#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

set -e

if ${execdir}/nctest |tee nctest.log ; then
ret=0
else
ret=1
fi
find . -name '*.nc'
${execdir}/ncdump/ncdump nctest_netcdf4_classic.nc
exit $ret
