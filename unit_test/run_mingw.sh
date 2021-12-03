#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

set -e

if ${execdir}/test_mingw ; then
ret=0
else
ret=1
fi
find . -name '*.nc'
${NCDUMP} test_mingw.nc
exit $ret

