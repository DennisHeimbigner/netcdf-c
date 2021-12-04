#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

set -e

export NETCDF_LOG_LEVEL=10
if test ${execdir}/nctest ; then ret=0; else ret=1; fi
unset NETCDF_LOG_LEVEL
exit $ret
