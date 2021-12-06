#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

set -e

export NETCDF_LOG_LEVEL=10
if ${execdir}/nctest ; then ret=0; else ret=1; fi
unset NETCDF_LOG_LEVEL
cat AAAA
cat XXXX
exit $ret
