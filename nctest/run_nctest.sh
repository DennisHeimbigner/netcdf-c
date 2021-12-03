#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

set -e

find . -name nctest_netcdf4_classic.nc
${execdir}/nctest
