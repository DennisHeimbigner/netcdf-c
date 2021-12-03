#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

set -e

echo "@@@@@"
find . -name '*.nc'
${execdir}/nctest
