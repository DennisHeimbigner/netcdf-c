#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

set -e

find .

cmp nctest_classic.nc $srcdir/ref_nctest_classic.nc
cmp nctest_64bit_offset.nc $srcdir/ref_nctest_64bit_offset.nc

