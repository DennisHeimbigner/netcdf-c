#!/bin/sh
# This is a metadata performance test for nczarr
# Dennis Heimbigner

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

. ./bm_common.sh

echo "Testing performance of nc_create and nc_open on file with large metadata"

ARGS="--ngroups=100 --ngroupattrs=100"

bmtest bm_many_atts $ARGS

#reclaim
