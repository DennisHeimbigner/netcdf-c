#!/bin/sh
# This is a metadata performance test for nczarr
# Dennis Heimbigner

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

. ./bm_common.sh

echo "Testing performance of nc_create and nc_open on file with large # of variables"

CMD=bm_many_objs
ARGS="--ngroups=100 --ngroupattrs=100 --nvars=100"

FMT=nc4 ; $CMD --format=$FMT --f=$CMD.$FMT $ARGS
FMT=nzf ; $CMD --format=$FMT --f=$CMD.$FMT $ARGS

#reclaim
#rm -fr $CMD.nc4
#rm -fr $CMD.nzf
