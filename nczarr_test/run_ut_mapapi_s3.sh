#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

tests3=1

. "$srcdir/run_ut_mapapi.sh"

main() {
echo ""
echo "*** Map Unit Testing"
echo ""; echo "*** Test zmap_s3sdk"
testmapcreate s3; testmapmeta s3; testmapdata s3; testmapsearch s3
}

if test "x$tests3" = x1 ; then main ; fi

exit 0
