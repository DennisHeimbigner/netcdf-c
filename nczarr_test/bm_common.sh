#!/bin/sh
# This is a metadata performance test for nczarr
# Dennis Heimbigner

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

set -e

#chunkclean src dst
cleanprops() {
    rm -f ./$2
    cat ./$1 | sed -e '/:_NCProperties/d' | sed -e '/:_SuperblockVersion/d' > ./$2
}

reclaim() {
rm -fr $1_nc4 $1_ncz $1_s3
rm -f $1_nc4.cdl $1_ncz.cdl $1_s3.cdl
rm -f tmpnc4 tmpncz tmps3
if test "x$FEATURE_S3TESTS" = xyes ; then
   aws s3api delete-object --endpoint-url=https://stratus.ucar.edu --bucket=unidata-netcdf-zarr-testing --key "$1.ncz"
fi
}

bmtest() {
PROG=$1
shift
ARGS=$@

reclaim ${PROG}
echo "${PROG}: nc4" 
${execdir}/${PROG} $ARGS --format=nc4 --X=1 --f=${PROG}
#reclaim ${PROG}
echo "${PROG}: ncz" 
${execdir}/${PROG} $ARGS --format=ncz --X=1 --f=${PROG}
#reclaim ${PROG}
if test "x$FEATURE_S3TESTS" = xyes ; then
echo "${PROG}: s3" 
${execdir}/${PROG} $ARGS --format=s3 --X=1 --f=${PROG}
#reclaim ${PROG}
fi
}

