#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

set -e

. "$srcdir/test_nczarr.sh"

s3isolate "testdir_s3env"
THISDIR=`pwd`
cd $ISOPATH

testpublicdata_ok() {
  AWS_ACCESS_KEY_ID='SHOULD NOT BE CHECKED' \
  AWS_SECRET_ACCESS_KEY='LIKEWISE' \
  AWS_PROFILE='no' \
  ${NCDUMP} -h 'https://s3.amazonaws.com/noaa-goes16/ABI-L1b-RadF/2022/001/18/OR_ABI-L1b-RadF-M6C01_G16_s20220011800205_e20220011809513_c20220011809562.nc#mode=bytes,s3,consolidated'
}

testauthfailed() {
  # Use bad keys for access MinIO playground
  AWS_ACCESS_KEY_ID="1241" \
  AWS_SECRET_ACCESS_KEY="123456789" \
  ${NCDUMP} -L10 -h \
     'https://play.min.io/bucket/unexising.zarr#mode=zarr,s3' \
  2>&1  | grep -q 'Authorization failure'
}

if test "x$FEATURE_S3" = xyes ; then
testpublicdata_ok
testauthfailed
fi
