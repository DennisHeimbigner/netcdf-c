#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi 
. ../test_common.sh

. "${srcdir}/test_nczarr.sh"

s3isolate "testdir_nczarr_fill"
THISDIR=`pwd`
cd $ISOPATH

set -x
set -e

echo "*** Test: Github issues #2063, #2062, #2059"

# Note: for some of these tests, the input is actually in V2 format
# so it is necessary to invoke nccopy to convert to V3 format.

testcase2059() {
zext=$1
echo "*** Test: Github issue #2059"
fileargs tmp_groups_regular "mode=zarr,$zext"
deletemap $zext $file
${NCCOPY} ${srcdir}/ref_groups.h5 "$fileurl"
rm -f tmp_nczfill.cdl
${ZMD} -h "$fileurl"
${NCDUMP} -s -n tmp_groups_regular "$fileurl" > tmp_groups_regular_$zext.cdl
sclean tmp_groups_regular_$zext.cdl
diff -wb ${srcdir}/ref_groups_regular.cdl tmp_groups_regular_$zext.cdl
}

testcase2062() {
zext=$1
echo "*** Test: Github issue #2062"
rm -fr tmp_ref_byte.zarr
unzip ref_byte.zarr.zip >> tmp_ignore.txt
# Convert to V3
${NCCOPY} -Xf "file://ref_byte.zarr#mode=zarr,$zext" "file://tmp_ref_byte.zarr#mode=zarr,$zext"
${ZMD} -h "file://tmp_ref_byte.zarr#mode=zarr,$zext"
${NCDUMP} -s -n ref_byte "file://tmp_ref_byte.zarr#mode=zarr,$zext" > tmp_byte_$zext.cdl
sclean tmp_byte_$zext.cdl
diff -wb ${srcdir}/ref_byte.cdl tmp_byte_$zext.cdl
rm -fr tmp_ref_byte.zarr
}

testcase2063() {
zext=$1
echo "*** Test: Github issue #2063"
rm -fr ref_byte_fill_value_null.zarr
unzip ref_byte_fill_value_null.zarr.zip >> tmp_ignore.txt
rm -fr tmp_nczfill.cdl
# Convert to V3
${NCCOPY} "file://ref_byte_fill_value_null.zarr#mode=zarr,$zext" "file://tmp_ref_byte_fill_value_null.zarr#mode=zarr,$zext"
${ZMD} -h "file://tmp_ref_byte_fill_value_null.zarr#mode=zarr,$zext"
${NCDUMP} -s -n ref_byte_fill_value_null "file://tmp_ref_byte_fill_value_null.zarr#mode=zarr,$zext" > tmp_byte_fill_value_null_$zext.cdl
sclean tmp_byte_fill_value_null_$zext.cdl
diff -wb ${abs_srcdir}/ref_byte_fill_value_null.cdl tmp_byte_fill_value_null_$zext.cdl
#rm -fr tmp_nczfill.cdl tmp_ref_byte_fill_value_null.zarr
}

if ! test -f ${ISOPATH}/ref_byte.zarr.zip ; then
  cp -f ${srcdir}/ref_byte.zarr.zip ${ISOPATH}/ref_byte.zarr.zip
  cp -f ${srcdir}/ref_byte_fill_value_null.zarr.zip ${ISOPATH}/ref_byte_fill_value_null.zarr.zip
fi

testcase2062 file
testcase2063 file
if test "x$FEATURE_HDF5" = xyes ; then
  testcase2059 file
  if test "x$FEATURE_NCZARR_ZIP" = xyes ; then
    testcase2059 zip
  fi
  if test "x$FEATURE_S3TESTS" = xyes ; then
    testcase2059 s3
  fi
fi