#!/bin/sh

# This test is intended to test various XARRAY support features.

# Test 1: XARRAY group creation:
# We have changed where XARRAY dimensions are created, so test this out.
# As before, unnamed dimensions are created as anonymous dimensions in root group.
# Now, named dimensions from XARRAY are created in the group immediately containing
# a variable instead of in the root group. This still leaves room for failure if multiple
# variables in the group map dimname -> shape inconsistently.
# Note we test only for file type because this feature is supported outside of zmaps.

if test "x$srcdir" = x ; then srcdir=`pwd`; fi 
. ../test_common.sh

. "$srcdir/test_nczarr.sh"

set -x
set -e

s3isolate "testdir_xarray"
THISDIR=`pwd`
cd $ISOPATH

rm -f tmp_xarraygroup.cdl
cat > tmp_xarraygroup.cdl <<EOF
netcdf xarraygroup {
dimensions: x = 2; y = 3; _Anonymous_Dim_3 = 3;
variables:
	double foo(x, y) ;
	int64 x(x) ;
	int64 y(y) ;
data:
 foo = 0.882122410001935, 0.965447248726893, 0.101624561750357, 0.456570884905498, 0.545247654537869, 0.497764826098154;
 x = 10, 20;
 y = 0, 1, 2;
group: g {
  dimensions: y = 4;
  variables: ubyte z(y);
  data: z = 97, 0, 0, 0;
} // group g
} // arraygroup
EOF

testdimlocation() {
  zext=file
  rm -fr tmp_xarraygroup.${zext}
  fileargs "tmp_xarraygroup" "mode=xarray,${zext}"
  ${NCGEN} -4 -lb -o ${fileurl} tmp_xarraygroup.cdl
  ${NCDUMP} $flags $fileurl > tmp_noshape1_${zext}.cdl
  # Cleanup
  #rm -fr tmp_xarraygroup.file
  #rm -fr tmp_xarraygroup.cdl
}

testanonlocation() {
  zext=file
  rm -fr tmp_xarraygroup.${zext}
  fileargs "tmp_xarraygroup" "mode=xarray,${zext}"
  ${NCGEN} -4 -lb -o ${fileurl} tmp_xarraygroup.cdl
  ${NCDUMP} $flags $fileurl > tmp_base_${zext}.cdl
  # Cleanup
  #rm -fr tmp_xarraygroup.file
  #rm -fr tmp_xarraygroup.cdl
}

testdimlocation
#testanonlocation

exit

