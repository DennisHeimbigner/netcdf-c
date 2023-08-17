#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi 
. ../test_common.sh

# This shell script tests ncdump and ncgen on netCDF-4 variables with multiple 
# unlimited dimensions.

set -e
if test "x$TESTNCZARR" = x1 ; then
. "$srcdir/test_nczarr.sh"
s3isolate "testdir_mud4"
THISDIR=`pwd`
cd $ISOPATH
fi

echo ""
echo "*** Testing ncdump output for multiple unlimited dimensions"

echo "*** creating netcdf file $file from ref_tst_mud4.cdl ..."

if test "x$TESTNCZARR" = x1 ; then
fileargs "tmp_mud4_${zfilt}"
deletemap $zext $file
file="$fileurl"
else
file="tmp_mud4_${zfilt}.nc"
rm -f $file
fi

${NCGEN} -4 -b -o $file $srcdir/ref_tst_mud4.cdl
echo "*** creating tmp_mud4.cdl from $file ..."
${NCDUMP} $file > tmp_mud4.cdl
# echo "*** comparing tst_mud4.cdl with ref_tst_mud4.cdl..."
diff -b tmp_mud4.cdl $srcdir/ref_tst_mud4.cdl
# echo "*** comparing annotation from ncdump -bc $file with expected output..."
${NCDUMP} -bc $file > tmp_mud4-bc.cdl
diff -b tmp_mud4-bc.cdl $srcdir/ref_tst_mud4-bc.cdl

# Now test with char arrays instead of ints
if test "x$TESTNCZARR" = x1 ; then
fileargs "tmp_mud4_chars${zfilt}"
deletemap $zext $file
file="$fileurl"
else
file="tmp_mud4_chars${zfilt}.nc"
rm -f $file
fi
echo "*** creating netcdf file $file from ref_tst_mud4_chars.cdl ..."
${NCGEN} -4 -b -o $file $srcdir/ref_tst_mud4_chars.cdl
echo "*** creating ${file}.cdl from $file ..."
${NCDUMP} $file > tmp_mud4_chars.cdl
# echo "*** comparing tmp_mud4_chars.cdl with ref_tst_mud4_chars.cdl..."
diff -b tmp_mud4_chars.cdl $srcdir/ref_tst_mud4_chars.cdl
gexit 0
# unused
# echo "*** comparing annotation from ncdump -bc tst_mud4_chars.nc with expected output..."
${NCDUMP} -bc $file > tmp_mud4_chars-bc.cdl
# diff -b tmp_mud4_chars-bc.cdl $srcdir/ref_tst_mud4_chars-bc.cdl
echo "*** All ncdump test output for multiple unlimited dimensions passed!"
exit 0
