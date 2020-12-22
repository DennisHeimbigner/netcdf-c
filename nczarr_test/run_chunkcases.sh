#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

. ${srcdir}/test_nczarr.sh

set -e

TC="${execdir}/tst_chunkcases -4"

makefile() {
  fileargs $1
  case "$zext" in
  nc4) F=$file;;
  nz4) F=$fileurl;;
  nzf) F=$fileurl;;
  s3) F=$fileurl;;
  *) echo "no such extension: $zext" ; exit 1;;
  esac
}

testcases() {

zext=$1
echo ""; echo "*** Test format $1"
    
# Test whole chunk write and read
echo "Test whole chunk write then read"
makefile tmp_whole
rm -f tmp_whole.txt tmp_whole.cdl tmp_err.txt
# This should fail 
if ! $TC -d 8,8 -c 4,4 -f 4,3 -e 4,4 -X w -OWw $F >> tmp_err.txt ; then
echo "XFAIL: wholechunk with bad -f"
fi
if ! $TC -d 8,8 -c 4,4 -f 4,4 -e 1,4 -X w -OWw $F  >> tmp_err.txt ; then
echo "XFAIL: wholechunk with bad -e"
fi
# This should succeed
$TC -d 8,8 -c 4,4 -f 4,4 -e 4,4 -X w -OWw $F
$TC -d 8,8 -c 4,4 -f 4,4 -e 4,4 -X w -OWr $F > tmp_whole.txt
diff -b ${srcdir}/ref_whole.txt tmp_whole.txt
${NCDUMP} $F > tmp_whole.cdl
diff -b ${srcdir}/ref_whole.cdl tmp_whole.cdl

# Test skipping whole chunks
echo "Test chunk skipping during read"
makefile tmp_skip
rm -f tmp_skip.txt tmp_skip.cdl tmp_skipw.cdl
$TC -d 6,6 -c 2,2 -Ow $F
$TC -s 5,5 -p 6,6 -Or $F > tmp_skip.txt
${NCDUMP} $F > tmp_skip.cdl
diff -b ${srcdir}/ref_skip.txt tmp_skip.txt
diff -b ${srcdir}/ref_skip.cdl tmp_skip.cdl

echo "Test chunk skipping during write"
makefile tmp_skipw
rm -f tmp_skipw.cdl
$TC -d 6,6 -s 5,5 -p 6,6 -Ow $F
${NCDUMP} $F > tmp_skipw.cdl
diff -b ${srcdir}/ref_skipw.cdl tmp_skipw.cdl

echo "Test dimlen % chunklen != 0"
makefile tmp_rem
rm -f tmp_rem.txt tmp_rem.cdl
$TC -d 8,8 -c 3,3 -Ow $F
${NCDUMP} $F > tmp_rem.cdl
diff -b ${srcdir}/ref_rem.cdl tmp_rem.cdl
${execdir}/ncdumpchunks -v v $F > tmp_rem.txt
diff -b ${srcdir}/ref_rem.txt tmp_rem.txt

echo "Test rank > 2"
makefile tmp_ndims
rm -f tmp_ndims.txt tmp_ndims.cdl
$TC -d 8,8,8,8 -c 3,3,4,4 -Ow $F
${NCDUMP} $F > tmp_ndims.cdl
diff -b ${srcdir}/ref_ndims.cdl tmp_ndims.cdl
${execdir}/ncdumpchunks -v v $F > tmp_ndims.txt
diff -b ${srcdir}/ref_ndims.txt tmp_ndims.txt

echo "Test miscellaneous 1"
makefile tmp_misc1
rm -f tmp_misc1.txt tmp_misc1.cdl
$TC -d 6,12,4 -c 2,3,1 -f 0,0,0 -e 6,1,4 -Ow $F
${NCDUMP} $F > tmp_misc1.cdl
diff -b ${srcdir}/ref_misc1.cdl tmp_misc1.cdl
${execdir}/ncdumpchunks -v v $F > tmp_misc1.txt
diff -b ${srcdir}/ref_misc1.txt tmp_misc1.txt
} # testcases()

testcases nzf
if test "x$FEATURE_HDF5" = xyes ; then
  testcases nz4
fi
if test "x$FEATURE_S3TESTS" = xyes ; then
  testcases s3
fi


