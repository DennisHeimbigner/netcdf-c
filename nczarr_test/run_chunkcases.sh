#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

. ${srcdir}/test_nczarr.sh

set -e
set -x

TC="${execdir}/tst_chunkcases -4"
ZM="${execdir}/zmapio -t int"

remfile() {
  case "$zext" in
  nc4) rm -fr $1 ;;
  nz4) rm -fr $1 ;;
  nzf) rm -fr $1 ;;
  s3) ;;
  *) echo "no such extension: $zext" ; exit 1;;
  esac
}

makefile() {
  fileargs $1
  remfile $file
  case "$zext" in
  nc4) F=$file;;
  nz4) F=$fileurl;;
  nzf) F=$fileurl;;
  s3) F=$fileurl;;
  *) echo "no such extension: $zext" ; exit 1;;
  esac
}

reset() {
rm -f tmp_ndims_${zext}.txt tmp_ndims_${zext}.dmp tmp_ndims_${zext}.cdl
rm -f tmp_ndims_${zext}.txt tmp_ndims_${zext}.dmp tmp_ndims_${zext}.cdl
rm -f tmp_misc1_${zext}.txt tmp_misc1_${zext}.dmp tmp_misc1_${zext}.cdl
rm -f tmp_avail1_${zext}.txt tmp_avail1_${zext}.dmp tmp_avail1_${zext}.cdl
}

runtests() {

echo ""; echo "*** Test format $1"

echo "Test rank > 2"
makefile tmp_ndims
$TC -d 8,8,8,8 -c 3,3,4,4 -Ow $F
find $file
find $file -exec ls -lda '{}' \;
${execdir}/ncdumpchunks -v v $F > tmp_ndims_${zext}.dmp
diff -b ${srcdir}/ref_ndims.dmp tmp_ndims_${zext}.dmp
#${NCDUMP} $F > tmp_ndims_${zext}.cdl
#diff -b ${srcdir}/ref_ndims.cdl tmp_ndims_${zext}.cdl
#remfile tmp_ndims

echo "Test miscellaneous 1"
makefile tmp_misc1
$TC -d 6,12,4 -c 2,3,1 -f 0,0,0 -e 6,1,4 -Ow $F
find $file
find $file -exec ls -lda '{}' \;
${execdir}/ncdumpchunks -v v $F > tmp_misc1_${zext}.dmp
diff -b ${srcdir}/ref_misc1.dmp tmp_misc1_${zext}.dmp
#${NCDUMP} $F > tmp_misc1_${zext}.cdl
#diff -b ${srcdir}/ref_misc1.cdl tmp_misc1_${zext}.cdl
#remfile tmp_misc1

echo "Test writing avail > 0"
makefile tmp_avail1
$TC -d 6,12,100 -c 2,3,50 -f 0,0,0 -p 6,12,100 -Ow $F
$TC -f 0,0,0 -e 6,3,75 -Or $F > tmp_avail1_${zext}.txt
diff -b ${srcdir}/ref_avail1.txt tmp_avail1_${zext}.txt
find $file
find $file -exec ls -lda '{}' \;
${NCDUMP} $F > tmp_avail1_${zext}.cdl
diff -b ${srcdir}/ref_avail1.cdl tmp_avail1_${zext}.cdl
#remfile tmp_avail1
}

testcase() {
zext=$1
reset
export NCTRACING=10
runtests
export NCTRACING="-1"
reset
}


testcase nzf
#if test "x$FEATURE_HDF5" = xyes ; then testcases nz4; fi
#if test "x$FEATURE_S3TESTS" = xyes ; then testcases s3; fi
