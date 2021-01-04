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

testcases() {

zext=$1
echo ""; echo "*** Test format $1"

echo "Test miscellaneous 1"
makefile tmp_misc1
rm -f tmp_misc1_${zext}.txt tmp_misc1_${zext}.dmp tmp_misc1_${zext}.cdl
$TC -d 6,12,4 -c 2,3,1 -f 0,0,0 -e 6,1,4 -Ow $F
${NCDUMP} $F > tmp_misc1_${zext}.cdl
diff -b ${srcdir}/ref_misc1.cdl tmp_misc1_${zext}.cdl
${execdir}/ncdumpchunks -v v $F > tmp_misc1_${zext}.dmp
diff -b ${srcdir}/ref_misc1.dmp tmp_misc1_${zext}.dmp
fi

echo "Test writing avail > 0"
makefile tmp_avail1
rm -f tmp_avail1_${zext}.txt tmp_avail1_${zext}.dmp tmp_avail1_${zext}.cdl
$TC -d 6,12,100 -c 2,3,50 -f 0,0,0 -p 6,12,100 -Ow $F
#$ZM $F
$TC -T4 -f 0,0,0 -e 6,3,75 -Or $F > tmp_avail1_${zext}.txt
diff -b ${srcdir}/ref_avail1.txt tmp_avail1_${zext}.txt
${NCDUMP} $F > tmp_avail1_${zext}.cdl
ls -l tmp_avail1_${zext}.cdl

} # testcases()

testcases nzf
#if test "x$FEATURE_HDF5" = xyes ; then testcases nz4; fi
#if test "x$FEATURE_S3TESTS" = xyes ; then testcases s3; fi
