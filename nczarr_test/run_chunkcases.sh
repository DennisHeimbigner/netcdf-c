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
    
if test 1 = 0 ; then
# Test whole chunk write and read
echo "Test whole chunk write then read"
makefile tmp_whole
rm -f tmp_whole_${zext}.txt tmp_whole_${zext}.dmp tmp_whole_${zext}.cdl tmp_err_${zext}.txt
# This should succeed
$TC -d 8,8 -c 4,4 -f 4,4 -e 4,4 -OWw $F
$TC -d 8,8 -c 4,4 -f 4,4 -e 4,4 -OWr $F > tmp_whole_${zext}.txt
diff -b ${srcdir}/ref_whole.txt tmp_whole_${zext}.txt
${NCDUMP} $F > tmp_whole_${zext}.cdl
diff -b ${srcdir}/ref_whole.cdl tmp_whole_${zext}.cdl
# These two should fail 
remfile $file
if ! $TC -d 8,8 -c 4,4 -f 4,3 -e 4,4 -OWw $F >> tmp_err_${zext}.txt ; then
echo "XFAIL: wholechunk with bad -f"
fi
remfile $file
if ! $TC -d 8,8 -c 4,4 -f 4,4 -e 1,4 -OWw $F  >> tmp_err_${zext}.txt ; then
echo "XFAIL: wholechunk with bad -e"
fi

# Test skipping whole chunks
echo "Test chunk skipping during read"
makefile tmp_skip
rm -f tmp_skip_${zext}.txt tmp_skip_${zext}.dmp tmp_skip_${zext}.cdl
$TC -d 6,6 -c 2,2 -Ow $F
$TC -s 5,5 -p 6,6 -Or $F > tmp_skip_${zext}.txt
diff -b ${srcdir}/ref_skip.txt tmp_skip_${zext}.txt
${NCDUMP} $F > tmp_skip_${zext}.cdl
diff -b ${srcdir}/ref_skip.cdl tmp_skip_${zext}.cdl

echo "Test chunk skipping during write"
makefile tmp_skipw
rm -f tmp_skipw_${zext}.txt tmp_skipw_${zext}.dmp tmp_skipw_${zext}.cdl
$TC -d 6,6 -s 5,5 -p 6,6 -Ow $F
${NCDUMP} $F > tmp_skipw_${zext}.cdl
diff -b ${srcdir}/ref_skipw.cdl tmp_skipw_${zext}.cdl

echo "Test dimlen % chunklen != 0"
makefile tmp_rem
rm -f tmp_rem_${zext}.txt tmp_rem_${zext}.dmp tmp_rem_${zext}.cdl
$TC -d 8,8 -c 3,3 -Ow $F
${NCDUMP} $F > tmp_rem_${zext}.cdl
diff -b ${srcdir}/ref_rem.cdl tmp_rem_${zext}.cdl
${execdir}/ncdumpchunks -v v $F > tmp_rem_${zext}.dmp
diff -b ${srcdir}/ref_rem.dmp tmp_rem_${zext}.dmp

echo "Test rank > 2"
makefile tmp_ndims
rm -f tmp_ndims_${zext}.txt tmp_ndims_${zext}.dmp tmp_ndims_${zext}.cdl
$TC -d 8,8,8,8 -c 3,3,4,4 -Ow $F
${NCDUMP} $F > tmp_ndims_${zext}.cdl
diff -b ${srcdir}/ref_ndims.cdl tmp_ndims_${zext}.cdl
${execdir}/ncdumpchunks -v v $F > tmp_ndims_${zext}.dmp
diff -b ${srcdir}/ref_ndims.dmp tmp_ndims_${zext}.dmp

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
$TC -T4 -d 6,12,100 -c 2,3,50 -f 0,0,0 -p 6,12,100 -Ow $F
$ZM $F
$TC -T4 -f 0,0,0 -e 6,3,75 -Or $F > tmp_avail1_${zext}.txt
diff -b ${srcdir}/ref_avail1.txt tmp_avail1_${zext}.txt
export NCTRACING=3
${NCDUMP} $F > tmp_avail1_${zext}.cdl
export NCTRACING="-1"
ls -l tmp_avail1_${zext}.cdl
diff -b ${srcdir}/ref_avail1.cdl tmp_avail1_${zext}.cdl
${execdir}/ncdumpchunks -v v $F > tmp_avail1_${zext}.dmp
diff -b ${srcdir}/ref_avail1.dmp tmp_avail1_${zext}.dmp

} # testcases()

testcases nzf
#if test "x$FEATURE_HDF5" = xyes ; then testcases nz4; fi
#if test "x$FEATURE_S3TESTS" = xyes ; then testcases s3; fi
