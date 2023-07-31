#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

. ${srcdir}/test_nczarr.sh

set -e

s3isolate "testdir_chunkcases"
THISDIR=`pwd`
cd $ISOPATH

TC="${execdir}/test_unlim_io -4"
ZM="${execdir}/zmapio -t int"

remfile() {
  case "$zext" in
  nc4) rm -fr $1 ;;
  file) rm -fr $1 ;;
  zip) rm -fr $1 ;;
  s3) ;;
  *) echo "no such extension: $zext" ; exit 1;;
  esac
}

remfile() {
  case "$zext" in
  nc4) rm -fr $1 ;;
  file) rm -fr $1 ;;
  zip) rm -fr $1 ;;
  s3) ;;
  *) echo "no such extension: $zext" ; exit 1;;
  esac
}

makefile() {
  fileargs $1
  remfile $file
  case "$zext" in
  nc4) F=$file;;
  file) F=$fileurl;;
  zip) F=$fileurl;;
  s3) F=$fileurl;;
  *) echo "no such extension: $zext" ; exit 1;;
  esac
}

testcase() {
zext=$1
echo ""; echo "*** Test format $1"
$TC -d 0 -c 2 -s 0 -e 1 -O cw tmp_unlim_io.nc
$TC -d 0 -c 2 -s 0 -e 1 -O cw file://tmp_unlim_io.file\#mode=nczarr,file
}

testcases() {
  testcase $1
}

testcases file
if test "x$FEATURE_NCZARR_ZIP" = xyes ; then testcases zip; fi
if test "x$FEATURE_S3TESTS" = xyes ; then testcases s3; fi
if test "x$FEATURE_S3TESTS" = xyes ; then s3sdkdelete "/${S3ISOPATH}" ; fi # Cleanup

