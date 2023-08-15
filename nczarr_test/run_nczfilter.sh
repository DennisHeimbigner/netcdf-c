#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi 
. ../test_common.sh

. "$srcdir/test_nczarr.sh"

# This shell script runs test_nczfilter.c

set -x
set -e

s3isolate "testdir_nczfilter"
THISDIR=`pwd`
cd $ISOPATH

echo ">>>>>>"
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:D:/a/netcdf-c/netcdf-c/liblib/.libs"
${execdir}/test_nczfilter

if test "x$FEATURE_S3TESTS" = xyes ; then s3sdkdelete "/${S3ISOPATH}" ; fi # Cleanup
