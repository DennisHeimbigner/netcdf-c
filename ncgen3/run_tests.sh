#!/bin/sh
# This shell script runs the ncgen3 tests.
# $Id: run_tests.sh,v 1.9 2009/09/24 18:19:11 dmh Exp $

echo "*** Testing ncgen3."
set -e

if test "x$srcdir" = x ;then srcdir=`pwd`; fi
. ../test_common.sh

# We need to find the drive letter, if any
DL=`${NCPATHCVT} -c -e / | sed -e 's|/cygdrive/\([a-zA-Z]\)/.*|\1|'`
if test "x$DL" != x ; then
  # Lower case drive letter
  DLL=`echo "$DL" | tr '[:upper:]' '[:lower:]'`
  DL="-d $DLL"
fi
echo "MSYS_PREFIX=$MSYS_PREFIX"
echo "MINGW_PREFIX=$MINGW_PREFIX"
KIND=`${NCPATHCVT} -k`

XX=`pwd`
ls -l
${NCPATHCVT} D:/a/netcdf-c/netcdf-c/netcdf-c/ncdump/ncpathcvt.exe

echo "*** creating classic file c0.nc from c0.cdl..."
${NCGEN3} -b -o c0.nc ${ncgen3c0}
echo "*** creating 64-bit offset file c0_64.nc from c0.cdl..."
#${NCGEN3} -k 64-bit-offset -b -o c0_64.nc ${ncgen3c0}

echo "*** Test successful!"
exit 0
