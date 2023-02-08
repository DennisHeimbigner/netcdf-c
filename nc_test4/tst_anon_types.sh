#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

set -e

echo "*** Testing Anonymous types in an H5 file"

rm -f ./tmp_anon_enum.cdl
$NCDUMP ${srcdir}/ref_anon_enum.h5 >./tmp_anon_enum.cdl
diff -b -w ${srcdir}/ref_anon_enum.cdl tmp_anon_enum.cdl

# TODO: add anonymous compound and opaque type tests

rm -f tmp_anon_enum.cdl
