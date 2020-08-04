#!/bin/sh
#
# Test to make sure ncdump works with a subdirectory which starts
# with a unicode character.
# See https://github.com/Unidata/netcdf-c/issues/1666 for more information.
# Ward Fisher

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

set -e

# Passing a utf8 name using either \x or actual characters
# to Visual Studio does not work well.
if test "x$FP_ISMSVC" = x ; then
UNIFILE='tst_utf8_海.nc'
UNIFILE='tst_utf8_\xce\x9a\xce\xb1'
UNIFILE='tst_utf8_Κα'
LC_ALL="C.UTF-8"
else
UNIFILE='tst_utf8_海.nc'
UNIFILE='tst_utf8_\xe6\xb5\xb7.nc'
UNIFILE='tst_utf8_\xce\x9a\xce\xb1'
UNIFILE='tst_utf8_Κα'
LC_ALL="en_US.UTF-8"
LC_ALL="C.UTF-8"
export LC_ALL
fi

echo ""


echo "*** Generating binary file $UNIFILE..."
${NCGEN} -b -o "${UNIFILE}" "${srcdir}/ref_tst_utf8.cdl"
echo "*** Accessing binary file ${UNIFILE}..."
${NCDUMP} -h "${UNIFILE}"

if test "x$FP_ISMSVC" != x ; then
c:/Windows/System32/chcp $CP
fi

echo "Test Passed. Cleaning up."
#rm ${UNIFILE}
