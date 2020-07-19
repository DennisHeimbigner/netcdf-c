#!/bin/sh
#
# Test to make sure ncdump works with a subdirectory which starts
# with a unicode character.
# See https://github.com/Unidata/netcdf-c/issues/1666 for more information.
# Ward Fisher

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

set -e

UNIDIR="海"
UNIFILE="tst_utf8_${UNIDIR}.nc"

echo ""
echo "Creating Unicode String Directory ${UNISTRING}"
rm -fr ${UNIDIR}
mkdir -p ${UNIDIR}
ls -ld ${UNIDIR}

echo "*** Generating binary file ${UNIDIR}/$UNIFILE..."
${NCGEN} -b -o "${UNIDIR}/${UNIFILE}" "${srcdir}/ref_tst_utf8.cdl"
echo "*** Accessing binary file ${UNIDIR}/${UNIFILE}..."
${NCDUMP} -h "${UNIDIR}/${UNIFILE}"

echo "Test Passed. Cleaning up."
rm "${UNIDIR}/${UNIFILE}"
rmdir "${UNIDIR}"


