#!/bin/sh
#
# Test to make sure ncdump works with a subdirectory which starts
# with a unicode character.
# See https://github.com/Unidata/netcdf-c/issues/1666 for more information.
# Ward Fisher

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

ERR() {
    RES=$?
    if [ $RES -ne 0 ]; then
        echo "Error found: $RES"
        exit $RES
    fi
}

#UNIDIR="\xe6\xb5\xb7"
UNIDIR='y海'
UNIFILE='tst_utf_海.nc'

echo ""
echo "Creating Unicode String Directory ${UNIDIR}"
rm -fr "${UNIDIR}"
mkdir -p "${UNIDIR}"
ls -ld "${UNIDIR}"

echo "*** Generating binary file ${UNIDIR}/${UNIFILE}..."
${NCGEN} -b -o "${UNIDIR}/${UNIFILE}" "${srcdir}/ref_tst_utf8.cdl"; ERR
echo "*** Accessing binary file ${UNIDIR}/${UNIFILE}..."
${NCDUMP} -h "${UNIDIR}/${UNIFILE}"; ERR

echo "Test Passed. Cleaning up."
rm -fr "${UNIDIR}"; ERR

