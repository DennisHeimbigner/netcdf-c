#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

set -x
set -e

# Test the glob matching properties of .rc files
# The internal .rc is constructed as follows:
# Use ${CWD}/.ncrc as the .rc file

# Create a special isolationdirectory
isolate "testdir_rcglob"
THISDIR=`pwd`
# And enter it to execute tests
cd $ISOPATH

failures=0

TESTS="[http://github.com/a/b/c]ncrc=ncrc1"
TESTS="$TESTS [http://github.com:8080/a/b/c]key0=v0"
TESTS="$TESTS [http://github.com]key0=v1"

# Test that no-url (key,value) still works
test0() {
    nm=test0
    rm -fr .ncrc
    # Build the .ncrc file
    GLOB="*://**/**"
    echo "[${GLOB}]key=valuea" >> .ncrc
    echo "key=valueb" >> .ncrc
    VALUE=`${execdir}/tst_rcglob "key"`
    if test "x$VALUE" = "xvalueb"; then echo "***PASS: ${nm}" ; else echo "***FAIL: ${nm}"; failures=1; fi
}

# Test that non-glob url (key,value) still works
test1() {
    nm=test1
    rm -fr .ncrc
    # Build the .ncrc file
    GLOB='https://a.b.c:1024/x.y.z'
    echo "[${GLOB}]key=valuea" >> .ncrc
    echo "key=valueb" >> .ncrc
    VALUE=`${execdir}/tst_rcglob "key" "${GLOB}"`
    if test "x$VALUE" = "xvaluea"; then echo "***PASS: ${nm}" ; else echo "***FAIL: ${nm}"; failures=1; fi
}

# Test that general glob url (key,value) works
test2() {
    nm=test2
    rm -fr .ncrc
    # Build the .ncrc file
    GLOB='*://**/**'
    URL='http://a.b.c/x.y.z'
    echo "[${GLOB}]key=valuea" >> .ncrc
    echo "key=valueb" >> .ncrc
    VALUE=`${execdir}/tst_rcglob "key" "${URL}"`
    if test "x$VALUE" = "xvaluea"; then echo "***PASS: ${nm}" ; else echo "***FAIL: ${nm}"; failures=1; fi
}

test0
test1
test2

if test "x$failures" = x0 ; then
    echo "***PASS all tests"
    exit
else
    echo "***FAIL some tests"
    exit 1
fi
