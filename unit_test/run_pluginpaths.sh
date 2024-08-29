#!bin/bash

# Test the programmatic API for manipulating the plugin paths.
# WARNING: This file is also used to build nczarr_test/run_pluginpath.sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

set -e

if test "x$TESTNCZARR" = x1 ; then
. "$srcdir/test_nczarr.sh"
s3isolate "testdir_pluginpath"
THISDIR=`pwd`
cd $ISOPATH
fi

# Test append/prepend
testlist() {
    ${execdir}/tst_pluginpaths -x list
}				

testset() {
    testlist
}

testset
