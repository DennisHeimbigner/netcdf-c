#!/bin/bash

# Test the programmatic API for manipulating the plugin paths.
# WARNING: This file is also used to build nczarr_test/run_pluginpath.sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

set -e

#VERBOSE=1

export DFALT="/zero;/one;/two;/three;/four"

if test "x$TESTNCZARR" = x1 ; then
. "$srcdir/test_nczarr.sh"
s3isolate "testdir_pluginpath"
THISDIR=`pwd`
cd $ISOPATH
fi

verbose() {
  tag="$2"
  if test "x$tag" != x ; then tag="$tag: "; fi
  if test "x${VERBOSE}" != x ; then echon "test$1: $tag" ; cat tmp_$1.txt; fi
}

# Test initial
testgetall() {
    echo "testgetall: " >> tmp_getall.txt
    rm -f tmp_getall.txt
    # get the global plugin path
    echo "global only: " >> tmp_getall.txt
    echon "	all: " >> tmp_getall.txt
    ${execdir}/tst_pluginpaths -x "load:${DFALT},remove:/one,getall" >> tmp_getall.txt
    echon "	hdf5: " >> tmp_getall.txt
    ${execdir}/tst_pluginpaths -x "load:${DFALT},sync:hdf5,remove:/one,getall:hdf5" >> tmp_getall.txt
    echon "	nczarr: " >> tmp_getall.txt
    ${execdir}/tst_pluginpaths -x "load:${DFALT},sync:nczarr,remove:/one,getall:nczarr" >> tmp_getall.txt
    # get plugin paths for hdf5 only
    echo "hdf5 only: " >> tmp_getall.txt
    echon "	hdf5: " >> tmp_getall.txt
    ${execdir}/tst_pluginpaths -x "load:${DFALT},append:/usr,sync:hdf5,getall:hdf5" >> tmp_getall.txt
    echon "	nczarr: " >> tmp_getall.txt
    ${execdir}/tst_pluginpaths -x "load:${DFALT},append:/usr,sync:hdf5,getall:nczarr" >> tmp_getall.txt
    # get plugin paths for nczarr only
    echo "nczarr only: " >> tmp_getall.txt
    echon "	nczarr: " >> tmp_getall.txt
    ${execdir}/tst_pluginpaths -x "load:${DFALT},prepend:/usr,sync:nczarr,getall:nczarr" >> tmp_getall.txt
    echon "	hdf5: " >> tmp_getall.txt
    ${execdir}/tst_pluginpaths -x "load:${DFALT},prepend:/usr,sync:nczarr,getall:hdf5" >> tmp_getall.txt
    diff -wBb ref_getall.txt tmp_getall.txt
}				

# Test ith
testgetith() {
    echo "testgetith: " >> tmp_getith.txt
    rm -f tmp_getith.txt
    # Test get of initial item
    ${execdir}/tst_pluginpaths -x "load:${DFALT},getith:0" >> tmp_getith.txt
    # Test get of interior item
    ${execdir}/tst_pluginpaths -x "load:${DFALT},getith:1" >> tmp_getith.txt
    # Test get of terminal item
    ${execdir}/tst_pluginpaths -x "load:${DFALT},getith:2" >> tmp_getith.txt
    verbose getith
    diff -wBb ref_getith.txt tmp_getith.txt
}				

# Test length
testlength() {
    echo "testlength: " >> tmp_length.txt
    rm -f tmp_length.txt
    # Set plugin path to a known set of value and then print length
    ${execdir}/tst_pluginpaths -x "load:${DFALT},length" >> tmp_length.txt
    verbose length
    diff -wBb ref_length.txt tmp_length.txt
}				

# Test load
testload() {
    echo "testload: " >> tmp_load.txt
    rm -f tmp_load.txt
    ${execdir}/tst_pluginpaths -x "load:${DFALT},getall" >> tmp_load.txt
    verbose load
    diff -wBb ref_load.txt tmp_load.txt
}				

# Test remove
testremove() {
    echo "testremove: " >> tmp_remove.txt
    rm -f tmp_remove.txt
    # get length of dfalt
    len=`${execdir}/tst_pluginpaths -x "load:${DFALT},length"`
    # test removal of initial item
    item=`${execdir}/tst_pluginpaths -x "load:${DFALT},getith:0"`
    echo "item[0]=${item}" >> tmp_remove.txt
    ${execdir}/tst_pluginpaths -x "load:${DFALT},remove:${item},getall" >> tmp_remove.txt
    # test removal of interior item
    item=`${execdir}/tst_pluginpaths -x "load:${DFALT},getith:2"`
    echo "item[2]=${item}" >> tmp_remove.txt
    ${execdir}/tst_pluginpaths -x "load:${DFALT},remove:${item},getall" >> tmp_remove.txt
    # test removal of terminal item
    last=`expr $len - 1`
    item=`${execdir}/tst_pluginpaths -x "load:${DFALT},getith:${last}"`
    echo "item[${last}]=${item}" >> tmp_remove.txt
    ${execdir}/tst_pluginpaths -x "load:${DFALT},remove:${item},getall" >> tmp_remove.txt
    verbose remove
    diff -wBb ref_remove.txt tmp_remove.txt
}				

# Test sync
testsync() {
    echo "testsync: " >> tmp_sync.txt
    rm -f tmp_sync.txt
    # Verify syncing to hdf5
    echon "hdf5: " >> tmp_sync.txt
    ${execdir}/tst_pluginpaths -x "load:${DFALT},append:/usr,sync,getall:hdf5" >> tmp_sync.txt
    verbose sync hdf5
    # Verify syncing to nczarr
    echon "nczarr: " >> tmp_sync.txt
    ${execdir}/tst_pluginpaths -x "load:${DFALT},prepend:/usr,sync,getall:nczarr" >> tmp_sync.txt
    verbose sync nczarr
    diff -wBb ref_sync.txt tmp_sync.txt
}				

# Test append/prepend
testxpend() {
    echo "testxpend: " >> tmp_xpend.txt
    rm -f tmp_xpend.txt
    ${execdir}/tst_pluginpaths -x "load:${DFALT},append:/tmp,prepend:/usr,getall" >> tmp_xpend.txt
    verbose xpend
    diff -wBb ref_xpend.txt tmp_xpend.txt
}				

testset() {
    testload
    testgetall
    testlength
    testgetith
    testremove
    testxpend
    testsync
}

#testset
testgetall
