#!/bin/bash

# Test the programmatic API for manipulating the plugin paths.
# This script is still full of cruft that needs to be removed

export SETX=1
set -x

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

IMPLS=
if test "x$FEATURE_HDF5" = xyes ; then IMPLS="$IMPLS hdf5"; fi
if test "x$FEATURE_NCZARR" = xyes ; then IMPLS="$IMPLS nczarr"; fi
# Remove leading blank
IMPLS=`echo "$IMPLS" | cut -d' ' -f2,3`
echo "IMPLS=|$IMPLS|"

#VERBOSE=1

# Watch out because bash will convert '/' to '\\' on windows
DFALT="\/zero;\/one;\/two;\/three;\/four"
DFALTSET="\/zero;\/one;\/mod;\/two;\/three;\/four"
DFALTHDF5="\/zero;\/one;\/two;\/hdf5;\/three;\/four"
DFALTNCZARR="\/zero;\/one;\/two;\/nczarr;three;\/four;\/five"

if test "x$TESTNCZARR" = x1 ; then
. "$srcdir/test_nczarr.sh"
s3isolate "testdir_pluginpath"
THISDIR=`pwd`
cd $ISOPATH
fi

TP="${execdir}/tst_pluginpaths"

filenamefor() {
  # tmp|ref_action
  eval "filename=${1}_$2"
}

dfaltfor() {
    case "$1" in
	hdf5) eval "dfalt=\"$DFALTHDF5\"" ;;
	nczarr) eval "dfalt=\"$DFALTNCZARR\"" ;;
	all) eval "dfalt=\"$DFALT\"" ;;
    esac
}

modfor() {
    local formatx="$1"
    local dfalt="$2"
    case "$formatx" in
	hdf5) mod="${dfalt};\/modhdf5" ;;
	nczarr) mod="\/modnczarr;${dfalt}" ;;
	all) mode="${dfalt}" ;;
    esac
}

#####

# Test that global state is same as that for HDF5 and NCZarr.
# It is difficult to test for outside interference, so not attempted.
testget() {
    filenamefor tmp get
    # Accumulate the output to avoid use of echo
    TMPGET=
    # print out the global state
    TMPGET="testget(global): "
    TMP=`${TP} -x "set:${DFALT},get:global"`
    TMPGET="${TMPGET}${TMP}"
    # print out the HDF5 state
    TMPGET="${TMPGET}testget(hdf5): "
    TMP=`${TP} -x "set:${DFALT},get:hdf5"`
    # print out the NCZarr state
    TMPGET="${TMPGET}testget(nczarr): "
    TMP=`${TP} -x "set:${DFALT},get:nczarr"`
    TMPGET="${TMPGET}${TMP}"
    echo "$TMPGET" | tr -d '\r' | cat >> ${filename}.txt
}

# Set the global state to some value and verify that it was sync'd to hdf5 and nczarr
testset() {
    filenamefor tmp set
    # print out the global state, modify it and print again
    TMPSET=
    TMPSET="testset(global): before: "
    TMP=`${TP} -x "set:${DFALT},get:global"`
    TMPSET="${TMPSET}testset(global): after: "
    TMP=`${TP} -x "set:${DFALT},set:${DFALTSET},get:global"`
    TMPSET="${TMPSET}${TMP}"
    # print out the HDF5 state
    TMPSET="${TMPSET}testset(hdf5): before: "
    TMP=`${TP} -x "set:${DFALT},get:hdf5"`
    TMPSET="${TMPSET}${TMP}"
    TMPSET="${TMPSET}testset(hdf5): after: "
    TMP=`${TP} -x "set:${DFALT},set:${DFALTSET},get:hdf5"`
    TMPSET="${TMPSET}${TMP}"
    # print out the NCZarr state
    TMPSET="${TMPSET}testset(nczarr): before: "
    TMP=`${TP} -x "set:${DFALT},get:nczarr"`
    TMPSET="${TMPSET}testset(nczarr): after: "
    TMP=`${TP} -x "set:${DFALT},set:${DFALTSET},get:nczarr"`
    TMPSET="${TMPSET}${TMP}"
    echo "$TMPGET" | tr -d '\r' | cat >> ${filename}.txt
}                           

#########################

cleanup() {
    rm -f tmp_*.txt
}

init() {
    cleanup
}

# Verify output for a specific action
verify() {
#    for action in get set ; do
for action in get ; do
        if diff -wBb ${srcdir}/ref_${action}.txt tmp_${action}.txt ; then
	    echo "***PASS: $action"
	else
	    echo "***FAIL: $action"
	    exit 1
        fi
    done
}

init
testget
testset
verify
cleanup
