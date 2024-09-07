#!/bin/bash

# Test the programmatic API for manipulating the plugin paths.
# WARNING: This file is also used to build nczarr_test/run_pluginpath.sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

set -e

IMPLS="hdf5 nczarr"

#VERBOSE=1

DFALT="/zero;/one;/two;/three;/four"
DFALTHDF5="/zero;/one;/two;/hdf5;/three;/four"
DFALTNCZARR="/zero;/one;/two;/nczarr;three;/four;/five"

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
	hdf5) mod="${dfalt};/modhdf5" ;;
	nczarr) mod="/modnczarr;${dfalt}" ;;
	all) mode="${dfalt}" ;;
    esac
}

#####

testread() {
    local formatx="$1"
    filenamefor tmp read
    dfaltfor $formatx
    case ${formatx} in
	all) echo "testread(${formatx}): " >> ${filename}.txt
	     for f in $IMPLS ; do testread $f ; done
	     ;;
	hdf5|nczarr) echon "testread(${formatx}): " >> ${filename}.txt
		    ${TP} -x "formatx:${formatx},write:${dfalt},read" >> ${filename}.txt ;;
	*) ;; # ignore all other cases
    esac
}                           

testwrite() {
    local formatx="$1"
    filenamefor tmp write
    dfaltfor $formatx
    modfor $formatx "$dfalt"
    case "${formatx}" in
	all)
	    echo "testwrite(${formatx}): " >> ${filename}.txt
	    for f in $IMPLS; do testwrite $f; done
	    ;;
        hdf5|nczarr)
	    echon "testwrite(${formatx}): before: " >> ${filename}.txt
		${TP} -x "formatx:${formatx},write:${dfalt},read"  >> ${filename}.txt
	    echon "testwrite(${formatx}): after: " >> ${filename}.txt
		${TP} -x "formatx:${formatx},write:${mod},read"  >> ${filename}.txt
	    ;;
	*) ;; # ignore all other cases
    esac
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
    for action in read write ; do
        if diff -wBb ref_${action}.txt tmp_${action}.txt ; then
	    echo "***PASS: $action"
	else
	    echo "***FAIL: $action"
        fi
    done
}

init
for fx in hdf5 nczarr all ; do
    testread  $fx
    testwrite $fx
done
verify
cleanup
