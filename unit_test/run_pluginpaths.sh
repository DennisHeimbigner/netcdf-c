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
# construct a path with multiple copies of the same dir
PATHMULTI="/zero;/one;/two;/zero;/three;/four;/zero"

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

filenamefor() {
  # tmp|ref_action
  eval "filename=${1}_$2"
}

before() {
    local action="$1"
    local formatx="$2"
    local cmds="$3"
    local dfalt
    dfaltfor $formatx
    echon "test${action}($formatx): before: " >> ${filename}.txt
    if test "x$cmds" = x ; then cmds="formatx:${formatx},load:${dfalt},getall"; fi
    eval "${execdir}/tst_pluginpaths -x \"$cmds\""  >> ${filename}.txt
}

after() {
    local action="$1"
    local formatx="$2"
    local cmds="$3"
    echon "test${action}($formatx): after: " >> ${filename}.txt
    eval "${execdir}/tst_pluginpaths -x \"$cmds\""  >> ${filename}.txt
}

dfaltfor() {
    case "$1" in
	hdf5) eval "dfalt=\"$DFALTHDF5\"" ;;
	nczarr) eval "dfalt=\"$DFALTNCZARR\"" ;;
	all) eval "dfalt=\"$DFALT\"" ;;
    esac
}

#####
testcase() {
    local action="$1"
    local formatx="$2"
    local filename
    local dfalt
    filenamefor tmp ${action}
    dfaltfor $formatx
    echo "test${action}(${formatx}): " >> ${filename}.txt
    eval "test$action $formatx"
    verifytest $action
}

testgetall() {
    local formatx="$1"
    case ${formatx} in
	all) for f in $IMPLS ; do eval "testcase ${action} $f" ; done ;;
	hdf5) ${execdir}/tst_pluginpaths -x "formatx:${formatx},load:${dfalt},getall" >> ${filename}.txt ;;
	nczarr) ${execdir}/tst_pluginpaths -x "formatx:${formatx},load:${dfalt},getall" >> ${filename}.txt ;;
	*) ;; # ignore all other cases
    esac
}                           

auxgetith() {
    local formatx="$1"
    local dfalt
    dfaltfor $formatx
    # get first
    echon "testgetith(${formatx}): 0: " >> ${filename}.txt
    ${execdir}/tst_pluginpaths -x "formatx:${formatx},load:${dfalt},getith:0" >> ${filename}.txt
    # get interior
    echon "testgetith(${formatx}): 1: " >> ${filename}.txt
    ${execdir}/tst_pluginpaths -x "formatx:${formatx},load:${dfalt},getith:1" >> ${filename}.txt
    # get last
    len=`${execdir}/tst_pluginpaths -x "formatx:${formatx},load:${dfalt},length"`
    last=`expr $len - 1`
    echon "testgetith(${formatx}): ${last}: " >> ${filename}.txt
    ${execdir}/tst_pluginpaths -x "formatx:${formatx},load:${dfalt},getith:${last}" >> ${filename}.txt
}

# Test ith
testgetith() {
    local formatx="$1"
    case ${formatx} in
	all) for f in $IMPLS ; do auxgetith "$f" ; done ;;
	hdf5) auxgetith hdf5 ;;
      	nczarr) auxgetith nczarr ;;
	*) ;; # ignore all other cases
    esac
}                           

# Test length
testlength() { # Set plugin path to a known set of value and then print length
    local formatx="$1"
    local dfalt
    dfaltfor $formatx
    case ${formatx} in
	all) for f in $IMPLS ; do testlength "$f" ; done ;;
	hdf5)
	    echon "testlength($formatx): " >> ${filename}.txt
	    ${execdir}/tst_pluginpaths -x "formatx:${formatx},load:${dfalt},length" >> ${filename}.txt
	    ;;
	nczarr)
	    echon "testlength($formatx): " >> ${filename}.txt
	    ${execdir}/tst_pluginpaths -x "formatx:${formatx},load:${dfalt},length" >> ${filename}.txt
	    ;;
	*) ;; # ignore all other cases
    esac
}                           

# Test load
testload() {
    local formatx="$1"
    case ${formatx} in
	all) for f in $IMPLS; do after load $f "formatx:${formatx},load:${dfalt},formatx:$f,getall" ; done ;;
        hdf5) after load hdf5 "formatx:${formatx},load:${dfalt},getall" ;;
        nczarr) after load nczarr "formatx:${formatx},load:${dfalt},getall" ;;
	*) ;; # ignore all other cases
    esac
}                           

# Test remove
auxremovei() {
    local index="$1"
    dfaltfor $formatx
    item=`${execdir}/tst_pluginpaths -x "formatx:${formatx},load:${dfalt},getith:${index}"`
    echo "testremove($formatx): item[${index}]=${item}" >> ${filename}.txt
    before remove $formatx
    after remove $formatx "formatx:${formatx},load:${dfalt},remove:${item},getall"
}

auxremovex() {
    local formatx="$1"
    dfaltfor $formatx
    # get length of dfalt
    len=`${execdir}/tst_pluginpaths -x "formatx:${formatx},load:${dfalt},length"`
    # test removal of initial item
    auxremovei 0
    # test removal of interior item
    auxremovei 2
    last=`expr $len - 1`
    auxremovei $last
}

testremove() {
    local formatx="$1"
    case ${formatx} in
	all) for f in $IMPLS; do auxremovex $f; done ;;
        hdf5) auxremovex $formatx ;;
	nczarr) auxremovex $formatx ;;
	*) ;; # ignore all other cases
    esac
}

# Test append/prepend
testxpend() {
    local formatx="$1"
    local dfalt
    case ${formatx} in
	all) 
	    for f in $IMPLS ; do
		dfaltfor $f
		before xpend $f
		after xpend $f "formatx:${formatx},load:${dfalt},append:/tmp,prepend:/usr,formatx:$f,getall"
	    done
	    ;;
        hdf5 | nczarr)
	    dfaltfor $formatx
	    before xpend $formatx
    	    after xpend $formatx "formatx:${formatx},load:${dfalt},append:/tmp,prepend:/usr,getall"
	    ;;
	*) ;; # ignore all other cases
    esac
}                               

########### pecial one-off tests ##########

auxremovemultix() {
    local formatx="$1"
    # test removal of item /zero
    item="/zero"
    echo "testremovemulti($formatx): item=${item}" >> ${filename}.txt
    before removemulti $formatx "formatx:${formatx},load:${PATHMULTI},getall"
    after removemulti $formatx "formatx:${formatx},load:${PATHMULTI},remove:${item},getall"
}

testremovemulti() { # Test removing multiple occurrences 
    local formatx="$1"
    case ${formatx} in
	all) for f in $IMPLS ; do auxremovemultix $f ; done ;;
        hdf5) auxremovemultix $formatx ;;
	nczarr) auxremovemultix $formatx ;;
	*) ;; # ignore all other cases
    esac
}

cleanup() {
    rm -f tmp_*.txt ref_*.txt
}

init() {
    cleanup
    rm -f all*.txt
    # extract the reference files
    tar -zxf ref_pluginpaths.tgz
}

# Verify output for a specific action
verifytest() {
    local action="$1"
    filenamefor ref $action; reffilename=$filename
    filenamefor tmp $action; tmpfilename=$filename
    # suppress unimplemented cases
    if diff -wBb ${reffilename}.txt ${tmpfilename}.txt ; then
	echo "***PASS: $action"
    else
	echo "***FAIL: $action"
exit
    fi
}

# Concatenate all the tmp_ | ref_ files for debugging
unify() {
    local what="$1"
    local ufile="all_${what}"
    rm -f $ufile
    for a in load getall length getith xpend remove removemulti ; do
	filenamefor $what $a
        cat "${filename}.txt" >> ${ufile}.txt
    done
}

unifybytype() {
    unify tmp
#    unify ref
}

# Test for a specific format
testset() {
    local formatx="$1"
    testcase load "$formatx"
if test 1 = 0 ; then
    testcase getall "$formatx"
    testcase length "$formatx"
    testcase getith "$formatx"
    testcase xpend "$formatx"
    testcase remove "$formatx"
    testcase removemulti "$formatx"
fi
}

init
testset hdf5
testset nczarr
testset all
if test "x$1" = xunify ; then unifybytype; fi
#cleanup
