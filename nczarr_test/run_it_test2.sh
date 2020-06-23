#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi 
. ../test_common.sh

. "$srcdir/test_nczarr.sh"

set -e

TESTSET="\
ref_dimscope \
ref_tst_group_data \
ref_tst_solar_1 \
ref_tst_nans \
ref_tst_nul4 \
"       

cdl="${TOPSRCDIR}/ncdump/cdl"
KFLAG=4

# Functions

checkxfail() {
   # determine if this is an xfailtest
   isxfail=
   for t in ${ALLXFAIL} ; do
     if test "x${t}" = "x${x}" ; then isxfail=1; fi
   done
}

diffcycle() {
echo ""; echo "*** Test cycle zext=$1"
for x in ${TESTSET} ; do
   if test $verbose = 1 ; then echo "*** Testing: ${x}" ; fi
   # determine if we need the specflag set
   specflag=
   headflag=
   for s in ${SPECIALTESTS} ; do
      if test "x${s}" = "x${x}" ; then specflag="-s"; headflag="-h"; fi
   done
   # determine if this is an xfailtest
   checkxfail ${x}
   deletemap ${x}        
   rm -f ${x}.dmp
   fileargs
   ${NCGEN} -b -${KFLAG} -o "${fileurl}" ${cdl}/${x}.cdl
   ${NCDUMP} ${headflag} ${specflag} -n ${x} ${fileurl} > ${x}.dmp
   # compare the expected (silently if XFAIL)
   if test "x$isxfail" = "x1" -a "x$SHOWXFAILS" = "x" ; then
     if diff -b -bw ${expected}/${x}.dmp ${x}.dmp >/dev/null 2>&1; then ok=1; else ok=0; fi
   else
     if diff -b -w ${expected}/${x}.dmp ${x}.dmp ; then ok=1; else ok=0; fi
   fi
   if test "x$ok" = "x1" ; then
     test $verbose = 1 && echo "*** SUCCEED: ${x}"
     passcount=`expr $passcount + 1`
   elif test "x${isxfail}" = "x1" ; then
     echo "*** XFAIL : ${x}"
     xfailcount=`expr $xfailcount + 1`
   else
     echo "*** FAIL: ${x}"
     failcount=`expr $failcount + 1`
   fi
done
}

echo "*** Testing ncgen with -${KFLAG} and zmap=${zext}"

main() {
extfor $1
RESULTSDIR="results.${zext}"
mkdir -p ${RESULTSDIR}
cd ${RESULTSDIR}
diffcycle
cd ..
totalcount=`expr $passcount + $failcount + $xfailcount`
okcount=`expr $passcount + $xfailcount`
echo "*** PASSED: zext=${zext} ${okcount}/${totalcount} ; ${xfailcount} expected failures ; ${failcount} unexpected failures"
}

rm -rf ${RESULTSDIR}
main nz4
main nzf

if test $failcount -gt 0 ; then exit 1; else  exit 0; fi
