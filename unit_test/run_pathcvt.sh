#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi 
. ../test_common.sh

set -e
set -x

echo "${MSYS_PREFIX}"
echo "${MSYS2_PREFIX}"
echo "${MINGW_PREFIX}"
cygpath -w "/"

XX=`pwd -W`
cd /
pwd
pwd -W
cd $XX

${execdir}/test_pathcvt

