#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi 
. ../test_common.sh

set -e
set -x

echo "${MSYS_PREFIX}"
echo "${MSYS2_PREFIX}"
echo "${MINGW_PREFIX}"
cygpath -w "/"

exec 2>/dev/null
cd /usr/local
pwd -W

${execdir}/test_pathcvt

