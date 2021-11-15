#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi 
. ../test_common.sh

set -e
set -x

echo "${MINGW_PREFIX}"
cygpath -w "/"
pwd -W

${execdir}/test_pathcvt
