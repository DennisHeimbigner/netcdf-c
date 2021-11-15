#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi 
. ../test_common.sh

set -e
set -x

echo "${MSYS2_PREFIX}"

${execdir}/test_pathcvt
