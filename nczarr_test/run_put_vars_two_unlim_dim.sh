#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

. "$srcdir/test_nczarr.sh"

set -e
set -x

find ${execdir} test_put_vars_two_unlim_dim${ext}

nm ${execdir}/.libs/test_put_vars_two_unlim_dim${ext}

${execdir}/test_put_vars_two_unlim_dim${ext}

exit 0
