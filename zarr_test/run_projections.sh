#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

set -e

echo ""
echo "*** Testing projection computations"

${execdir}/t_projections -r 1 -d 4 -c 2 -s "[0:4:1]"

# cleanup

exit
