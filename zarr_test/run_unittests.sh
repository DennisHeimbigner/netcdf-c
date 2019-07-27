#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

set -e
set -x

# Control which tests are executed
# possible set: nc4 proj
TESTS="nc4"

echo ""
echo "*** Unit Testing"

for T in "$TESTS" ; do
case "$T" in

nc4)
echo ""; echo "*** Test zmap_nc4"
${execdir}/ut_mapnc4
;;

proj)
echo ""; echo "*** Test projection computations"
${execdir}/ut_projections -r 1 -d 4 -c 2 -s "[0:4:1]"
;;

*) echo "Unknown test set: $T"; exit ;;

esac
done

# cleanup

exit
