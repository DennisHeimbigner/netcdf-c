#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

set -e
set -x

# Control which test sets are executed
# possible sets: mapnc4 proj
TESTS="proj"

echo ""
echo "*** Unit Testing"

for T in "$TESTS" ; do
case "$T" in

mapnc4)
echo ""; echo "*** Test zmap_nc4"
CMD="${execdir}/ut_mapnc4 -c"
$CMD create
ncdump ./testnc4.nc >./ut_create.cdl
diff -wb ${srcdir}/ref_ut_create.cdl ./ut_create.cdl
$CMD delete
if test -f testnc4.nc; then
    echo "delete did not delete testnc4.nc"
    exit 1
fi
$CMD writemeta
ncdump ./testnc4.nc >./ut_wmeta.cdl
diff -wb ${srcdir}/ref_ut_wmeta.cdl ./ut_wmeta.cdl
$CMD writemeta2 # depends on writemeta
ncdump ./testnc4.nc >./ut_wmeta2.cdl
diff -wb ${srcdir}/ref_ut_wmeta2.cdl ./ut_wmeta2.cdl
$CMD readmeta > ut_rmeta.txt # depends on writemeta2
diff -wb ${srcdir}/ref_ut_rmeta.txt ./ut_rmeta.txt
$CMD write # depends on writemeta2
ncdump ./testnc4.nc >./ut_write.cdl
diff -wb ${srcdir}/ref_ut_write.cdl ./ut_write.cdl
$CMD read # depends on writemeta2
$CMD misc
;;

proj)
echo ""; echo "*** Test projection computations"
echo ""; echo "*** Test 1"
${execdir}/ut_projections -r 1 -d 4 -c 2 -s "[0:4:1]" > ut_proj1.txt
diff -wb ${srcdir}/ref_ut_proj1.txt ./ut_proj1.txt
;;

*) echo "Unknown test set: $T"; exit ;;

esac
done

exit
