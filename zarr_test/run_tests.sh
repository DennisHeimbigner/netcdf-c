#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

set -e
set -x

# Control which test sets are executed
# possible sets: meta
TESTS="meta"

echo ""
echo "*** Integration Testing"

for T in $TESTS ; do
case "$T" in

meta)
echo ""; echo "*** Test meta-data write/read"
CMD="${execdir}/t_meta -c"
$CMD create
ncdump ./testmeta.ncz >./t_meta_create.cdl
diff -wb ${srcdir}/ref_t_meta_create.cdl ./t_meta_create.cdl
$CMD dim1
ncdump ./testmeta.ncz >./t_meta_dim1.cdl
diff -wb ${srcdir}/ref_t_meta_dim1.cdl ./t_meta_dim1.cdl
$CMD var1
ncdump ./testmeta.ncz >./t_meta_var1.cdl
diff -wb ${srcdir}/ref_t_meta_var1.cdl ./t_meta_var1.cdl
;;

*) echo "Unknown test set: $T"; exit ;;

esac
done

exit
