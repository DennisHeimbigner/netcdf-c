#!/bin/bash

if test "x$srcdir" = x ; then srcdir=`pwd`; fi 
. ../test_common.sh

set -e

# Test cases
# group_only - type defined in same group as var/attr
# ancestor_only - type defined in some ancestor group of var/attr
# ancestor_subgroup - type defined in both ancestor group and subgroup
# preorder - type defined in some preceding, non ancestor group

TSTS="type_group_only type_ancestor_only type_ancestor_subgroup type_preorder"

SETUP=1

setup() {
    ${NCGEN} -4 -lb ${srcdir}/$1.cdl
}

testscope() {
${NCCOPY} ${srcdir}/$1.nc ${srcdir}/$1_copy.nc
${NCDUMP} -h -n $1 ${srcdir}/$1_copy.nc >copy_$1.cdl
diff -wB $1.cdl copy_$1.cdl
REFT=`${execdir}/printfqn $1.nc test_variable`
COPYT=`${execdir}/printfqn $1_copy.nc test_variable`
if test "x$REFT" != "x$COPYT" ; then
  echo "***Fail: ref=${REFT} copy=${COPYT}"
fi
}

if test "x$SETUP" = x1 ; then
for t in $TSTS ; do
  setup $t
done
fi

for t in $TSTS ; do
  testscope $t
done

exit 0


