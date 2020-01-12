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

# Process _NCProperties to remove version specific info
# Usage cleanncprops <src> <dst>
cleanncprops() {
  src="$1"
  dst="$2"
  rm -f $dst
  cat $src \
  | sed -e 's/_SuperblockVersion=[0-9][0-9]*/_Superblockversion=0/' \
  | sed -e 's/\(\\"_NCProperties\\":[ 	]*\\"version=\)[0-9],.*/\1\\"}" ;/' \
  | cat >$dst
}

#  | sed -e 's/\(_NCProperties\\":.*version=\)[0-9],netcdf=[^\\"]*/\1/'
for T in $TESTS ; do
case "$T" in

meta)
echo ""; echo "*** Test meta-data write/read"
CMD="${execdir}/t_meta${ext} -c"
$CMD create
ncdump ./testmeta.ncz >./t_meta_create.cdl
cleanncprops t_meta_create.cdl tmp_meta_create.cdl
diff -wb ${srcdir}/ref_t_meta_create.cdl ./tmp_meta_create.cdl
rm -f tmp_meta_create.cdl
$CMD dim1
ncdump ./testmeta.ncz >./t_meta_dim1.cdl
cleanncprops t_meta_dim1.cdl tmp_meta_dim1.cdl
diff -wb ${srcdir}/ref_t_meta_dim1.cdl ./tmp_meta_dim1.cdl
rm -f tmp_meta_dim1.cdl
$CMD var1
ncdump ./testmeta.ncz >./t_meta_var1.cdl
cleanncprops t_meta_var1.cdl tmp_meta_var1.cdl
diff -wb ${srcdir}/ref_t_meta_var1.cdl ./tmp_meta_var1.cdl
rm -f tmp_meta_var1.cdl
;;

*) echo "Unknown test set: $T"; exit ;;

esac
done
