#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

set -e

export NCZ_NOOPTIMIZATION=1
alias zc='../ncdump/.libs/ncdumpchunks -v v'

# Test whole variable write and read
echo "Test whole variable write then read"
rm -f tmp_whole.txt tmp_whole.cdl
${execdir}/tst_chunkcases -D -4 -d 6,6 -c 6,6 -W -w file://tmp.nzf\#mode=nczarr,nzf
${execdir}/tst_chunkcases -D -4 -d 6,6 -c 6,6 -W -r file://tmp.nzf\#mode=nczarr,nzf > tmp_whole.txt
diff -b ${srcdir}/ref_tmp_whole.txt tmp_whole.txt
${NCDUMP} file://tmp.nzf\#mode=nczarr,nzf > tmp_whole.cdl
diff -b ${srcdir}/ref_tmp_whole.cdl tmp_whole.cdl

exit
