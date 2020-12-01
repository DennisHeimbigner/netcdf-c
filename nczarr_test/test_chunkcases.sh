#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

set -e

alias zc='../ncdump/.libs/ncdumpchunks -v v'

TC="${execdir}/tst_chunkcases -4"

# Test whole variable write and read
F="file://tmp_whole.nzf#mode=nczarr,nzf"
#F=tmp_whole.nc
echo "Test whole variable write then read"
rm -f tmp_whole.txt tmp_whole.cdl
$TC -d 6,6 -c 6,6 -X w -w $F
$TC -d 6,6 -c 6,6 -X w -r $F > tmp_whole.txt
diff -b ${srcdir}/ref_whole.txt tmp_whole.txt
${NCDUMP} $F > tmp_whole.cdl
diff -b ${srcdir}/ref_whole.cdl tmp_whole.cdl

# Test skipping whole chunks
F="file://tmp_skip.nzf#mode=nczarr,nzf"
#F=tmp_skip.nc
echo "Test chunk skipping during read"
rm -f tmp_skip.txt tmp_skip.cdl tmp_skipw.cdl
$TC -d 6,6 -c 2,2 -w $F
$TC -s 5,5 -p 6,6 -r $F > tmp_skip.txt
${NCDUMP} $F > tmp_skip.cdl
diff -b ${srcdir}/ref_skip.txt tmp_skip.txt
diff -b ${srcdir}/ref_skip.cdl tmp_skip.cdl

echo "Test chunk skipping during write"
rm -f tmp_skipw.cdl
$TC -d 6,6 -s 5,5 -p 6,6 -w $F
${NCDUMP} $F > tmp_skipw.cdl
diff -b ${srcdir}/ref_skipw.cdl tmp_skipw.cdl

#$TC -s 5,5  -p 6,6 -O
#../ncdump/ncdumpchunks -v v $F





