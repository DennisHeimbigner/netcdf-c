#!/bin/bash

# Test the implementations of specific filters

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

. "$srcdir/test_nczarr.sh"

set -e

# Function to remove selected -s attributes from file;
# These attributes might be platform dependent
sclean() {
    cat $1 \
 	| sed -e '/:_IsNetcdf4/d' \
	| sed -e '/:_Endianness/d' \
	| sed -e '/_NCProperties/d' \
	| sed -e '/_SuperblockVersion/d' \
      	| sed -e '/_Format/d' \
        | sed -e '/global attributes:/d' \
	| cat > $2
}

# Function to extract _Filter attribute from a file
# These attributes might be platform dependent
getfilterattr() {
V="$1"
sed -e '/${V}.*:_Filter/p' -ed <$2 >$3
}

# Function to extract _Codecs attribute from a file
# These attributes might be platform dependent
getcodecsattr() {
V="$1"
sed -e '/${V}.*:_Codecs/p' -ed <$2 >$3
}

trimleft() {
sed -e 's/[ 	]*\([^ 	].*\)/\1/' <$1 >$2
}


setfilter() {
    FF="$1"
    FSRC="$2"
    FDST="$3"
    FIH5="$4"
    FICX="$5"
    FFH5="$6"
    FFCX="$7"
    if test "x$FFH5" = x ; then FFH5="$FIH5" ; fi
    if test "x$FFCX" = x ; then FFCX="$FICX" ; fi
    rm -f $FDST
    cat $FSRC \
	| sed -e "s/ref_any/${FF}/" \
	| sed -e "s/IH5/${FIH5}/" -e "s/FH5/${FFH5}/" \
	| sed -e "s/ICX/${FICX}/" -e "s/FCX/${FFCX}/" \
	| sed -e 's/"/\\"/g' -e 's/@/"/g' \
	| cat > $FDST
}

# Execute the specified tests

testbzip2() {
zext=$1	
echo "*** Testing processing of filter bzip2 for map $zext"
deletemap $zext tmp_bzip2
fileargs tmp_bzip2
setfilter bzip2 ref_any.cdl tmp_bzip2.cdl '307,9' '[{\"id\": \"bz2\",\"level\": \"9\"}]'
${NCGEN} -4 -lb -o $fileurl ${srcdir}/tmp_bzip2.cdl
${NCDUMP} -n bzip2 -s $fileurl > tmp_bzip2.tmp
sclean tmp_bzip2.tmp tmp_bzip2.dump
diff -b -w tmp_bzip2.cdl tmp_bzip2.dump
}

testblosc() {
zext=$1	
echo "*** Testing processing of filter blosc for map $zext"
deletemap $zext tmp_blosc
fileargs tmp_blosc
setfilter blosc ref_any.cdl tmp_blosc.cdl '32001,2,2,4,0,5,1,1' '[{\"id\": \"blosc\",\"clevel\": 5,\"blocksize\": 0,\"cname\": \"lz4\",\"shuffle\": 1}]'
${NCGEN} -4 -lb -o $fileurl ${srcdir}/tmp_blosc.cdl
${NCDUMP} -n blosc -s $fileurl > tmp_blosc.tmp
sclean tmp_blosc.tmp tmp_blosc.dump
diff -b -w tmp_blosc.cdl tmp_blosc.dump
}

testset() {
# Which test cases to exercise
    testbzip2 $1
    testblosc $1
}

testset file
if test "x$FEATURE_NCZARR_ZIP" = xyes ; then testset zip ; fi
if test "x$FEATURE_S3TESTS" = xyes ; then testset s3 ; fi

exit 0
