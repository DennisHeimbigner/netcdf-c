#!/bin/bash

# Test the implementations of specific filters

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

set -e

#cd ../plugins; make clean all >/dev/null; cd ../nczarr_test

# Load the findplugins function
. ${builddir}/findplugin.sh
echo "findplugin.sh loaded"

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

# Locate the plugin path and the library names; argument order is critical
# Find bzip2 and capture
# Assume all test filters are in same plugin dir
findplugin h5bzip2

echo "final HDF5_PLUGIN_PATH=${HDF5_PLUGIN_PATH}"
export HDF5_PLUGIN_PATH

setfilter() {
    FF="$1"
    FSRC="$2"
    FDST="$3"
    FIH5="$4"
    FFH5="$6"
    if test "x$FFH5" = x ; then FFH5="$FIH5" ; fi
    rm -f $FDST
    cat $FSRC \
	| sed -e "s/ref_any/${FF}/" \
	| sed -e "s/IH5/${FIH5}/" -e "s/FH5/${FFH5}/" \
	| sed -e 's/"/\\"/g' -e 's/@/"/g' \
	| cat > $FDST
}

# Execute the specified tests

testbzip2() {
echo "*** Testing processing of filter bzip2"
setfilter bzip2 ref_any.cdl tmp_bzip2.cdl '307,9'
${NCGEN} -4 -lb ${srcdir}/tmp_bzip2.cdl
${NCDUMP} -n bzip2 -s tmp_bzip2.nc > tmp_bzip2.tmp
sclean tmp_bzip2.tmp tmp_bzip2.dump
diff -b -w tmp_bzip2.cdl tmp_bzip2.dump
}

testblosc() {
echo "*** Testing processing of filter blosc"
setfilter blosc ref_any.cdl tmp_blosc.cdl '32001,2,2,4,256,5,1,1'
${NCGEN} -4 -lb ${srcdir}/tmp_blosc.cdl
${NCDUMP} -n blosc -s tmp_blosc.nc > tmp_blosc.tmp
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
