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
	      -e '/:_Endianness/d' \
	      -e '/_NCProperties/d' \
	      -e '/_SuperblockVersion/d' \
      	      -e '/_Format/d' \
              -e '/global attributes:/d' \
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
    FILT="$4"
    CODEC="$5"
    CODEC2=`echo "$CODEC" | sed -e 's|"|\\\\\\\\"|g'`
    rm -f $FDST
    cat ${srcdir}/$FSRC \
	| sed -e "s/ref_any/${FF}/" \
	      -e "s|@IX@|ivar:_Filter = \"${FILT}\" ;\n\t\tivar:_Codecs = \"${CODEC2}\" ;|" \
      	      -e "s|@FX@|fvar:_Filter = \"${FILT}\" ;\n\t\tfvar:_Codecs = \"${CODEC2}\" ;|" \
	      -e "s|@chunked@|\"chunked\"|" \
	| cat > $FDST
}

# Execute the specified tests

runfilter() {
zext="$1"
zfilt="$2"
zparams="$3"
zcodec="$4"
echo "*** Testing processing of filter $zfilt for map $zext"
deletemap $zext "tmp_${zfilt}"
fileargs "tmp_${zfilt}"
rm -f tmp_${zfilt}.cdl tmp_${zfilt}.tmp tmp_${zfilt}.dump
setfilter $zfilt ref_any.cdl tmp_${zfilt}.cdl "$zparams" "$zcodec"
if ${NCGEN} -4 -lb -o $fileurl "tmp_${zfilt}.cdl" ; then
  ${NCDUMP} -n $zfilt -s $fileurl > tmp_${zfilt}.tmp
  sclean "tmp_${zfilt}.tmp" tmp_${zfilt}.dump
fi
}

runfilterbuiltin() {
zext="$1"
zfilt="$2"
zparams="$3"
zcodec="$4"
echo "*** Testing processing of filter $zfilt for map $zext"
rm -f tmp_$zfilt.cdl tmp_$zfilt.dump tmp_$zfilt.tmp
deletemap $zext "tmp_${zfilt}"
fileargs "tmp_${zfilt}"
case "$zfilt" in
    fletcher32) sed -e "s|@IX@|ivar:_Fletcher32=\"${zparams}\" ;|" -e "s|@chunked@|\"chunked\"|" \
		    < ref_any.cdl > tmp_${zfilt}.cdl ;;
    shuffle)    sed -e "s|@IX@|ivar:_Shuffle=\"${zparams}\" ;|" -e "s|@chunked@|\"chunked\"|" \
		    < ref_any.cdl > tmp_${zfilt}.cdl ;;
    deflate)    sed -e "s|@IX@|ivar:_Deflate=\"${zparams}\" ;|" -e "s|@chunked@|\"chunked\"|" \
		    < ref_any.cdl > tmp_${zfilt}.cdl ;;
esac
if ${NCGEN} -4 -lb -o $fileurl "tmp_${zfilt}.cdl" ; then
  ${NCDUMP} -n $zfilt -s $fileurl > "tmp_${zfilt}.tmp"
  sclean "tmp_${zfilt}.tmp" "tmp_${zfilt}.dump"
fi
}

testfletcher32() {
  zext=$1
  runfilterbuiltin $zext fletcher32 'true' '[{\"id\": \"Fletcher32\"}]'
  if test -f "tmp_fletcher32.dump" ; then
      # need to Filter
      diff -b -w "tmp_fletcher32.cdl" "tmp_fletcher32.dump"
  else
      echo "XFAIL: filter=fletcher32 zext=$zext"
  fi
}

testshuffle() {
  zext=$1
  runfilterbuiltin $zext shuffle 'true' '[{\"id\": \"Fletcher32\"}]'
  if ! diff -b -w "tmp_shuffle.cdl" "tmp_shufflex.dump" ; then
      echo "***FAIL: filter=shuffle zext=$zext"
  fi
}

testdeflate() {
  zext=$1
  runfilterbuiltin $zext deflate 'true' '[{\"id\": \"Deflate"}]'
  if ! diff -b -w "tmp_deflate.cdl" "tmp_deflatex.dump" ; then
      echo "***FAIL: filter=deflate zext=$zext"
  fi
}

testbzip2() {
  zext=$1
  runfilter $zext bzip2 '307,9' '[{\"id\": \"bz2\",\"level\": \"9\"}]'
  if ! diff -b -w "tmp_bzip2.cdl" "tmp_bzip2.dump" ; then
      echo "***FAIL: filter=bzip2 zext=$zext"
  fi
}

testszip() {
  zext=$1
#  H5_SZIP_NN_OPTION_MASK=32;  H5_SZIP_MAX_PIXELS_PER_BLOCK_IN=32
  runfilter $zext szip '4,32,32' '[{\"id\": \"szip\",\"mask\": 32,\"pixels-per-block\": 32}]'
  if ! diff -b -w "tmp_szip.cdl" "tmp_szip.dump" ; then
      echo "***FAIL: filter=szip zext=$zext"
  fi
}

testblosc() {
  zext=$1
  runfilter $zext blosc '32001,0,0,0,0,5,1,1' '[{"id": "blosc","clevel": 5,"blocksize": 0,"cname": "lz4","shuffle": 1}]'
  if ! diff -b -w "tmp_blosc.cdl" "tmp_blosc.dump"; then
      echo "***FAIL: filter=blosc zext=$zext"
  fi
}

testset() {
# Which test cases to exercise
    testfletcher32 $1
    testshuffle $1    
    testdeflate $1
    testszip $1
    testbzip2 $1
    testblosc $1
}

testset file
#if test "x$FEATURE_NCZARR_ZIP" = xyes ; then testset zip ; fi
#if test "x$FEATURE_S3TESTS" = xyes ; then testset s3 ; fi

exit 0
