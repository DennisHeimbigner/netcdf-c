#!/bin/sh



set -x
TESTDIR="$1"
S3TESTSUBTREE="$2"

echo ">>> *** Remove /${TESTSUBTREE} from S3 repository"
echo ${TESTDIR}/nczarr_test/s3util -u "https://s3.us-east-1.amazonaws.com/unidata-zarr-test-data" -k "/${S3TESTSUBTREE}" clear
exit 0
