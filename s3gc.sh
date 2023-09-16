#!/bin/bash

if test "x$srcdir" = x ; then srcdir=`pwd`; fi 
. ./test_common.sh

delta="$1"

# Sanity checks

# 1. This requires that the AWS CLI (command line interface) is installed.
if ! which aws ; then
    echo ">>>> The s3cleanup script requires the \"aws\" command (i.e. the AWS command line interface program)"
    echo ">>>> Try installing \"awscli\" package with apt or equivalent."
    exit 0
fi

# 2. Make sure S3TESTSUBTREE is defined
if test "x$S3TESTSUBTREE" = x ; then
    echo ">>>> The s3cleanup script requires that S3TESTSUBTREE is defined."
    exit 1
fi

# 3. Make sure delta is defined
if test "x$delta" = x ; then
    echo ">>>> No delta argument provided"
    echo ">>>> Usage: s3gc <delta>"
    echo ">>>>        where <delta> is number of days prior to today to begin cleanup"
    exit 1
fi


# This script takes a delta (in days) as an argument.
# It then removes from the Unidata S3 bucket those keys
# that are older than (current_date - delta).

# Compute current_date - delta

# current date
current=`date +%s`
# convert delta to seconds
deltasec=$((delta*24*60*60))
# Compute cleanup point
lastdate=$((current-deltasec))

rm -f s3gc.json

# Get complete set of keys in ${S3TESTSUBTREE} prefix
aws s3api list-objects-v2 --bucket unidata-zarr-test-data --prefix "${S3TESTSUBTREE}" | grep -F '"Key":' >s3gc.keys
set +x
while read -r line; do
  KEY=`echo "$line" | sed -e 's|[^"]*"Key":[^"]*"\([^"]*\)".*|\1|'`
  # Ignore keys that do not start with ${S3TESTSUBTREE}
  PREFIX=`echo "$KEY" | sed -e 's|\([^/]*\)/.*|\1|'`
  if test "x$PREFIX" = "x$S3TESTSUBTREE" ; then
      ALLKEYS="$ALLKEYS $KEY"
  fi
done < s3gc.keys
set -x
# Look at each key and see if it is less than lastdate.
# If so, then record that key

# Capture the keys with old uids to delete
unset DELLIST
unset MATCH
FIRST=1
DELLIST="{\"Objects\":["
# We can delete at most 1000 objects at a time, so divide into sets of size 500
REM="$ALLKEYS"
while test "x$REM" != x ; do
K500=`echo "$REM" | cut -d' ' -f 1-500`
REM=`echo "$REM" | cut -d' ' -f 501-`
for key in $K500 ; do
    case "$key" in
        "$S3TESTSUBTREE/testset_"*)
             # Capture the uid for this key         
             s3uid=`echo $key | sed -e "s|$S3TESTSUBTREE/testset_\([0-9][0-9]*\)/.*|\1|"`
             # check that we got a uid       
             if test "x$s3uid" != x ; then
                # Test age of the uid
                if test $((s3uid < lastdate)) = 1; then
                    if test $FIRST = 0 ; then DELLIST="${DELLIST},"; fi
                    DELLIST="${DELLIST}\n{\"Key\":\"$key\"}"
                    MATCH=1
                fi
             else
                echo "Excluding \"$key\""
             fi
	     ;;
        *) echo "Ignoring \"$key\"";;
    esac
    FIRST=0
done
DELLIST="${DELLIST}],\"Quiet\":false}"
rm -f s3gc.json
if test "x$MATCH" = x1 ;then
    echo "$DELLIST" > s3gc.json
echo        aws s3api delete-objects --bucket unidata-zarr-test-data --delete "file://s3gc.json"
fi
done
#rm -f s3gc.json

