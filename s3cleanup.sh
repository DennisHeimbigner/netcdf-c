#!/bin/sh

# Uncomment to get verbose output
VERBOSE=1

if test "x$VERBOSE" = x1 ; then set -x; fi

# Constants passed in from configure.ac/CMakeLists
abs_top_srcdir='/home/dmh/git/netcdf.fork'
abs_top_builddir='/home/dmh/git/netcdf.fork'
# The process id
puid='14'

# Additional configuration information
. ${abs_top_builddir}/test_common.sh

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
    exit 1;
fi

rm -f s3cleanup_${puid}.json s3cleanup_${puid}.uids s3cleanup_${puid}.keys

# Get complete set of keys in ${S3TESTSUBTREE} prefix
unset ALLKEYS
if ! aws s3api list-objects-v2 --bucket unidata-zarr-test-data --prefix "${S3TESTSUBTREE}" | grep -F '"Key":' >s3cleanup_${puid}.keys ; then
    echo "No keys found"
    rm -f s3cleanup_${puid}.uids s3cleanup_${puid}.keys
    rm -f s3cleanup_${puid}.json
    exit 0
fi

while read -r line; do
  KEY=`echo "$line" | sed -e 's|[^"]*"Key":[^"]*"\([^"]*\)".*|\1|'`
  # Ignore keys that do not start with ${S3TESTSUBTREE}
  PREFIX=`echo "$KEY" | sed -e 's|\([^/]*\)/.*|\1|'`
  if test "x$PREFIX" = "x$S3TESTSUBTREE" ; then
      ALLKEYS="$ALLKEYS $KEY"
  fi
done < s3cleanup_${puid}.keys

# get the uid's for all the subtrees to be deleted
UIDS=`cat ${top_srcdir}/s3cleanup_${puid}.uids | tr -d '\r' | tr '\n' ' '`
# Capture the keys matching any uid
unset DELLIST
unset MATCH
FIRST=1
DELLIST="{\"Objects\":["
for key in $ALLKEYS ; do
    for uid in $UIDS ; do
        case "$key" in
            "$S3TESTSUBTREE/testset_${uid}"*)
                # capture the key'
                if test $FIRST = 0 ; then DELLIST="${DELLIST},"; fi
		DELLIST="${DELLIST}
{\"Key\":\"$key\"}"
                MATCH=1
                ;;
            *) if test "x$VERBOSE" = x1 ; then echo "Ignoring \"$key\""; fi ;;
        esac
    done
    FIRST=0
done
DELLIST="${DELLIST}],\"Quiet\":false}"
if test "x$MATCH" = x1 ;then
    echo "$DELLIST" > s3cleanup_${puid}.json
    aws s3api delete-objects --bucket unidata-zarr-test-data --delete "file://s3cleanup_${puid}.json"
fi
rm -f s3cleanup_${puid}.uids s3cleanup_${puid}.keys
rm -f s3cleanup_${puid}.json
