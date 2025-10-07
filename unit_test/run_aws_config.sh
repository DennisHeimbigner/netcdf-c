#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi 
. ../test_common.sh

set -e

#CMD="valgrind --leak-check=full"

isolate "testdir_aws_config"

THISDIR=`pwd`
cd $ISOPATH

mkdir -p $THISDIR/.aws/

test_cleanup() {
    rm -rfv $THISDIR/.aws/
}
trap test_cleanup EXIT

test_cleanup

mkdir -p $THISDIR/.aws

cat << 'EOF' > $THISDIR/.aws/config
[uni]
region = somewhere-1
endpoint_url = https://example.com/bucket/prefix/1
key = value
extrakey = willbepropagated

[profile unidata]
region = us-east-1
endpoint_url = https://s3.example.domain/
dummy_key = dummy_value

[profile play]
region = us-east-2
endpoint_url = https://endpoint.example.com/
EOF

cat << 'EOF' > $THISDIR/.aws/credentials
[uni]
region = somewhere-2
endpoint_url = https://example.com/bucket/prefix/2
key = value-overwritten

[play] 
aws_access_key_id = DummyKeys
aws_secret_access_key = DummySecret
EOF

rm -fr test1 test2 test 3 test4 test5

cat > test1 <<EOF
Active profile:unidata
	endpoint_url -> https://s3.example.domain/
	region -> us-east-1
	dummy_key -> dummy_value
EOF

cat > test2 <<EOF
Active profile:play
	endpoint_url -> https://endpoint.example.com/
	region -> us-east-2
EOF

cat > test3 <<EOF
Active profile:uni
	endpoint_url -> https://example.com/bucket/prefix/2
	region -> somewhere-2
	key -> value-overwritten
EOF

cat > test4 <<EOF
Active profile:uni
	key -> value-overwritten
	region -> somewhere-2
	endpoint_url -> https://example.com/bucket/prefix/2
	extrakey -> willbepropagated
EOF

cat > test5 <<EOF
Active profile:no
EOF

cmp() {
    CMP=`cat $1`
    if test "x$2" != "x$CMP" ; then FAIL=1; fi
}

echo -e "Testing loading AWS configuration in ${THISDIR}/.aws/config"
export AWS_CONFIG_FILE=${THISDIR}/.aws/config
export AWS_SHARED_CREDENTIALS_FILE=${THISDIR}/.aws/credentials
export AWS_PROFILE=unidata

OUT=`${CMD} ${execdir}/aws_config  endpoint_url region dummy_key`
cmp test1 "$OUT"

export AWS_PROFILE=play
OUT=`${CMD} ${execdir}/aws_config  endpoint_url region`
cmp test2 "$OUT"

export AWS_PROFILE=uni
OUT=`${CMD} ${execdir}/aws_config  endpoint_url region key`
cmp test3 "$OUT"

export AWS_PROFILE=uni
OUT=`${CMD} ${execdir}/aws_config key=value-overwritten region=somewhere-2 endpoint_url=https://example.com/bucket/prefix/2 extrakey=willbepropagated`
cmp test4 "$OUT"

# Will use profile=no
unset AWS_PROFILE
OUT=`${CMD} ${execdir}/aws_config`
cmp test5 "$OUT"

if test "x$FAIL" = x ; then echo "***PASS" ; else echo "***FAIL" ; fi

exit

