#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi 
. ../test_common.sh

tests3=1

. "$srcdir/run_it_test1.sh"

main() {
 test s3 'https://stratus.ucar.edu/unidata-netcdf-zarr-testing'
}

if test "x$tests3" = x1 ; then main ; fi
