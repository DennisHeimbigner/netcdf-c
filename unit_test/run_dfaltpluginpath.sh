#!/bin/sh

# Test the various plugin path defaults that can occur

if test "x$srcdir" = x ; then srcdir=`pwd`; fi 
. ../test_common.sh

set -e

#CMD="valgrind --leak-check=full"

TESTHPP="/tmp;${HOME}"

# Test with no HDF5_PLUGIN_PATH
unset HDF5_PLUGIN_PATH
NOHPP1=`ncpluginpath -f global`
if test "x$NOHPP1" = "" || test "x$NOHPP1" = "" ; then
echo "***PASS: default plugin path = |$NOHPP1|"
else
echo "***FAIL: default plugin path = |$NOHPP1|"
fi
exit

# Test with given HDF5_PLUGIN_PATH
unset HDF5_PLUGIN_PATH
export HDF5_PLUGIN_PATH="$TESTHPP"
HPP1=`ncpluginpath -f global`
if test "x$HPP1" = "$TESTHPP" ; then
else
echo "***PASS: default plugin path: |$HPP1| HDF5_PLUGIN_PATH=|"$HDF5_PLUGIN_PATH|"
fi
echo "***FAIL: default plugin path: |$HPP1| HDF5_PLUGIN_PATH=|"$HDF5_PLUGIN_PATH|"
exit
