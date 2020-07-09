#!/bin/sh

alias ad='aws s3api list-objects --endpoint-url=https://stratus.ucar.edu --bucket=unidata-netcdf-zarr-testing'

# Check settings
checksetting() {
if test -f ${TOPBUILDDIR}/libnetcdf.settings ; then
    local PATTERN
    PATTERN="${1}:[ 	]*yes"
    if grep "$PATTERN" <${TOPBUILDDIR}/libnetcdf.settings ; then
       HAVE_SETTING=1
    else
       unset HAVE_SETTING
    fi
fi
}

extfor() {
    case "$1" in
    nc4) zext="nz4" ;;
    nz4) zext="nz4" ;;
    nzf) zext="nzf" ;;
    s3) zext="s3" ;;
    *) echo "unknown kind: $1" ; exit 1;;
    esac
}

deletemap() {
    case "$1" in
    nc4) rm -fr $2;;
    nz4) rm -fr $2;;
    nzf) rm -fr $2;;
    esac
}

mapexists() {
    mapexists=1
    case "$1" in
    nz4) if test -f $file; then mapexists=0; fi ;;
    nzf) if test -f $file; then mapexists=0; fi ;;		 
    s3)
	if "./zmapio $fileurl" ; then mapexists=1; else mapexists=0; fi
        ;;
    *) echo unknown format: $1 : abort ; exit 1 ;;
    esac
    if test $mapexists = 1 ; then
      echo "delete did not delete $1"
    fi
}

fileargs() {
  if test "x$zext" = xs3 ; then
      fileurl="https://stratus.ucar.edu/unidata-netcdf-zarr-testing/test$tag#mode=nczarr,$zext"
      file=$fileurl
  else
      file="test$tag.$zext"
      fileurl="file://test$tag.$zext#mode=nczarr,$zext"
  fi
}


dumpmap1() {
    tmp=
    if test -f $1 ; then
      ftype=`file -b $1`
      case "$ftype" in
      [Aa][Ss][Cc]*) tmp=`cat $1 | tr '\r\n' '  '` ;;
      [Jj][Aa][Oo][Nn]*) tmp=`cat $1 | tr '\r\n' '  '` ;;
      data*) tmp=`hexdump -v -e '1/1 " %1x"' testmap.nzf/data1/0` ;;
      empty*) unset tmp ;;
      *) echo fail ; exit 1 ;;
      esac
      echo "$1 : |$tmp|" >> $2
    else
      echo "$1" >> $2
    fi
}

dumpmap() {
    case "$1" in
    nz4) rm -f $3 ; ${NCDUMP} $2 > $3 ;;
    nzf)
	rm -f $3
	export LC_ALL=C
	lr=`find $2 | sort| tr  '\r\n' '  '`
	for f in $lr ; do  dumpmap1 $f $3 ; done
	;;
    s3) ad ;;
    *) echo "dumpmap failed" ; exit 1;
    esac
}

ZMD="${execdir}/zmapio"
