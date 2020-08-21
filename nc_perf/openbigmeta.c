/*! \file

Copyright 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001, 2002,
2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014,
2015, 2016, 2017, 2018
University Corporation for Atmospheric Research/Unidata.

See \ref copyright file for more info.

*/
/*
Open a netcdf-4 file with horrendously large metadata.
*/

#include <config.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include "netcdf.h"

#include "tst_utils.h"

#define FILE "bigmeta.nc"

struct Defaults dfalts;

int
main(int argc, char **argv)
{
    int ncid;
    time_t starttime, endtime;
    long long delta;
    char* path = NULL;

    CHECK(getdefaultoptions(argc,argv,&dfalts));

    CHECK(nc4_buildpath(FILE,dfalts.formatx,&path));

    starttime = 0;
    endtime = 0;
    time(&starttime);
    assert(nc_open(path,NC_NETCDF4,&ncid) == NC_NOERR);
    time(&endtime);
    assert(nc_close(ncid) == NC_NOERR);

    /* Compute the delta 1 second resolution is fine for this */
    delta = (long long)(endtime - starttime);
    printf("open delta=%lld\n",delta);
    nullfree(path);
    return 0;
}
