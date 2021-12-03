/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

/**
Test the handling of aws profiles and regions.
*/

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "netcdf.h"
#include "ncpathmgr.h"

#define DEBUG

static int
test(void)
{
    int stat = NC_NOERR;
    if((stat=nc_create("test_mingw.nc",NC_NETCDF4|NC_CLOBBER,&ncid)))
        {fprintf(stderr,"line %d stat=%d\n",__LINE__,stat);goto done;}
    if((stat=nc_close(ncid)))
        {fprintf(stderr,"line %d stat=%d\n",__LINE__,stat);goto done;}

    if((stat=nc_open("test_mingw.nc",NC_NETCDF4,&ncid)))
        {fprintf(stderr,"line %d stat=%d\n",__LINE__,stat);goto done;}
    if((stat=nc_close(ncid)))
        {fprintf(stderr,"line %d stat=%d\n",__LINE__,stat);goto done;}
done:
   return stat;
}

int
main(int argc, char** argv)
{
    int stat = NC_NOERR;

    /* Load RC and .aws/config */
    if((stat = nc_initialize())) goto done;
    test();

done:    
    if(stat) printf("*** FAIL: %s(%d)\n",nc_strerror(stat),stat);
    exit(stat?1:0);
}
