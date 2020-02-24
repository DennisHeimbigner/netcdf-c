/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#include "zprojtest.h"

int
main(int argc, char** argv)
{
    int stat = NC_NOERR;
    int i;
    ProjTest test;
    NCZChunkRange ncrv[NC_MAX_VAR_DIMS];

    /* Initialize */
    memset(&test,0,sizeof(test));
    memset(ncrv,0,sizeof(ncrv));

    if((stat = ut_proj_init(argc, argv, &test))) goto done;

    if((stat = NCZ_compute_chunk_ranges(test.rank, test.slices, test.chunklen, ncrv)))
	goto done;
    printf("Ranges:\n");
    for(i=0;i<test.rank;i++)
        printf("\t[%d] %s\n",i,printrange(&ncrv[i],buf));

done:
    if(stat)
	nc_strerror(stat);
    return (stat ? 1 : 0);    
}
