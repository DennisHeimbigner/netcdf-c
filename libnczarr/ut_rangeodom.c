/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#include "zprojtest.h"

/**
Test walking all combinations of chunks covered by a slice.
*/

int
main(int argc, char** argv)
{
    int stat = NC_NOERR;
    int r;
    ProjTest test;
    NClist* listv[NC_MAX_VAR_DIMS];
    NCZChunkRange ncrv[NC_MAX_VAR_DIMS];
    NCZSliceProjections slpv[NC_MAX_VAR_DIMS];
    NCZOdometer* odom = NULL;

    /* Initialize */
    memset(&test,0,sizeof(test));
    memset(listv,0,sizeof(listv));
    memset(ncrv,0,sizeof(ncrv));
    memset(slpv,0,sizeof(slpv));

    if((stat = ut_proj_init(argc, argv, &test))) goto done;
    
    if((stat = NCZ_compute_chunk_ranges(test.rank,test.slices,test.chunklen,ncrv)))
	goto done;

    printf("ChunkRanges(%u):\n",(unsigned)test.rank);
    for(r=0;r<test.rank;r++) {
        printf("[%d] %s\n",r,nczprint_chunkrange(ncrv[r]));
    }
    if((stat = NCZ_chunkindexodom(test.rank, ncrv, &odom)))
	goto done;

    while(nczodom_more(odom)) {
	size64_t* indices = nczodom_indices(odom);
        printf("[");
        for(r=0;r<test.rank;r++)
            printf("%s%llu",(r==0?"":","),indices[r]);
        printf("]\n");
	nczodom_next(odom);
    }

done:
    if(stat)
	nc_strerror(stat);
    return (stat ? 1 : 0);    
}
