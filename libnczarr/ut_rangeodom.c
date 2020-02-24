/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#include "zprojtest.h"

static void odomprinter(size_t rank, size64_t* indices);

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

    /* Initialize */
    memset(&test,0,sizeof(test));
    memset(listv,0,sizeof(listv));
    memset(ncrv,0,sizeof(ncrv));
    memset(slpv,0,sizeof(slpv));

    if((stat = ut_proj_init(argc, argv, &test))) goto done;
    
    if((stat = NCZ_compute_chunk_ranges(test.rank,test.slices,test.chunklen,ncrv)))
	goto done;

    printf("ChunkRanges(%d):\n",test.rank);
    for(r=0;r<test.rank;r++) {
        printf("[%d] %s\n",r,nczprint_chunkrange(ncrv[r]));
    }

    if((stat = NCZ_chunkindexodom(test.rank, ncrv, odomprinter)))
	goto done;

done:
    if(stat)
	nc_strerror(stat);
    return (stat ? 1 : 0);    
}

static void
odomprinter(size_t rank, size64_t* indices)
{
    int r;
    printf("[");
    for(r=0;r<rank;r++)
        printf("%s%llu",(r==0?"":","),indices[r]);
    printf("]\n");
}
