/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#include "zprojtest.h"

/**
Test walking all combinations of chunks covered by a slice.
*/

void rangeodomprinter(int rank, void* indices);

int
main(int argc, char** argv)
{
    int stat = NC_NOERR;
    int r;
    ProjTest test;
    NClist* listv[NC_MAX_VAR_DIMS];
    NCZChunkRange ncrv[NC_MAX_VAR_DIMS];
    NCZSliceProjections slpv[NC_MAX_VAR_DIMS];
    NCZ_UT_PRINTER printer;

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
    printer.printer = rangeodomprinter;
    nczprinter = &printer;
    if((stat = NCZ_chunkindexodom(test.rank, ncrv, NULL)))
	goto done;

done:
    if(stat)
	nc_strerror(stat);
    return (stat ? 1 : 0);    
}

void
rangeodomprinter(int rank, void* data)
{
	int r;
        size64_t* indices = data;
        printf("[");
        for(r=0;r<rank;r++)
            printf("%s%llu",(r==0?"":","),indices[r]);
        printf("]\n");
}
