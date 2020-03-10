/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#include "zprojtest.h"

/**
Test computation of applying slices to chunk ranges
*/

int
main(int argc, char** argv)
{
    int stat = NC_NOERR;
    int i,r;
    size_t n[NC_MAX_VAR_DIMS];
    ProjTest test;
    NCZProjection* listv[NC_MAX_VAR_DIMS];
    NCZChunkRange ncrv[NC_MAX_VAR_DIMS];

    /* Initialize */
    memset(&test,0,sizeof(test));
    memset(listv,0,sizeof(listv));
    memset(ncrv,0,sizeof(ncrv));

    if((stat = ut_proj_init(argc, argv, &test))) goto done;
    
    if((stat = NCZ_compute_chunk_ranges(test.rank,test.slices,test.chunklen,ncrv)))
	goto done;

    printf("ChunkRanges(%u):\n",(unsigned)test.rank);
    for(r=0;r<test.rank;r++) {
        printf("[%d] %s\n",r,nczprint_chunkrange(ncrv[r]));
    }

    for(r=0;r<test.rank;r++) {
	NCZChunkRange* ncr = &ncrv[r];
	n[r] = (ncr->stop - ncr->start);
        listv[r] = calloc(n[r],sizeof(NCZProjection));
        for(i=0;i<n[r];i++) {
            if((stat = NCZ_compute_projections(test.dimlen[r], test.chunklen[r],i+ncr->start, &test.slices[r], i, listv[r])))
	    goto done;
	}
    }

    /* Dump Results */
    for(r=0;r<test.rank;r++) {
	NCZChunkRange* ncr = &ncrv[r];
        NCZProjection* listr = listv[r];
        printf("|listv[%d]|: %lu\n",r,(unsigned long)n[r]);
        printf("%s %s\n",nczprint_chunkrange(*ncr), nczprint_slice(test.slices[r]));
        for(i=0;i<n[r];i++) {
            NCZProjection* proj = &listr[i];
            printf("[%d] %s\n",i,nczprint_projection(*proj));
	}
    }

done:
    if(stat)
	nc_strerror(stat);
    return (stat ? 1 : 0);    
}
