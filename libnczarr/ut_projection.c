/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#include "zprojtest.h"

/**
Test computation of applying a slice to a sequence of chunks
*/

int
main(int argc, char** argv)
{
    int stat = NC_NOERR;
    int i,r;
    ProjTest test;
    NClist* listv[NC_MAX_VAR_DIMS];
    NCZChunkRange ncrv[NC_MAX_VAR_DIMS];

    /* Initialize */
    memset(&test,0,sizeof(test));
    memset(listv,0,sizeof(listv));
    memset(ncrv,0,sizeof(ncrv));

    if((stat = ut_proj_init(argc, argv, &test))) goto done;
    
    assert(test.rank == 1);

    if((stat = NCZ_compute_chunk_ranges(test.rank,test.slices,test.chunklen,ncrv)))
	goto done;

    printf("ChunkRanges(%d):\n",test.rank);
    for(r=0;r<test.rank;r++) {
        printf("[%d] %s\n",r,nczprint_chunkrange(ncrv[r]));
    }

    for(r=0;r<test.rank;r++) {

	NCZChunkRange* ncr = &ncrv[r];
        listv[r] = nclistnew();
        for(i=ncr->start;i<ncr->stop;i++) {
            if((stat = NCZ_compute_projection(test.dimlen[r], test.chunklen[r],i, &test.slices[r], listv[r])))
	    goto done;
	}
        assert((ncr->stop - ncr->start) == nclistlength(listv[r]));
    }

    /* Dump Results */
    for(r=0;r<test.rank;r++) {
	NCZChunkRange* ncr = &ncrv[r];
        NClist* listr = listv[r];
        printf("|listv[%d]: %lu\n",r,(unsigned long)nclistlength(listv[r]));
        printf("%s %s\n",nczprint_chunkrange(ncr[r]), nczprint_slice(test.slices[r]));
        for(i=0;i<nclistlength(listr);i++) {
            NCZProjection* proj = (NCZProjection*)nclistget(listr,i);
            printf("[%d] %s\n",i,nczprint_projection(*proj));
	}
    }

done:
    if(stat)
	nc_strerror(stat);
    return (stat ? 1 : 0);    
}
