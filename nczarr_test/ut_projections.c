/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#include "zincludes.h"
#include "ut_projtest.h"

void ut_chunk_test(int sort, ...);

/**
Test computation of applying a slice to a sequence of chunks
*/

int
main(int argc, char** argv)
{
    int i,r,stat = NC_NOERR;
    Vardef* var = NULL;
    struct Common common;
    NCZSliceProjections slpv[NC_MAX_VAR_DIMS];
    NCZChunkRange ncrv[NC_MAX_VAR_DIMS];
    char* tmp = NULL;
    
    /* Initialize */
    memset(&slpv,0,sizeof(slpv));
    memset(&common,0,sizeof(common));

    if((stat = ut_init(argc, argv, &options))) goto done;

    /* printer off for these tests */
    zutest.tests = 0;
    zutest.print = NULL;

    var = nclistget(options.vardefs,0);

    tmp=nczprint_slicesx(var->rank,options.slices,1);
    printf("Slices: %s\n",tmp);
    nullfree(tmp);

    /* Compute chunk ranges */
    if((stat = NCZ_compute_chunk_ranges(var->rank,options.slices,var->chunksizes,ncrv)))
	goto done;

    if((stat=NCZ_compute_all_slice_projections(
	var->rank,
        options.slices,
        var->dimsizes,
        var->chunksizes,
        ncrv,
	slpv))) goto done;

    /* Dump Results */
    for(r=0;r<var->rank;r++) {
	NCZSliceProjections* slp = &slpv[r];
	char *sr, *sl;
        if(r != slp->r) usage(NC_EINTERNAL);
	sr = nczprint_chunkrange(slp->range);
	sl = nczprint_slice(options.slices[r]);
        printf("%s %s\n",sr,sl);
	nullfree(sr); nullfree(sl);
        for(i=0;i<slp->count;i++) {
            NCZProjection* proj = &slp->projections[i];
	    tmp = nczprint_projection(*proj);
            printf("[%d] %s\n",i,tmp);
	    nullfree(tmp);
	}
    }
    /* Cleanup */
    NCZ_clearsliceprojections(var->rank,slpv);

#if 0
    /* Compute corresponding slice projections  */
    for(r=0;r<var->rank;r++) {
        if((stat = NCZ_compute_per_slice_projections(
			r,
			&options.slices[r],
			&ncrv[r],
			var->dimsizes[r],
			var->chunksizes[r],
			&slpv[r]))) goto done;
    }

    /* Dump Results */
    for(r=0;r<var->rank;r++) {
	NCZSliceProjections* slp = &slpv[r];
	char *sr, *sl;
        if(r != slp->r) usage(NC_EINTERNAL);
	sr = nczprint_chunkrange(slp->range);
	sl = nczprint_slice(options.slices[r]);
        printf("%s %s\n",sr,sl);
	nullfree(sr); nullfree(sl);
        for(i=0;i<slp->count;i++) {
            NCZProjection* proj = &slp->projections[i];
	    tmp = nczprint_projection(*proj);
            printf("[%d] %s\n",i,tmp);
	    nullfree(tmp);
	}
    }
    /* Cleanup */
    NCZ_clearsliceprojections(var->rank,slpv);
#endif

done:
    fflush(stdout);
    if(stat) usage(stat);
    return  0;
}

void
ut_chunk_test(int sort,...)
{
    int i;
    va_list ap;    
#if 0
    struct Common* common = NULL;    
#endif
    size64_t rank; /* variable rank */
    NCZSlice* slices = NULL; /* the complete set of slices |slices| == R*/
    size64_t* chunksizes = NULL; /* the chunk length corresponding to the dimensions */
    NCZChunkRange* ranges = NULL; /* computed chunk ranges */

    va_start(ap,sort);
    
    switch (sort) {
    default: break; /* ignore */
    case UTEST_RANGE: /* () */
	rank = va_arg(ap,size64_t);
        slices = va_arg(ap,NCZSlice*);
	chunksizes = va_arg(ap,size64_t*);
        ranges = va_arg(ap,NCZChunkRange*);
        printf("Chunksizes: %s\n",nczprint_vector(rank,chunksizes));
        printf("Slices: ");
	for(i=0;i<rank;i++)
	    printf(" %s",nczprint_slicesx(rank,slices,1));
	printf("\n");
        printf("Ranges: ");
	for(i=0;i<rank;i++)
	    printf(" %s",nczprint_chunkrange(ranges[i]));
	printf("\n");
	break;
    }
    va_end(ap);
}

