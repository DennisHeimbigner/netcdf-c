/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/
#include "zincludes.h"

/* Forward */
static int compute_intersection(const NCZSlice* slice, const size64_t chunklen, NCZChunkRange* range);
static size64_t floordiv(size64_t x, size64_t y);
static size64_t ceildiv(size64_t x, size64_t y);

/**************************************************/
/* Goal:create a vector of chunk ranges: one for each slice in
   the top-level input. For each slice, compute the index (not
   absolute position) of the first chunk that intersects the slice
   and the index of the last chunk that intersects the slice.
   In practice, the count = last - first + 1 is stored instead of the last index.
*/
int
NCZ_compute_chunk_ranges(
	size64_t rank, /* variable rank */
        const NCZSlice* slices, /* the complete set of slices |slices| == R*/
	const size64_t* chunklen, /* the chunk length corresponding to the dimensions */
        NCZChunkRange* ncr)
{
    int stat = NC_NOERR;
    int i;

    for(i=0;i<rank;i++) {
        NCZChunkRange* range = &ncr[i];
	if((stat = compute_intersection(&slices[i],chunklen[i],range)))
	    goto done;
    }

done:
    return stat;
}

static int
compute_intersection(
        const NCZSlice* slice,
	const size64_t chunklen,
        NCZChunkRange* range)
{
    range->start = floordiv(slice->start, chunklen);
    range->stop = ceildiv(slice->stop, chunklen);
    return NC_NOERR;
}

/**
Compute the projection of a slice as applied to n'th chunk.
This is somewhat complex because for the first projection, the
start is the slice start, but after that, we have to take into
account that for a non-one stride, the start point in a
projection may be offset by some value in the range of
0..(slice.stride-1).
*/
int
NCZ_compute_projection(size64_t dimlen, size64_t chunklen, size64_t chunkindex, const NCZSlice* slice, NClist* projections)
{
    int stat = NC_NOERR;
    int n;
    size64_t offset,count;
    NCZProjection* projection;
    
    n = nclistlength(projections); /* Which projection */

    if((projection = calloc(1,sizeof(NCZProjection))) == NULL)
	{stat = NC_ENOMEM; goto done;}

    projection->chunkindex = chunkindex;
    offset = chunklen * chunkindex;

    /* Actual limit of the n'th touched chunk, taking
       dimlen and stride->stop into account. */
    projection->limit = (chunkindex + 1) * chunklen;
    if(projection->limit > dimlen) projection->limit = dimlen;
    if(projection->limit > slice->stop) projection->limit = slice->stop;

    /* Len is not last place touched,
       but the last place that could be touched */
    projection->len = projection->limit - offset;

    if(n == 0) {
	/*initial case: original slice start is in 1st projection */
	projection->first = slice->start + offset; /* absolute */
    } else { /* n > 0 */
	NCZProjection* prev = nclistget(projections,n-1);
	/* prevunused is the amount unused at end of the previous chunk.
	   => we need to skip (slice->stride-prevunused) in this chunk */
        /* Compute limit of previous chunk */
	size64_t prevunused = prev->limit - prev->last;
	projection->first = offset + (slice->stride - prevunused); /* absolute */
    }
    /* Compute number of places touched in this chunk */
    count  = ceildiv((projection->limit - projection->first), slice->stride);
    /* Last place to be touched */
    projection->last = projection->first + (slice->stride * (count - 1));

    /* Compute the slice relative to this chunk.
       Recall the possibility that start+stride >= projection->limit */
    projection->slice.start = (projection->first - offset);
    projection->slice.stop = projection->slice.start + (slice->stride * count) + 1;
    if(slice->stop > projection->limit) {
        projection->slice.stop = projection->len;
    }
    projection->slice.stride = slice->stride;

    /* compute the I/O position: the "location" in the memory
       array to read/write items */
    projection->iopos = ceildiv(offset - slice->start, slice->stride);
    /* And number of I/O items */
    projection->iocount = count;

    nclistpush(projections,projection);
    projection = NULL;
done:
    return stat;
}

/* Goal:
Create a vector of projections wrt a slice and a sequence of chunks.
*/

int
NCZ_compute_per_slice_projections(
        const NCZSlice* slice, /* the slice for which projections are computed */
	const NCZChunkRange* range, /* range */
	size64_t dimlen, /* the dimension length for r'th dimension */
	size64_t chunklen, /* the chunk length corresponding to the dimension */
	NCZSliceProjections* slp)
{
    int stat = NC_NOERR;
    size64_t index,slicecount;
    NClist* nsplist = NULL; /* List<NCZSliceProjection>*/

    /* Compute the total number of output items defined by this slice
           (equivalent to count as used by nc_get_vars) */
    slicecount = ceildiv((slice->stop - slice->start), slice->stride);
    if(slicecount < 0) slicecount = 0;

    /* Iterate over each chunk that intersects slice to produce projection */
    for(index=range->start;index<range->stop;index++) {
	NCZProjection* projection = NULL;
	if((projection = calloc(1,sizeof(NCZProjection)))==NULL)
	    {stat = NC_ENOMEM; goto done;}
	nclistpush(nsplist,projection);
	if((stat = NCZ_compute_projection(dimlen, chunklen, index, slice, nsplist)))
	    goto done;
    }

    if(slp) {
	/* Fill in the Slice Projections to return */
	slp->range = *range;
	slp->projections = nsplist;
	nsplist = NULL;
    }    

done:
    nclistfreeall(nsplist);
    return stat;
}

/* Goal:create a vector of SliceProjection instances: one for each
    slice in the top-level input. For each slice, compute a set
    of projections from it wrt a dimension and a chunk size
    associated with that dimension.
*/
int
NCZ_compute_all_slice_projections(
	size64_t rank, /* variable rank */
        const NCZSlice* slices, /* the complete set of slices |slices| == R*/
	const size64_t* dimlen, /* the dimension lengths associated with a variable */
	const size64_t* chunklen, /* the chunk length corresponding to the dimensions */
        NCZSliceProjections* results)
{
    int stat = NC_NOERR;
    size64_t r; 
    NCZChunkRange ranges[NC_MAX_VAR_DIMS];

    /* Compute the chunk ranges for each chunk in a given dim */
    memset(ranges,0,sizeof(ranges));
    if((stat = NCZ_compute_chunk_ranges(rank,slices,chunklen,ranges)))
	goto done;

    for(r=0;r<rank;r++) {
	/* Compute each of the rank SliceProjections instances */
	NCZSliceProjections* slp = &results[r];
        if((stat=NCZ_compute_per_slice_projections(
					&slices[r],
					&ranges[r],
					dimlen[r],
					chunklen[r],
                                        slp))) goto done;
    }

done:
    return stat;
}

/**************************************************/
/* Utilities */
    
static size64_t
floordiv(size64_t x, size64_t y)
{
      return x/y;
}

static size64_t
ceildiv(size64_t x, size64_t y)
{
      size64_t div = x/y;
      if((x % y) != 0) div++;
      return div;
}

#if 0
static void
clearsliceprojection(NCZSliceProjections* slp)
{
    if(slp != NULL)
	nclistfreeall(slp->projections);
}

static void
clearallprojections(NCZAllProjections* nap)
{
    if(nap != NULL) {
	int i;
	for(i=0;i<nap->rank;i++) 
	    nclistfreeall(&nap->allprojections[i].projections);
    }
}
#endif

