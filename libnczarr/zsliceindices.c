/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/
#include "zincludes.h"

/* Forward */

static int
compute_intersection(
        const NCZSlice* slice,
	const size64_t chunklen,
        NCZChunkRange* range)
{
    size64_t stop;
    range->start = floordiv(slice->start, chunklen);
    stop = ceildiv(slice->stop, chunklen);
    range->count = (stop - range->start) + 1;
    return NC_NOERR;
}

/* Goal:create a vector of chunk ranges: one for each slice in
   the top-level input. For each slice, compute the index (not
   absolute position) of the first chunk that intersects the slice
   and the index of the last chunk that intersects the slice.
   In practice, the count = last - first is stored instead of the last index.
*/
static int
compute_chunk_intersections(
	size64_t rank, /* variable rank */
        const NClist* slices, /* the complete set of slices |slices| == R*/
	const size64_t* chunklen, /* the chunk length corresponding to the dimensions */
        NClist* results) /** List<NCZChunkRange*> */
{
    int stat = NC_NOERR;
    for(i=0;i<rank;i++) {
        NCZChunkRange* range = NULL;
	if((range = calloc(1,sizeof(NCZChunkRange))) == NULL)
	    {stat = NC_ENOMEM; goto done;}
	if((stat = compute_intersection((NCZSlice*)nclistget(slices,i),chunklen[i],range)))
	    goto done;
	nclistpush(results,range);
	range = NULL;
    }
done:
    if(range) free(range);
    return stat;
}

/* Goal:
Create a set of projections wrt a slice.
*/

static int
compute_per_slice_projections(
	size64_t rank, /* Which dimension/slice are we computing? */
        NCZSlice* slice, /* the slice for which projections are computed */
	size64_t dimlen, /* the dimension length for r'th dimension */
	size64_t chunklen, /* the chunk length corresponding to the dimension */
	NCZSliceProjections** nspp)
{
    int stat = NC_NOERR;
    NCZSliceProjections* nsp = NULL;
    NCZProjection* projection = NULL;
    size64_t count, nchunks, chunk0, chunkn;

    if((nsp = calloc(1,sizeof(NCZSliceProjections))) == NULL)
	{stat = NC_ENOMEM; goto done;}

    if((stat = compute_chunk_intersections(rank,slices

    nsp->chunkindex0 = 
    nsp->offset =
    nsp->projections = nclistnew();


    /* Compute the total number of output items defined by this slice
       (equivalent to count as used by nc_get_vars) */
    count = ceildiv((slice.stop - slice.start), slice.stride);
    if(count < 0) count = 0;

    /* index (in 0..nchunks-1) of the first chunk touched by the slice */
    chunk0 = floordiv(slice.start,chunklen);
    /* index (in 0..nchunks-1) of the last chunk touched by the slice */
    chunkn = ceildiv(slice.stop,chunklen);

    /* Compute number of chunks covered */
    nchunks = (chunkn - chunk0);

    /* compute the 

    /* For each touched chunk index we compute a
       projection specific to that index */
    for(n=0;n<nchunks;n++) {
	if((project = calloc(1,sizeof(NCZProjection))) == NULL)
	    {stat = NC_ENOMEM; goto done;}
        projection->chunkindex = chunk0+n; /* index, not the length */

        /* Compute the projection slice start for the n'th projection chunk.
           This is somewhat complex because for the first
           projection, the start is the slice start, but after that,
           we have to take into account that for a non-one stride,
           the start point in a projection may be offset by some
           value in the range of 0..(slice.stride-1)
	*/
        if(n == 0) {/*initial case: original slice start is in 1st projection */
	    projection->slice.start = slice->start - projection->offset; /* relative to chunk0 */
	    /* Compute the last position touched; take into account the
               possibility that start point+stride >= limit */
	    projection.last = slice.start + slice.stride * floordiv((projection.limit - 1) - slice.start,slice.stride);
        } else { /* n > 0 */
	    /* prevunused is the amount unused at end of the previous chunk.
	       => we need to skip (slice.stride-prevunused) in this chunk */
	    int prevunused = projections[n-1].limit - projections[n-1].last;
	    projection.slice.start = slice.stride - prevunused;
	}
	/* In all cases */
	projection.slice.stride = slice.stride;

	/* Len is not last place touched,
           but the last place that could be touched */
        projection.len = projection.limit - projection.offset;

        if(slice.stop > projection.limit) {
            projection.slice.stop = projection.len;
        } else { /* slice.stop <= projections.limit */
	    /* selection ends within current chunk */
            projection.slice.stop = slice.stop - projection.offset;
        }

       /* compute the output position: the "location" in the output;
          array to start storing items; again, per slice, not global. */
       projection.outpos = ceildiv(projection.offset - slice.start, slice.stride);

       /* Append to projections vector */
       projections[n] = projection;
    }

    /* Fill in the SliceIndex to return */
    sliceindex.chunk0 = chunk0;
    sliceindex.nchunks = nchunks;
    sliceindex.count = count;
    sliceindex.projections = projections;
    
    if(results) *results = sliceindex;
done:
    return stat;
}

/* Goal: create a vector of chunk ranges: one for each slice in
   the top-level input. For each slice, compute the index (not
   absolute position) of the first chunk that intersects the slice
   and the index of the last chunk that intersects the slice.
   In practice, the count = last - first is stored instead of the last index.
*/
static int
ncz_compute_chunk_intersections(
	size64_t rank, /* variable rank */
        const NClist* slices, /* the complete set of slices |slices| == R*/
	const size64_t* chunklen, /* the chunk length corresponding to the dimensions */
        NClist* results)
{
    int stat = NC_NOERR;
    for(i=0;i<rank;i++) {
        NCZChunkRange* range = NULL;
	if((range = calloc(1,sizeof(NCZChunkRange))) == NULL)
	    {stat = NC_ENOMEM; goto done;}
	if((stat = compute_intersection((NCZSlice*)nclistget(slices,i),chunklen[i],range)))
	    goto done;
	nclistpush(results,range);
	range = NULL;
    }
done:
    if(range) free(range);
    return stat;
}

/* Goal:create a vector of SliceIndex instances: one for each
    slice in the top-level input. For each slice, compute a set
    of projections from it wrt a dimension and a chunk size
    associated with that dimension.
*/
int
ncz_compute_all_slice_projections(
	size64_t R, /* variable rank */
        const NCZSlice* slices, /* the complete set of slices |slices| == R*/
	const size64_t* dimlen, /* the dimension lengths associated with a variable */
	const size64_t* chunklen, /* the chunk length corresponding to the dimensions */
        NCZSliceIndex** results)
{
    int stat = NC_NOERR;
    size64_t r;
    NCZSliceIndex* sliceindices = NULL;

    if(results) *results = NULL;

    if((sliceindices = calloc(R,sizeof(NCZSliceIndex))) == NULL)
	{stat = NC_ENOMEM; goto done;}

    for(r=0;r<R;r++) {
	/* Compute each of the R SliceIndex instances */
	NCZSliceIndex sliceindex;
        if((compute_per_slice_projection(r,
					slices[r],
					dimlen[r],
					chunklen[r],
                                        &sliceindex))) goto done;
	
	sliceindices[r] = sliceindex;
    }
    if(results) *results = sliceindices;
done:
    return stat;
}

 
/* Goal:create a single SliceIndex instance. This is the core
   of the whole algorithm.
*/

static int
compute_per_slice_projection(
	size64_t r, /* Which dimension/slice are we computing? */
        NCZSlice slice, /* the slice for which projections are computed */
	size64_t dimlen, /* the dimension length for r'th dimension */
	size64_t chunklen, /* the chunk length corresponding to the dimension */
	NCZSliceIndex* results)
{
    int stat = NC_NOERR;
    int n;
    NCZSliceIndex sliceindex;
    size64_t count, nchunks, chunk0, chunkn;
    NCZProjection* projections; /* Length determined below */

    if(results) memset(results,0,sizeof(NCZSliceIndex));

    /* Compute the total number of output items defined by this slice
       (equivalent to count as used by nc_get_vars) */
    count = ceildiv((slice.stop - slice.start), slice.stride);
    if(count < 0) count = 0;

    /* Compute number of chunks touched by this slice,
       also the number of projections to compute */
    nchunks = ceildiv(dimlen, chunklen);

    /* index (in 0..nchunks-1) of the first chunk touched by the slice */
    chunk0 = floordiv(slice.start,chunklen);
    /* index (in 0..nchunks-1) of the last chunk touched by the slice */
    chunkn = ceildiv(slice.stop,chunklen);
    assert(chunkn == (chunk0 + (nchunks)));

    /* Create the vector of projections */
    if((projections = calloc(nchunks,sizeof(NCZProjection))) == NULL)
	{stat = NC_ENOMEM; goto done;}

    /* For each touched chunk index we compute a
       projection specific to that index */
    for(n=0;n<nchunks;n++) {
	NCZProjection projection; /* Instance being computed */

        projection.chunkindex = chunk0+n; /* index, not the length */

        /* Distance from start of chunk0 to this chunk. */
        projection.offset = chunklen * n;

        /* Actual limit of the n'th touched chunk,
           which is end of this chunk but no greater than dimlen */
        projection.limit = (n + 1) * chunklen;
	if(projection.limit > dimlen) projection.limit = dimlen;

        /* Compute the projection slice start for the n'th projection chunk.
           This is somewhat complex because for the first
           projection, the start is the slice start, but after that,
           we have to take into account that for a non-one stride,
           the start point in a projection may be offset by some
           value in the range of 0..(slice.stride-1)
	*/
        if(n == 0) {/*initial case: original slice start is in 1st projection */
	    projection.slice.start = slice.start - projection.offset; /* relative to chunk0 */
	    /* Compute the last position touched; take into account the
               possibility that start point+stride >= limit */
	    projection.last = slice.start + slice.stride * floordiv((projection.limit - 1) - slice.start,slice.stride);
        } else { /* n > 0 */
	    /* prevunused is the amount unused at end of the previous chunk.
	       => we need to skip (slice.stride-prevunused) in this chunk */
	    int prevunused = projections[n-1].limit - projections[n-1].last;
	    projection.slice.start = slice.stride - prevunused;
	}
	/* In all cases */
	projection.slice.stride = slice.stride;

	/* Len is not last place touched,
           but the last place that could be touched */
        projection.len = projection.limit - projection.offset;

        if(slice.stop > projection.limit) {
            projection.slice.stop = projection.len;
        } else { /* slice.stop <= projections.limit */
	    /* selection ends within current chunk */
            projection.slice.stop = slice.stop - projection.offset;
        }

       /* compute the output position: the "location" in the output;
          array to start storing items; again, per slice, not global. */
       projection.outpos = ceildiv(projection.offset - slice.start, slice.stride);

       /* Append to projections vector */
       projections[n] = projection;
    }

    /* Fill in the SliceIndex to return */
    sliceindex.chunk0 = chunk0;
    sliceindex.nchunks = nchunks;
    sliceindex.count = count;
    sliceindex.projections = projections;
    
    if(results) *results = sliceindex;
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
      if(div*y != x) div++;
      return div;
}

static void
freeslicelist(NClist* slices)
{
    int i;
    for(i=0;i<nclistlength(slices);i++)
	free(nclistget(slices,i);
    nclistfree(slices);
}

static void
freechunkrangelist(NClist* ranges)
{
    int i;
    for(i=0;i<nclistlength(ranges);i++)
	free(nclistget(ranges,i);
    nclistfree(ranges);
}
