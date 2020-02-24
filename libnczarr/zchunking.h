/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#ifndef ZCHUNKING_H
#define ZCHUNKING_H


/* Define the intersecting set of chunks for a slice
   in terms of chunk indices (not absolute positions)
*/
typedef struct NCZChunkRange {
    size64_t start; /* index, not absolute */
    size64_t stop;
} NCZChunkRange;

/* A per-dimension slice for the incoming hyperslab */
typedef struct NCZSlice {
    size64_t start;
    size64_t stop; /* start + (count*stride) */
    size64_t stride;
} NCZSlice;

#if 0
/* Complete Chunk position as cross product of the per-dimension positions */
typedef struct NCZChunkPos {
    size64_t rank;
    NCZChunkPos offsets[NC_MAX_VAR_DIM];
} NCZChunkPos;

/* Complete Set of slices of the hyperslab */
typedef struct NCZChunkPos {
    size64_t rank;
    NCZSlice slices[NC_MAX_VAR_DIM];
} NCZChunkPos;
#endif

typedef struct NCProjection {
    size64_t chunkindex; /* which chunk are we projecting */
    size64_t first;  /* absolute first position to be touched in this chunk */
    size64_t last;   /* absolute position of last value touched */
    size64_t len;    /* Not last place touched, but the offset of last place
                        that could be touched */
    size64_t limit;  /* Actual limit of chunk = min(limit,dimlen) */
    size64_t iopos;    /* start point in the data memory to access the data */
    size64_t iocount;  /* no. of I/O items */
    NCZSlice slice;  /* slice relative to this chunk */
} NCZProjection;

/* Set of Projections for a slice */
typedef struct NCZSliceProjections {
    NCZChunkRange range;
    NClist* projections; /* List<NCZProjection*> Vector of projections
                                derived from the original slice when
				intersected across the chunk
                             */
} NCZSliceProjections;

#if 0
/* Track the set of chunks touched by the current hyperslab */
/* This will be iterated over to construct a set of proections
   for each chunk in turn.
*/
typedef struct NCZChunkSeq {
    size64_t nchunks; /* number of chunks touched by this slice index */
    NCZChunkpos* chunks; /* Chunks touched by this hyperslab */
} struct NCZChunkSeq;
#endif

typedef struct NCZOdometer {
  size64_t rank; /*rank */
  NCZSlice slices[NC_MAX_VAR_DIMS];
  size64_t index[NC_MAX_VAR_DIMS]; /* current value of the odometer*/
} NCZOdometer;

/**************************************************/
/* From zsliceindices.c */
extern int NCZ_compute_all_slice_projections(
	size64_t rank,
        const NCZSlice* slices,
	const size64_t* dimlen,
	const size64_t* chunklen,
        NCZSliceProjections* results);

/* From zodom.c */
extern NCZOdometer* nczodom_new(size_t, const size_t*, const size_t*, const size_t*);
extern NCZOdometer* nczodom_fromslices(size_t rank, const NCZSlice* slices);
extern int nczodom_more(NCZOdometer*);
extern int nczodom_next(NCZOdometer*);
extern size64_t* nczodom_indices(NCZOdometer*);

#if 0
/* From allchunks.c */
extern int nczhunking_init(void);
extern int ncz_evaluateslices(NC_FILE_INFO_T*, NC_VAR_INFO_T*, NCZSlice*, void*, size64_t);
#endif

/* Expose functions for unit tests */
extern int NCZ_compute_chunk_ranges(size64_t, const NCZSlice*, const size64_t*, NCZChunkRange*);
extern int NCZ_compute_projection(size64_t dimlen, size64_t chunklen, size64_t chunkindex, const NCZSlice* slice, NClist* projections);
extern int NCZ_compute_per_slice_projections(const NCZSlice* slice, const NCZChunkRange* range, size64_t dimlen, size64_t chunklen, NCZSliceProjections* slp);
extern int NCZ_compute_all_slice_projections(size64_t rank, const NCZSlice* slices, const size64_t* dimlen, const size64_t* chunklen, NCZSliceProjections* results);
extern int NCZ_chunkindexodom(size_t rank, const NCZChunkRange* ranges, void (*printer)(size_t rank, size64_t* indices));


#endif /*ZCHUNKING_H*/
