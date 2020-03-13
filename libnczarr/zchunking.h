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
    size64_t len; /* full dimension length */
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
    int id;
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
    size64_t r; /* 0<=r<rank */
    NCZChunkRange range; /* Chunk ranges covered by this set of projections */
    size_t count; /* |projections| == (range.stop - range.start) */
    NCZProjection* projections; /* Vector of projections derived from the
                                   original slice when intersected across
				   the chunk */
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
  size64_t max[NC_MAX_VAR_DIMS]; /* max size of ith index */
  size64_t index[NC_MAX_VAR_DIMS]; /* current value of the odometer*/
} NCZOdometer;

/* Combine some values to simplify internal argument lists */
struct Common {
    NC_FILE_INFO_T* file;
    NC_VAR_INFO_T* var;
    int reading; /* 1=> read, 0 => write */
    size_t rank;
    size64_t dimlens[NC_MAX_VAR_DIMS];
    size64_t chunklens[NC_MAX_VAR_DIMS];
    void* memory;
    size_t typesize;
    int swap; /* var->format_info_file->native_endianness == var->endianness */
    size64_t shape[NC_MAX_VAR_DIMS]; /* shape of the output hyperslab */
    NCZSliceProjections* allprojections;
};

/**************************************************/
/* From zchunking.c */
extern int NCZ_compute_chunk_ranges(size64_t, const NCZSlice*, const size64_t*, NCZChunkRange* ncr);
extern int NCZ_compute_projections(size64_t dimlen, size64_t chunklen, size64_t chunkindex, const NCZSlice* slice, size_t n, NCZProjection* projections);
extern int NCZ_compute_per_slice_projections(size_t rank, const NCZSlice*, const NCZChunkRange*, size64_t dimlen, size64_t chunklen, NCZSliceProjections* slp);
extern int NCZ_compute_all_slice_projections(size64_t rank, const NCZSlice* slices, const size64_t* dimlen, const size64_t* chunklen, const NCZChunkRange*, NCZSliceProjections*);

/* From zodom.c */
extern NCZOdometer* nczodom_new(size_t, const size64_t*, const size64_t*, const size64_t*, const size64_t*);
extern NCZOdometer* nczodom_fromslices(size_t rank, const NCZSlice* slices);
extern int nczodom_more(NCZOdometer*);
extern int nczodom_next(NCZOdometer*);
extern size64_t* nczodom_indices(NCZOdometer*);
extern size64_t nczodom_offset(NCZOdometer*);
extern void nczodom_reset(NCZOdometer* odom);
extern void nczodom_free(NCZOdometer*);


/* From zwalk.c */
extern int ncz_chunking_init(void);
extern int NCZ_transferslice(NC_VAR_INFO_T* var, int reading,
		  size64_t* start, size64_t* count, size64_t* stride,
		  void* memory, size_t typesize);
extern size64_t NCZ_computelinearoffset(size_t, const size64_t*, const size64_t*);

/* Special entry points for unit testing */
struct Common;
extern int NCZ_chunkindexodom(size_t rank, const NCZChunkRange* ranges, size64_t*, NCZOdometer** odom);
extern int NCZ_projectslices(size64_t* dimlens,
		  size64_t* chunklens,
		  NCZSlice* slices,
		  struct Common*, NCZOdometer**);

#define floordiv(x,y) ((x) / (y))

#define ceildiv(x,y) (((x) % (y)) == 0 ? ((x) / (y)) : (((x) / (y)) + 1))

#endif /*ZCHUNKING_H*/
