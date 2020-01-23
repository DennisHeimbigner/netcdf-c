/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#ifndef ZCHUNKING_H
#define ZCHUNKING_H

/* Define the position of a chunk WRT a dimension
   in terms of absolute position */
typedef struct NCZChunkOffset {
    size64_t start;
    size64_t stop;
} NCZChunkPos;

/* Complete Chunk position as cross product of the per-dimension positions */
typedef struct NCZChunkPos {
    size64_t rank;
    NCZChunkPos offsets[NC_MAX_VAR_DIM];
} NCZChunkPos;

/* A per-dimension slice for the incoming hyperslab */
typedef struct NCZSlice {
    size64_t start;
    size64_t stop;
    size64_t stride;
} NCZSlice;

/* Complete Set of slices of the hyperslab */
typedef struct NCZChunkPos {
    size64_t rank;
    NCZSlice slices[NC_MAX_VAR_DIM];
} NCZChunkPos;

typedef struct NCProjection {
    size64_t start;  /* first position to be touched in the chunk */
    size64_t last;   /* position of last value touched */
    size64_t limit;  /* last accessible position in the chunk aka stop*/
    size64_t len;    /* Not last place touched, but the offset of last place
                        that could be touched aka limit*/
    size64_t iopos;  /* start point in the data memory to access the data */
    NCZSlice slice; /* slice relative to this chunk */
} NCZProjection;

/* Track the set of chunks touched by the current hyperslab */
/* This will be iterated over to construct a set of proections
   for each chunk in turn.
*/
typedef struct NCZChunkSeq {
    size64_t nchunks; /* number of chunks touched by this slice index */
    NCZChunkpos* chunks; /* Chunks touched by this hyperslab */
} struct NCZChunkSeq;

/* Set of Projections of each slice for a given Chunk */
typedef struct NCZProjectionSet {
    size64_t chunkindex; /* Which chunk is this for? */
    NCZProjection* projections; /* Vector of projections
                                derived from the original slice when
				intersected across the chunk
                                |projections| == R
                             */
    size64_t count;   /*total number of io items defined by this slice index */
} NCZProjectionSet;

typedef struct NCZOdometer {
  size64_t R; // rank
  size64_t start[NC_MAX_VAR_DIMS];
  size64_t stop[NC_MAX_VAR_DIMS];
  size64_t stride[NC_MAX_VAR_DIMS];
  size64_t index[NC_MAX_VAR_DIMS]; /* current value of the odometer*/
} NCZOdometer;

/* From odom.c */
extern NCZOdometer* nczodom_new(size64_t, const size64_t*, const size64_t*, const size64_t*);
extern NCZOdometer* nczodom_fromslices(size64_t rank, const NCZSlice* slices);
extern int nczodom_more(NCZOdometer*);
extern int nczodom_next(NCZOdometer*);
extern size64_t* nczodom_indices(NCZOdometer*);

/* From sliceindices.c */
extern int ncz_compute_all_slice_projections(size64_t R, const NCZSlice*, const size64_t*, const size64_t*, NCZSliceIndex**);

/* From allchunks.c */
extern int nczhunking_init(void);
extern int ncz_evaluateslices(NC_FILE_INFO_T*, NC_VAR_INFO_T*, NCZSlice*, void*, size64_t);

/* From zchunkio.c */
extern int ncz_buildchunkkey(int R, size64_t* chunkindices, char** keyp);

/* From zput.c */
extern int ncz_put_chunk(NC_FILE_INFO_T*,NC_VAR_INFO_T*,?);

#endif /*ZCHUNKING_H*/
