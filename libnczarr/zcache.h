/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#ifndef ZCACHE_H
#define ZCACHE_H

#include "zincludes.h"

/* Note in the following: the term "real"
   refers to the unfiltered/uncompressed data
   The term filtered refers to the result of running
   the real data through the filter chain. Note that the
   sizeof the filtered data might be larger than the size of
   the real data.
   The term "raw" is used to refer to the data on disk and
   it may be either real or filtered.
*/

/* This holds all the fields
   to support either impl of cache
*/

/* Opaque */
struct ChunkKey;
struct NC_VAR_INFO_T;

/* Header for all kinds of caches */
typedef struct NCZCache NCZCache;
struct NCZCache {
    int valid; /* 0 => following fields need to be re-calculated */
    struct NC_VAR_INFO_T* var; /* backlink */
    size64_t ndims; /* true ndims == var->ndims + scalar */
    size_t used; /* How much total space is being used */
    NCLRU* lru; /* all cache entries in lru order */
    char dimension_separator;
    struct {
	void (*free_chunk_cache)(NCZCache* cache);
	int (*read_cache_chunk)(NCZCache* cache, const size64_t* indices, void** datap);
	int (*flush_chunk_cache)(NCZCache* cache);
	size64_t (*cache_entrysize)(NCZCache* cache);
	size64_t (*cache_size)(NCZCache* cache);
	int (*buildchunkpath)(NCZCache* cache, const size64_t* chunkindices, struct ChunkKey* key);
	int (*ensure_fill_chunk)(NCZCache* cache);
	int (*reclaim_fill_chunk)(NCZCache* cache);
	int (*chunk_cache_modify)(NCZCache* cache, const size64_t* indices);
    } fcns;
};

/**************************************************/

#define FILTERED(cache) (nclistlength((NClist*)(cache)->var->filters))

extern int NCZ_set_var_chunk_cache(int ncid, int varid, size_t size, size_t nelems, float preemption);
extern int NCZ_adjust_var_cache(NC_VAR_INFO_T *var);

/* Dispatch wrappers */
extern void NCZ_free_chunk_cache(NCZCache* cache);
extern int NCZ_read_cache_chunk(NCZCache* cache, const size64_t* indices, void** datap);
extern int NCZ_flush_chunk_cache(NCZCache* cache);
extern size64_t NCZ_cache_entrysize(NCZCache* cache);
extern size64_t NCZ_cache_size(NCZCache* cache);
extern int NCZ_buildchunkpath(NCZCache* cache, const size64_t* chunkindices, struct ChunkKey* key);
extern int NCZ_ensure_fill_chunk(NCZCache* cache);
extern int NCZ_reclaim_fill_chunk(NCZCache* cache);
extern int NCZ_chunk_cache_modify(NCZCache* cache, const size64_t* indices);

#endif /*ZCACHE_H*/
