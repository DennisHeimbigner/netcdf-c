/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#ifndef ZCACHE_H
#define ZCACHE_H

typedef struct NCZCacheEntry {
    int modified;
    const NC_VAR_INFO_T* var; /* backlink */
    size64_t* offset;
    size64_t indices[NC_MAX_VAR_DIMS];
    size64_t size; /* |data| */
    void* data;
} NCZCacheEntry;

typedef struct NCZChunkCache {
    NC_FILE_INFO_T* file; /* back link */
    size_t maxsize;
    NC_hashmap* entries; /* NC_hashmap<NCZCacheEntry*>*/
} NCZChunkCache;

/**************************************************/

extern int NCZ_create_chunk_cache(NC_FILE_INFO_T* file, size_t maxsize, NCZChunkCache** cachep);
extern void NCZ_free_chunk_cache(NCZChunkCache* cache);
extern int NCZ_read_cache_chunk(NCZChunkCache* cache, const NC_VAR_INFO_T* var, const size64_t* indices, size64_t* datalenp, void** datap);
extern int ncz_flush_chunk_cache(NCZChunkCache* cache);
extern int ncz_chunk_cache_modified(NCZChunkCache* cache, const NC_VAR_INFO_T* var, const size64_t* indices);

#endif /*ZCACHE_H*/
