/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#ifndef ZCACHE_H
#define ZCACHE_H

typedef struct NCZCacheEntry {
    int modified;
    size64_t* offset;
    size64_t indices[NC_MAX_VAR_DIMS];
    void* data;
} NCZCacheEntry;

typedef struct NCZChunkCache {
    const NC_VAR_INFO_T* var; /* backlink */
    size64_t entrysize; /* all entries assumed to have same size */
    NC_hashmap* entries; /* NC_hashmap<NCZCacheEntry*>*/
} NCZChunkCache;

/**************************************************/

extern int NCZ_create_chunk_cache(NC_VAR_INFO_T* var, size64_t, NCZChunkCache** cachep);
extern void NCZ_free_chunk_cache(NCZChunkCache* cache);
extern int NCZ_read_cache_chunk(NCZChunkCache* cache, const size64_t* indices, void** datap);
extern int NCZ_flush_chunk_cache(NCZChunkCache* cache);
extern int NCZ_chunk_cache_modified(NCZChunkCache* cache, const size64_t* indices);
extern size64_t NCZ_cache_entrysize(NCZChunkCache* cache);
extern size64_t NCZ_cache_size(NCZChunkCache* cache);

#endif /*ZCACHE_H*/
