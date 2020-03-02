/* Copyright 2018, University Corporation for Atmospheric
 * Research. See COPYRIGHT file for copying and redistribution
 * conditions. */

/**
 * @file @internal The functions which control NCZ
 * caching. These caching controls allow the user to change the cache
 * sizes of ZARR before opening files.
 *
 * @author Dennis Heimbigner, Ed Hartnett
 */

#include "zincludes.h"
#include "zcache.h"

#define VERIFY 1
#define FLUSH 1

/* Forward */
static int buildchunkkey(size_t R, const size64_t* chunkindices, char** keyp);
static int get_chunk(const NC_VAR_INFO_T* var, const char* chunkkey, size64_t datalen, void* data);
static int put_chunk(const NC_VAR_INFO_T* var, const char* chunkkey, size64_t datalen, const void* data);

/**
 * Create a chunk cache object
 *
 * @param var containing var
 * @param size Size in bytes to set cache.
 * @param nelems Number of elements to hold in cache.
 * @param preemption Premption strategy (between 0 and 1).
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Bad preemption.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_create_chunk_cache(NC_VAR_INFO_T* var, size_t maxsize, NCZChunkCache** cachep)
{
    int stat = NC_NOERR;
    NCZChunkCache* cache = NULL;

    if((cache = calloc(1,sizeof(NCZChunkCache))) == NULL)
	{stat = NC_ENOMEM; goto done;}
    cache->var = var;
    cache->maxsize = maxsize;
    if((cache->entries = NC_hashmapnew(0)) == NULL)
	{stat = NC_ENOMEM; goto done;}
    if(cachep) {*cachep = cache; cache = NULL;}
done:
    nullfree(cache);
    return stat;
}

int
NCZ_read_cache_chunk(NCZChunkCache* cache, const size64_t* indices, size64_t* datalenp, void** datap)
{
    int stat = NC_NOERR;
    char* key = NULL;
    NCZCacheEntry* entry = NULL;
    size_t rank = cache->var->ndims;

    /* Create the key for this cache */
    if((stat=buildchunkkey(rank, indices, &key))) goto done;
    /* See if already in cache */
    if(!NC_hashmapget(cache->entries, key, strlen(key), (uintptr_t*)entry)) { /* !found */
	/* Create a new entry */
	if((entry = calloc(1,sizeof(NCZCacheEntry)))==NULL)
	    {stat = NC_ENOMEM; goto done;}
	memcpy(entry->indices,indices,rank*sizeof(size64_t));
	/* Read the object in toto */
	if((stat=get_chunk(cache->var,key,entry->size,&entry->data)))
	    goto done;
#ifdef VERIFY
        /* Verify chunksize */
	{
	    size64_t chunksize = 1;
	    size_t r;
	    for(r=0;r<rank;r++) chunksize *= cache->var->dim[r]->len;
	    if(chunksize != entry->size)
	        {stat = NC_EINTERNAL; goto done;}
	}
#endif
    }
    if(datalenp) *datalenp = entry->size;	
    if(datap) *datap = entry->data;

done:
    nullfree(key);
    return stat;
}

#if 0
int
NCZ_write_cache_chunk(NCZChunkCache* cache, const NC_VAR_INFO_T* var, const size64_t* indices, size64_t datalen, void* data)
{
    int stat = NC_NOERR;
    char* key = NULL;
    NCZCacheEntry* entry = NULL;
    size_t rank = var->ndims;

    /* Create the key for this cache */
    if((stat=buildchunkkey(rank, indices, &key))) goto done;
    /* See if already in cache */
    if(!NC_hashmapget(cache->entries, key, strlen(key), (uintptr_t*)entry)) { /* !found */
	/* Create a new entry */
	if((entry = calloc(1,sizeof(NCZCacheEntry)))==NULL)
	    {stat = NC_ENOMEM; goto done;}
	entry->var = var;
	memcpy(entry->indices,indices,rank*sizeof(size64_t));
	/* Store the data */
	entry->size = datalen;
	if((entry->data = malloc(entry->size))== NULL)
	    {stat = NC_ENOMEM; goto done;}
	memcpy(entry->data,data,entry->size);
#ifdef VERIFY
        /* Verify chunksize */
	{
	    size64_t chunksize = 1;
	    size_t r;
	    for(r=0;r<rank;r++) chunksize *= var->dim[r]->len;
	    if(chunksize != entry->size)
	        {stat = NC_EINTERNAL; goto done;}
	}
#endif
    }
#ifdef VERIFY
    if(entry->var != var) {stat = NC_EINTERNAL; goto done;}
#endif

done:
    nullfree(key);
    return stat;
}
#endif

int
NCZ_flush_cache_chunk(NCZChunkCache* cache)
{
    int stat = NC_NOERR;
    size_t i;

    /* Iterate over the entries in hashmap */
    for(i=0;;i++) {
        NCZCacheEntry* entry = NULL;
        uintptr_t data = 0;
	const char* key;
	/* get ith entry key */
	if(NC_hashmapith(cache->entries,i,NULL,&key) == NC_EINVAL) break;
	if(key != NULL) {
	    /* It exists  */
	    (void)NC_hashmapget(cache->entries,key,strlen(key),&data);
	    entry = (NCZCacheEntry*)data;
	    if(entry->modified) {
	        /* Write out this chunk in toto*/
  	        if((stat=put_chunk(cache->var,key,entry->size,entry->data)))
	            goto done;
	    }
	    entry->modified = 0;
	}
    }

done:
    return stat;
}

int
ncz_chunk_cache_modified(NCZChunkCache* cache, const size64_t* indices)
{
    int stat = NC_NOERR;
    char* key = NULL;
    NCZCacheEntry* entry = NULL;
    size_t rank = cache->var->ndims;

    /* Create the key for this cache */
    if((stat=buildchunkkey(rank, indices, &key))) goto done;

    /* See if already in cache */
    if(NC_hashmapget(cache->entries, key, strlen(key), (uintptr_t*)entry)) { /* found */
	entry->modified = 1;
#ifdef FLUSH
	if((stat=put_chunk(cache->var,key,entry->size,entry->data)))
	            goto done;
	entry->modified = 0;
#endif	
    }

done:
    nullfree(key);
    return stat;
}

/**************************************************/
/*
From Zarr V2 Specification:
"The compressed sequence of bytes for each chunk is stored under
a key formed from the index of the chunk within the grid of
chunks representing the array.  To form a string key for a
chunk, the indices are converted to strings and concatenated
with the period character (".") separating each index. For
example, given an array with shape (10000, 10000) and chunk
shape (1000, 1000) there will be 100 chunks laid out in a 10 by
10 grid. The chunk with indices (0, 0) provides data for rows
0-1000 and columns 0-1000 and is stored under the key "0.0"; the
chunk with indices (2, 4) provides data for rows 2000-3000 and
columns 4000-5000 and is stored under the key "2.4"; etc."
*/

/**
 * @param R Rank
 * @param chunkindices The chunk indices
 * @param keyp Return the chunk key string
 */
static int
buildchunkkey(size_t R, const size64_t* chunkindices, char** keyp)
{
    int stat = NC_NOERR;
    int r;
    NCbytes* key = ncbytesnew();

    if(keyp) *keyp = NULL;
    
    for(r=0;r<R;r++) {
	char index[64];
        if(r > 0) ncbytesappend(key,'.');
	/* Print as decimal with no leading zeros */
	snprintf(index,sizeof(index),"%lu",(unsigned long)chunkindices[r]);	
    }
    ncbytesnull(key);
    if(keyp) *keyp = ncbytesextract(key);

    ncbytesfree(key);
    return stat;
}

/**
 * @internal Push data to chunk of a file.
 * If chunk does not exist, create it
 *
 * @param file Pointer to file info struct.
 * @param proj Chunk projection
 * @param datalen size of data
 * @param data Buffer containing the chunk data to write
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
put_chunk(const NC_VAR_INFO_T* var, const char* chunkkey, size64_t datalen, const void* data)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;
    NCZMAP* map = NULL;

    LOG((3, "%s: var: %p", __func__, var));

    zfile = ((var->container)->nc4_info)->format_file_info;
    map = zfile->map;

    if((stat = nczmap_write(map,chunkkey,0,datalen,data) != NC_NOERR))
	    goto done;

done:
    return stat;
}

/**
 * @internal Push data from memory to file.
 * If chunk does not exist, create it.
 * Currently caching is not used.
 *
 * @param file Pointer to file info struct.
 * @param proj Chunk projection
 * @param datalen size of data
 * @param data Buffer containing the chunk data to write
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
get_chunk(const NC_VAR_INFO_T* var, const char* chunkkey, size64_t datalen, void* data)
{
    int stat = NC_NOERR;
    NCZMAP* map = NULL;
    NC_FILE_INFO_T* file = NULL;
    NCZ_FILE_INFO_T* zfile = NULL;

    LOG((3, "%s: file: %p", __func__, file));

    file = (var->container)->nc4_info;
    zfile = file->format_file_info;
    map = zfile->map;

    if((stat = nczmap_read(map,chunkkey,0,datalen,(char*)data) != NC_NOERR))
	    goto done;

done:
    return stat;
}

void
NCZ_free_chunk_cache(NCZChunkCache* cache)
{
    size_t i;

    /* Iterate over the entries in hashmap */
    for(i=0;;i++) {
        NCZCacheEntry* entry = NULL;
        uintptr_t data = 0;
	const char* key;
	/* get ith entry key */
	if(NC_hashmapith(cache->entries,i,&data,&key) == NC_EINVAL) break;
	if(key != NULL) {
	    /* It exists; reclaim entry  */
	    entry = (NCZCacheEntry*)data;
	    nullfree(entry->data);
	    nullfree(entry);
	}
    }
}
