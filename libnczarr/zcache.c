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

#define FLUSH

/* Forward */
static int get_chunk(NCZChunkCache* cache, const char* key, NCZCacheEntry* entry);
static int put_chunk(NCZChunkCache* cache, const char* key, const NCZCacheEntry*);
static int create_chunk(NCZChunkCache* cache, const char* key, NCZCacheEntry* entry);
static int buildchunkkey(size_t R, const size64_t* chunkindices, char** keyp);

/**
 * Create a chunk cache object
 *
 * @param var containing var
 * @param entrysize Size in bytes of an entry
 * @param cachep return cache pointer
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Bad preemption.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_create_chunk_cache(NC_VAR_INFO_T* var, size64_t chunksize, NCZChunkCache** cachep)
{
    int stat = NC_NOERR;
    NCZChunkCache* cache = NULL;
    NC_FILE_INFO_T* file = var->container->nc4_info;
    void* fill = NULL;

    if((cache = calloc(1,sizeof(NCZChunkCache))) == NULL)
	{stat = NC_ENOMEM; goto done;}
    cache->var = var;
    cache->chunksize = chunksize;
    {
	size_t i;
	unsigned char* p;
	size_t typesize = var->type_info->size;

	/* Get chunksize fill */
	if((stat = nc4_get_fill_value(file,var,&fill))) goto done;
	/* propagate */		
	if((cache->fillchunk = malloc(cache->chunksize))==NULL)
	    {stat = NC_ENOMEM; goto done;}
	for(p=cache->fillchunk,i=0;i<cache->chunksize;i+=typesize,p+=typesize) 		
	    memcpy(p,fill,typesize);
    }
    if((cache->entries = NC_hashmapnew(0)) == NULL)
	{stat = NC_ENOMEM; goto done;}
    if(cachep) {*cachep = cache; cache = NULL;}
done:
    nullfree(fill);
    nullfree(cache);
    return THROW(stat);
}

size64_t
NCZ_cache_entrysize(NCZChunkCache* cache)
{
    return cache->chunksize;
}

/* Return number of active entries in cache */
size64_t
NCZ_cache_size(NCZChunkCache* cache)
{
    return NC_hashmapcount(cache->entries);
}

int
NCZ_read_cache_chunk(NCZChunkCache* cache, const size64_t* indices, void** datap)
{
    int stat = NC_NOERR;
    char* chunkkey = NULL;
    char* varkey = NULL;
    char* key = NULL;
    NCZCacheEntry* entry = NULL;
    int rank = cache->var->ndims;
    NC_FILE_INFO_T* file = cache->var->container->nc4_info;

    /* Create the key for this cache */
    if((stat = NCZ_buildchunkpath(cache,indices,&key))) goto done;

    /* See if already in cache */
    if(NC_hashmapget(cache->entries, key, strlen(key), (uintptr_t*)&entry)) { /* found */
        if(datap) *datap = entry->data;
	entry = NULL; /*avoid reclaiming */
	goto done;
    } else { /*!found*/
	/* Create a new entry */
	if((entry = calloc(1,sizeof(NCZCacheEntry)))==NULL)
	    {stat = NC_ENOMEM; goto done;}
	memcpy(entry->indices,indices,rank*sizeof(size64_t));
	/* Create the local copy space */
	if((entry->data = calloc(1,cache->chunksize)) == NULL)
	    {stat = NC_ENOMEM; goto done;}
	/* Try to read the object in toto */
	stat=get_chunk(cache,key,entry);
	switch (stat) {
	case NC_NOERR: break;
	case NC_EACCESS: /*signals the chunk needs to be created */
	    /* If the file is read-only, then fake the chunk */
	    entry->modified = (!file->no_write);
	    if(!file->no_write) {
                if((stat = create_chunk(cache,key,entry))) goto done;
	    }
	    /* apply fill value */
	    memcpy(entry->data,cache->fillchunk,cache->chunksize);
	    break;
	default: goto done;
	}
	/* Put entry into hashmap */
        if(!NC_hashmapadd(cache->entries, (uintptr_t)entry, key, strlen(key)))
	    {stat = NC_EINTERNAL; goto done;}
        if(datap) *datap = entry->data;
	entry = NULL;
    }

done:
    if(entry) nullfree(entry->data);
    nullfree(entry);
    nullfree(key);
    nullfree(varkey);
    nullfree(chunkkey);
    return THROW(stat);
}

int
NCZ_write_cache_chunk(NCZChunkCache* cache, const size64_t* indices, const void* data)
{
    int stat = NC_NOERR;
    char* key = NULL;
    NCZCacheEntry* entry = NULL;
    int rank = cache->var->ndims;

    /* Create the key for this cache */
    if((stat=buildchunkkey(rank, indices, &key))) goto done;
    /* See if already in cache */
    if(!NC_hashmapget(cache->entries, key, strlen(key), (uintptr_t*)&entry)) { /* !found */
	/* Create a new entry */
	if((entry = calloc(1,sizeof(NCZCacheEntry)))==NULL)
	    {stat = NC_ENOMEM; goto done;}
	memcpy(entry->indices,indices,rank*sizeof(size64_t));
	if((entry->data = calloc(1,cache->chunksize)) == NULL)
	    {stat = NC_ENOMEM; goto done;}
	memcpy(entry->data,data,cache->chunksize);
    }
    /* Mark entry as modified */
    entry->modified = 1;    
#ifdef FLUSH
    if((stat=put_chunk(cache,key,entry))) goto done;
#endif

done:
    nullfree(key);
    return THROW(stat);
}

int
NCZ_flush_chunk_cache(NCZChunkCache* cache)
{
    int stat = NC_NOERR;
    size_t i;

    if(NCZ_cache_size(cache) == 0) goto done;
    
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
  	        if((stat=put_chunk(cache,key,entry)))
	            goto done;
	    }
	    entry->modified = 0;
	}
    }

done:
    return THROW(stat);
}

#if 0
int
NCZ_chunk_cache_modified(NCZChunkCache* cache, const size64_t* indices)
{
    int stat = NC_NOERR;
    char* key = NULL;
    NCZCacheEntry* entry = NULL;
    int rank = cache->var->ndims;

    /* Create the key for this cache */
    if((stat=buildchunkkey(rank, indices, &key))) goto done;

    /* See if already in cache */
    if(NC_hashmapget(cache->entries, key, strlen(key), (uintptr_t*)entry)) { /* found */
	entry->modified = 1;
    }

done:
    nullfree(key);
    return THROW(stat);
}
#endif

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
	char sindex[64];
        if(r > 0) ncbytescat(key,".");
	/* Print as decimal with no leading zeros */
	snprintf(sindex,sizeof(sindex),"%lu",(unsigned long)chunkindices[r]);	
	ncbytescat(key,sindex);
    }
    ncbytesnull(key);
    if(keyp) *keyp = ncbytesextract(key);

    ncbytesfree(key);
    return THROW(stat);
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
put_chunk(NCZChunkCache* cache, const char* key, const NCZCacheEntry* entry)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;
    NCZMAP* map = NULL;

    LOG((3, "%s: var: %p", __func__, cache->var));

    zfile = ((cache->var->container)->nc4_info)->format_file_info;
    map = zfile->map;

    stat = nczmap_write(map,key,0,cache->chunksize,entry->data);
    switch(stat) {
    case NC_NOERR: break;
    case NC_EACCESS:
	/* Create the chunk */
	if((stat = nczmap_define(map,key,cache->chunksize))) goto done;
	/* write again */
	if((stat = nczmap_write(map,key,0,cache->chunksize,entry->data)))
	    goto done;
	break;
    default: goto done;
    }
done:
    return THROW(stat);
}

/**
 * @internal Push data from memory to file.
 *
 * @param cache Pointer to parent cache
 * @param key chunk key
 * @param entry cache entry to read into
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
get_chunk(NCZChunkCache* cache, const char* key, NCZCacheEntry* entry)
{
    int stat = NC_NOERR;
    NCZMAP* map = NULL;
    NC_FILE_INFO_T* file = NULL;
    NCZ_FILE_INFO_T* zfile = NULL;

    LOG((3, "%s: file: %p", __func__, file));

    file = (cache->var->container)->nc4_info;
    zfile = file->format_file_info;
    map = zfile->map;
    assert(map && entry->data);

    stat = nczmap_read(map,key,0,cache->chunksize,(char*)entry->data);

    return THROW(stat);
}

static int
create_chunk(NCZChunkCache* cache, const char* key, NCZCacheEntry* entry)
{
    int stat = NC_NOERR;
    NC_FILE_INFO_T* file = NULL;
    NCZ_FILE_INFO_T* zfile = NULL;
    NCZMAP* map = NULL;

    file = (cache->var->container)->nc4_info;
    zfile = file->format_file_info;
    map = zfile->map;

    /* Create the chunk */
    if((stat = nczmap_define(map,key,cache->chunksize))) goto done;
    entry->modified = 1; /* mark as modified */
    /* let higher function decide on fill */
done:
    return THROW(stat);
}

void
NCZ_free_chunk_cache(NCZChunkCache* cache)
{
    size_t i;

    if(cache == NULL) return;
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
    NC_hashmapfree(cache->entries);
    cache->entries = NULL;
    nullfree(cache->fillchunk);
    nullfree(cache);
}

int
NCZ_buildchunkpath(NCZChunkCache* cache, const size64_t* chunkindices, char** keyp)
{
    int stat = NC_NOERR;
    char* chunkname = NULL;
    char* varkey = NULL;
    char* key = NULL;

    /* Get the chunk object name */
    if((stat = buildchunkkey(cache->var->ndims, chunkindices, &chunkname))) goto done;
    /* Get the var object key */
    if((stat = NCZ_varkey(cache->var,&varkey))) goto done;
    /* Prefix the path to the containing variable object */
    if((stat=nczm_concat(varkey,chunkname,&key))) goto done;
    if(keyp) {*keyp = key; key = NULL;}

done:
    nullfree(chunkname);
    nullfree(varkey);
    nullfree(key);
    return THROW(stat);
}
