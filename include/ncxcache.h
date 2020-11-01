/*
Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
See COPYRIGHT for license information.
*/

#ifndef NCXCACHE_H
#define NCXCACHE_H

#include "nclist.h"
#include "ncexhash.h" /* Also includes name map and id map */

/*
This cache data structure is an ordered list of objects. It is
used to create an LRU cache of arbitrary objects.
*/

/* Doubly linked list element */
typedef struct NCxnode {
    struct NCxnode* next;
    struct NCxnode* prev;
    void* content; /* associated data of some kind */
} NCxnode;

typedef struct NCxcache {
   NCxnode lru;
   NCexhashmap* map;
} NCxcache;

/* Locate object by hashkey */
EXTERNL int ncxcachelookup(NCxcache* cache, ncexhashkey_t hkey, void** contentp);

/* Insert object into the cache >*/
EXTERNL int ncxcacheinsert(NCxcache* cache, ncexhashkey_t hkey, void* obj);

/* Bring to front of the LRU queue */
EXTERNL int ncxcachetouch(NCxcache* cache, ncexhashkey_t hkey);

/* "Remove" object from the cache; return object */
EXTERNL int ncxcacheremove(NCxcache* cache, ncexhashkey_t hkey, void** obj);

/* Free cache. */
EXTERNL void ncxcachefree(NCxcache* cache);

/* Create a cache: size == 0 => use defaults */
EXTERNL int ncxcachenew(size_t initsize, NCxcache**) ;

/* Macro function */

/* Get the number of entries in an NCxcache */
#define ncxcachecount(cache) (cache == NULL ? 0 : ncexhashcount((cache)->map))

/* Debugging */
EXTERNL void ncxcacheprint(NCxcache* cache);
EXTERNL void* ncxcachetop(NCxcache* cache);

#endif /*NCXCACHE_H*/
