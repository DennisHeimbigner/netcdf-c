/*
Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
See LICENSE.txt for license information.
*/

#ifndef NCEXHASH_H
#define NCEXHASH_H

#if HAVE_CONFIG_H
#include "config.h"
#endif
#include <stdint.h>
#include <netcdf.h>

#if 0
#ifndef EXTERNL
# ifdef _WIN32
#  ifdef DLL_EXPORT /* define when building the library */
#   define EXTERNL __declspec(dllexport) extern
#  else
#   define EXTERNL __declspec(dllimport) extern
#  endif
# else /*!_WIN32*/
#  define EXTERNL extern
# endif /* _WIN32 */
#endif
#endif

/*
Implement extendible hashing as defined in:
````
R. Fagin, J. Nievergelt, N. Pippenger, and H. Strong, "Extendible Hashing -·a fast access method for dynamic files", ACM Transactions on Database Systems, vol. 4, No. 3, pp. 315-344, 1979.
````
*/

/*! Hashmap-related structs.
  NOTES:
  1. 'data' is the an arbitrary uintptr_t integer or void* pointer.
  2. hashkey is a crc64 hash of key -- it is assumed to be unique for keys.
    
  WARNINGS:
  1. It is critical that |uintptr_t| == |void*|
*/

#define exhashkey_t unsigned long long
#define EXHASHKEYBITS 64

typedef struct NCexentry {
    exhashkey_t hashkey; /* Hash id */
    uintptr_t data;
} NCexentry;

typedef struct NCexleaf {
    unsigned uid; /* primarily for debug */
    struct NCexleaf* next; /* linked list of all leaves for cleanup */
    int depth; /* local depth */
    int active; /* index of the first emptry slot */
    NCexentry* entries; /* |entries| == leaflen*/
} NCexleaf;

/* Top Level Vector */
typedef struct NCexhash {
    int leaflen; /* # entries a leaf can store */
    int depth; /* Global depth */
    NCexleaf* leaves; /* head of the linked list of leaves */
    int nactive; /* # of active entries in whole table */
    NCexleaf** directory; /* |directory| == 2^depth */
    unsigned uid; /* unique id counter */
    /* Allow a single iterator over the entries */
    struct {
	int walking; /* 0=>not in use */
	int index; /* index of current entry in leaf */
	NCexleaf* leaf; /* leaf we are walking */
    } iterator;
} NCexhash;

/** Creates a new exhash using LSB */
EXTERNL NCexhash* ncexhashnew(int leaflen);

/** Reclaims the exhash structure. */
EXTERNL void ncexhashfree(NCexhash*);

/** Returns the number of active elements. */
EXTERNL int ncexhashcount(NCexhash*);

/* Hash key based API */

/* Lookup by Hash Key */
EXTERNL int ncexhashget(NCexhash*, exhashkey_t hkey, uintptr_t*);

/* Insert by Hash Key */
EXTERNL int ncexhashput(NCexhash*, exhashkey_t hkey, uintptr_t data);

/* Remove by Hash Key */
EXTERNL int ncexhashrem(NCexhash*, exhashkey_t hkey, uintptr_t* datap);

/** Change the data for the specified key; takes hashkey. */
EXTERNL int ncexhashsetdata(NCexhash*, exhashkey_t hkey, uintptr_t newdata);

/* Return the hash key for specified key; takes key+size*/
EXTERNL exhashkey_t ncexhashkey(const char* key, size_t size);

/* Walk the entries in some order */
/*
@return NC_NOERR if keyp and datap are valid
@return NC_ERANGE if iteration is finished
@return NC_EINVAL for all other errors
*/
EXTERNL int ncexhashiterate(NCexhash* map, exhashkey_t* keyp, uintptr_t* datap);

/* Debugging */
EXTERNL void ncexhashprint(NCexhash*);
EXTERNL void ncexhashprintstats(NCexhash*);
EXTERNL void ncexhashprintdir(NCexhash*, NCexleaf** dir);
EXTERNL void ncexhashprintleaf(NCexhash*, NCexleaf* leaf);
EXTERNL void ncexhashprintentry(NCexhash* map, NCexentry* entry);
EXTERNL char* ncexbinstr(exhashkey_t hkey, int depth);

#endif /*NCEXHASH_H*/

