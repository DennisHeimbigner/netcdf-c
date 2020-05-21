/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#ifndef ZPROJTEST_H
#define ZPROJTEST_H

#include <stdarg.h>
#include "zincludes.h"
#include "ut_test.h"

typedef struct ProjTest {
    struct Test test; /* Superclass */
    int rank;
    size64_t dimlen[NC_MAX_VAR_DIMS];
    size64_t chunklen[NC_MAX_VAR_DIMS];
    NCZSlice slices[NC_MAX_VAR_DIMS];
    NCZChunkRange range[NC_MAX_VAR_DIMS];
    unsigned int typesize;
}  ProjTest;

typedef struct Dimdef {
    char* name;
    size64_t size;
} Dimdef;

typedef struct Vardef  {
    char* name;
    nc_type typeid;
    size_t typesize;
    int rank;
    Dimdef* dimrefs[NC_MAX_VAR_DIMS];
    size64_t dimsizes[NC_MAX_VAR_DIMS];
    size64_t chunksizes[NC_MAX_VAR_DIMS];
} Vardef;

/* Expose functions for unit tests */
typedef struct NCZ_UT_PRINTER {
    int printsort;
    void (*printer)(struct NCZ_UT_PRINTER*);
    /* Union of all fields */
    int rank;
    size64_t count;
    size64_t offset;
    size64_t* indices;
    size64_t* vector;
    void** pvector;
    NCZOdometer* odom;
    void* output;
    size_t used;
} NCZ_UT_PRINTER;

#if 0
extern NCbytes* buf;
#endif

extern int ut_proj_init(int argc, char** argv, ProjTest* test);

extern int parsedimdef(const char* s0, Dimdef** defp);
extern int parsevardef(const char* s0, NClist* dimdefs, Vardef** varp);
extern int parseslices(const char* s0, int*, NCZSlice*);
extern int parseintvector(const char* s0, int typelen, void** vectorp);
extern int parsestringvector(const char* s0, int stopchar, char*** namesp);
extern void freedimdefs(NClist* defs);
extern void freevardefs(NClist* defs);
extern void freeranges(NCZChunkRange*);
extern void freeslices(NCZSlice*);
extern void freestringvec(char** vec);
extern void freeprojvector(int, NCZProjection** vec);

extern char* printprojtest(ProjTest* test);
#if 0
extern char* printvec(int rank, size64_t* vec);
extern char* printslices(int rank, NCZSlice* slices);
#endif

#endif /*ZPROJTEST_H*/
