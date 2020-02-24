/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#ifndef ZPROJTEST_H
#define ZPROJTEST_H

#include "zincludes.h"
#include "ztest.h"

typedef struct ProjTest {
    size_t rank;
    size64_t dimlen[NC_MAX_VAR_DIMS];
    size64_t chunklen[NC_MAX_VAR_DIMS];
    NCZSlice slices[NC_MAX_VAR_DIMS];
    NCZChunkRange range;
}  ProjTest;

extern NCbytes* buf;

extern int ut_proj_init(int argc, char** argv, ProjTest* test);

extern int parseslices(const char* s0, NCZSlice* slices);
extern int parsevector(const char* s0, size64_t* vector);
extern int parsevectorn(const char* s0, size_t len, size64_t* vector);

extern char* printtest(ProjTest* test);
extern char* printvec(int rank, size64_t* vec);
extern char* printslices(int rank, NCZSlice* slices);

#endif /*ZPROJTEST_H*/
