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
    NCZSlice* slices;
    NCZChunkRange range;
    unsigned int typesize;
}  ProjTest;

extern NCbytes* buf;

extern int ut_proj_init(int argc, char** argv, ProjTest* test);

extern int parseslices(const char* s0, NCZSlice**);
extern int parseintvectorn(const char* s0, int typelen, void** vectorp);
extern int parsestringvector(const char* s0, int stopchar, char*** namesp);

extern char* printtest(ProjTest* test);
#if 0
extern char* printvec(int rank, size64_t* vec);
extern char* printslices(int rank, NCZSlice* slices);
#endif

#endif /*ZPROJTEST_H*/
